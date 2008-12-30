/***
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program; if not, write to the Free Software
 *    Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */
/*
 * Created on: 08-Dec-2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.rss.impl.tuple;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.simpledbm.rss.api.bm.BufferAccessBlock;
import org.simpledbm.rss.api.bm.BufferManager;
import org.simpledbm.rss.api.exception.ExceptionHandler;
import org.simpledbm.rss.api.fsm.FreeSpaceChecker;
import org.simpledbm.rss.api.fsm.FreeSpaceCursor;
import org.simpledbm.rss.api.fsm.FreeSpaceManager;
import org.simpledbm.rss.api.fsm.FreeSpaceMapPage;
import org.simpledbm.rss.api.fsm.FreeSpaceScan;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.loc.LocationFactory;
import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockException;
import org.simpledbm.rss.api.locking.LockMode;
import org.simpledbm.rss.api.locking.util.LockAdaptor;
import org.simpledbm.rss.api.platform.Platform;
import org.simpledbm.rss.api.platform.PlatformObjects;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.registry.ObjectFactory;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.registry.Storable;
import org.simpledbm.rss.api.registry.StorableFactory;
import org.simpledbm.rss.api.sp.SlottedPage;
import org.simpledbm.rss.api.sp.SlottedPageManager;
import org.simpledbm.rss.api.tuple.TupleContainer;
import org.simpledbm.rss.api.tuple.TupleException;
import org.simpledbm.rss.api.tuple.TupleInserter;
import org.simpledbm.rss.api.tuple.TupleManager;
import org.simpledbm.rss.api.tuple.TupleScan;
import org.simpledbm.rss.api.tx.BaseLoggable;
import org.simpledbm.rss.api.tx.BaseTransactionalModule;
import org.simpledbm.rss.api.tx.Compensation;
import org.simpledbm.rss.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.LoggableFactory;
import org.simpledbm.rss.api.tx.LogicalUndo;
import org.simpledbm.rss.api.tx.Redoable;
import org.simpledbm.rss.api.tx.Savepoint;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.api.tx.TransactionalModuleRegistry;
import org.simpledbm.rss.api.tx.Undoable;
import org.simpledbm.rss.api.wal.Lsn;
import org.simpledbm.rss.tools.diagnostics.Trace;
import org.simpledbm.rss.util.Dumpable;
import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;

/**
 * Implements the Tuple Manager interface as specified in 
 * {@link org.simpledbm.rss.api.tuple}. 
 * <p>
 * Some of the implementation decisions are given below:
 * <ol>
 * <li>Tuple inserts are done in two stages. In the first stage,
 * a new Location is allocated and locked exclusively. Also, the
 * tuple data is inserted into the first page that will be used by the
 * tuple. The rest of the tuple data is inserted in the second stage. The
 * rationale for splitting the insert into two stages is to allow
 * the client to perform other operations in between, such as creating
 * Index keys.</li>
 * <li>If a tuple is too large to fit into one page, it is broken into
 * chunks of {@link TupleManagerImpl.TupleSegment}. Each segment is inserted
 * into a page, and the segments are linked together in a singly linked
 * list. Since the segments are inserted in order, in the first pass the links
 * are not set, and the implementation has to revisit all the pages and update
 * the links between the segments. The last segment/page does not require an update.</li>
 * <li>At the time of insert, the implementation tries to allocate as much space
 * as possible on each page. When a page is visited, an attempt is made to reclaim
 * any deleted segments within the page.</li>
 * <li>Deletes are logical, ie, the Tuple Segments are marked with a logical
 * delete flag. Space Map information is updated immediately, however. During deletes,
 * the links between the segments of a tuple are broken. This is because the
 * link is reused to store the tuple's Location. Thus, it is possible to determine
 * the Location of a tuple from any of the deleted segments. This is important
 * because the tuple reclamation logic uses locking to determine whether a logically
 * deleted tuple can be physically removed.</li>
 * <li>If the delete is undone, the links are restored.</li>
 * <li>When tuples are updated, existing segments are updated with new data. If the new
 * tuple is longer than the old tuple, then additional segments are added. The 
 * {@link TupleInserter#completeInsert()} interface is reused for this purpose. No attempt is
 * made to change the size of existing segments, even if there is additional space available in
 * the page. This is to keep the algorithm simple. Also, segments are never released,
 * even if the new tuple has become smaller and does not occupy all the segments.</li>
 * <li>Note that due to the way this is implemented, more than one segment of the same 
 * tuple can end up in the same page.</li>
 * <li>Since the structure of the tuple is opaque to this module, updates are not
 * very efficiently handled. Current implementation logs the full before and after images
 * of the tuple being updated. In future, this needs to be made more efficient by using some
 * form of binary diff algorithm.
 * </li>
 * <li>Space used by deleted tuples is reclaimed when some other transaction visits
 * the affected page and tries to use the space. Locking is used to determine whether
 * the delete was committed. The actual physical removal of the tuple segments are
 * logged. Note that this method of determining whether a segment can be released is
 * conservative and sometimes results in segments being retained even when they are no
 * longer required. Why this happens will become clear with an example. Suppose that
 * a tuple that has 4 segments is deleted. Each segment is marked deleted. Now, some
 * transaction creates a new tuple, reusing the Location of the deleted tuple. However,
 * the new tuple may not reuse all the segments used by the old tuple, in fact, only
 * the first segment is guaranteed to be reused. As a result of the tuple insert, the
 * Location gets exclusively locked. If this transaction or any other transaction
 * encounters the remaining segments that are marked deleted, the reclamation logic will
 * incorrectly assume that the delete is still uncommitted, because the lock on the
 * location will fail. The segments will get eventually reused, when the transaction
 * that locked the tuple commits.</li>
 * <li>If both data page and free space map page need to be updated, then the
 * latch order is data page first, and then free space map page while holding the
 * data page latch.</li>
 * <li>As per Mohan, space map pages are always logged using redo only log records.
 * During normal processing, the data page update is logged first followed by the space
 * map page update. But during undo processing, the space map page update is logged
 * first, followed by data page update. Note that this does not change the latch
 * ordering.</li>
 * </ol>
 * 
 * @author Dibyendu Majumdar
 * @since 08-Dec-2005
 */
public class TupleManagerImpl extends BaseTransactionalModule implements
        TupleManager {

    final Logger log;
    
    final ExceptionHandler exceptionHandler;

    final MessageCatalog mcat;
    
    private static final short MODULE_ID = 6;

    private static final short TYPE_BASE = 60;

    private static final short TYPE_LOCATIONFACTORY = TYPE_BASE;

    private static final short TYPE_LOG_DELETESLOT = TYPE_BASE + 1;

    private static final short TYPE_LOG_INSERTTUPLESEGMENT = TYPE_BASE + 2;

    private static final short TYPE_LOG_UNDOINSERTTUPLESEGMENT = TYPE_BASE + 3;

    private static final short TYPE_LOG_UPDATESEGMENTLINK = TYPE_BASE + 4;

    private static final short TYPE_LOG_UNDOABLEREPLACESEGMENTLINK = TYPE_BASE + 5;

    private static final short TYPE_LOG_UNDOREPLACESEGMENTLINK = TYPE_BASE + 6;

    private static final short TYPE_LOG_UNDOABLEUPDATETUPLESEGMENT = TYPE_BASE + 7;

    private static final short TYPE_LOG_UNDOUPDATETUPLESEGMENT = TYPE_BASE + 8;

    final ObjectRegistry objectFactory;

    final LoggableFactory loggableFactory;

    private final FreeSpaceManager spaceMgr;

    final BufferManager bufmgr;

    final SlottedPageManager spMgr;

    final TupleIdFactory locationFactory;

    final LockAdaptor lockAdaptor;
    
    private final SlottedPage emptyPage;

    public TupleManagerImpl(Platform platform, ObjectRegistry objectFactory,
            LoggableFactory loggableFactory, FreeSpaceManager spaceMgr,
            BufferManager bufMgr, SlottedPageManager spMgr,
            TransactionalModuleRegistry moduleRegistry, PageManager pageFactory,
            LockAdaptor lockAdaptor, Properties p) {

    	PlatformObjects po = platform.getPlatformObjects(TupleManager.LOGGER_NAME);
        this.log = po.getLogger();
        this.exceptionHandler = po.getExceptionHandler();
        this.mcat = po.getMessageCatalog();
    	this.lockAdaptor = lockAdaptor;
        this.locationFactory = new TupleIdFactory();
        this.objectFactory = objectFactory;
        this.loggableFactory = loggableFactory;
        this.spaceMgr = spaceMgr;
        this.bufmgr = bufMgr;
        this.spMgr = spMgr;

        moduleRegistry.registerModule(MODULE_ID, this);

        objectFactory.registerSingleton(TYPE_LOCATIONFACTORY, locationFactory);
        objectFactory.registerObjectFactory(TYPE_LOG_DELETESLOT, new DeleteSlot.DeleteSlotFactory());
        objectFactory.registerObjectFactory(
            TYPE_LOG_INSERTTUPLESEGMENT, 
            new InsertTupleSegment.InsertTupleSegmentFactory());
        objectFactory.registerObjectFactory(
            TYPE_LOG_UNDOINSERTTUPLESEGMENT,
            new UndoInsertTupleSegment.UndoInsertTupleSegmentFactory());
        objectFactory.registerObjectFactory(
            TYPE_LOG_UPDATESEGMENTLINK,
            new UpdateSegmentLink.UpdateSegmentLinkFactory());
        objectFactory.registerObjectFactory(
            TYPE_LOG_UNDOABLEREPLACESEGMENTLINK,
            new UndoableReplaceSegmentLink.UndoableReplaceSegmentLinkFactory());
        objectFactory.registerObjectFactory(
            TYPE_LOG_UNDOREPLACESEGMENTLINK,
            new UndoReplaceSegmentLink.UndoReplaceSegmentLinkFactory());
        objectFactory.registerObjectFactory(
            TYPE_LOG_UNDOABLEUPDATETUPLESEGMENT,
            new UndoableUpdateTupleSegment.UndoableUpdateTupleSegmentFactory());
        objectFactory.registerObjectFactory(
            TYPE_LOG_UNDOUPDATETUPLESEGMENT,
            new UndoUpdateTupleSegment.UndoUpdateTupleSegmentFactory());

        emptyPage = (SlottedPage) pageFactory.getInstance(
                spMgr.getPageType(),
                new PageId());
        emptyPage.init();
    }

    /**
     * Returns the approximate space available in an empty page.
     */
    final int getPageSpace() {
    	return emptyPage.getSpace();
    }
    
    final int getPageSlotOverhead() {
    	return emptyPage.getSlotOverhead();
    }
    
    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.tx.BaseTransactionalModule#generateCompensation(org.simpledbm.rss.api.tx.Undoable)
     */
    @Override
    public Compensation generateCompensation(Undoable undoable) {
        if (undoable instanceof InsertTupleSegment) {
            InsertTupleSegment logrec = (InsertTupleSegment) undoable;
            UndoInsertTupleSegment clr = new UndoInsertTupleSegment(MODULE_ID, TYPE_LOG_UNDOINSERTTUPLESEGMENT);
            clr.slotNumber = logrec.slotNumber;
            return clr;
        } else if (undoable instanceof UndoableReplaceSegmentLink) {
            UndoableReplaceSegmentLink logrec = (UndoableReplaceSegmentLink) undoable;
            UndoReplaceSegmentLink clr = new UndoReplaceSegmentLink(MODULE_ID, TYPE_LOG_UNDOREPLACESEGMENTLINK);
            clr.slotNumber = logrec.slotNumber;
            clr.newSegmentationFlags = logrec.oldSegmentationFlags;
            clr.nextSegmentPage = logrec.oldNextSegmentPage;
            clr.nextSegmentSlotNumber = logrec.oldNextSegmentSlot;
            return clr;
        } else if (undoable instanceof UndoableUpdateTupleSegment) {
            // FIXME Test case needed
            UndoableUpdateTupleSegment logrec = (UndoableUpdateTupleSegment) undoable;
            UndoUpdateTupleSegment clr = new UndoUpdateTupleSegment(MODULE_ID, TYPE_LOG_UNDOUPDATETUPLESEGMENT);
            clr.slotNumber = logrec.slotNumber;
            clr.spaceMapPage = logrec.spaceMapPage;
            clr.segment = logrec.oldSegment;
            clr.segmentationFlags = logrec.oldSegmentationFlags;
            return clr;
        }
        return null;
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.tx.BaseTransactionalModule#undo(org.simpledbm.rss.api.tx.Transaction, org.simpledbm.rss.api.tx.Undoable)
     */
    @Override
    public void undo(Transaction trx, Undoable undoable) {
        if (undoable instanceof InsertTupleSegment) {
            undoInsertSegment(trx, undoable);
        } else if (undoable instanceof UndoableReplaceSegmentLink) {
            undoDeleteSegment(trx, undoable);
        }
    }

    /**
     * Undo the insert of a tuple segment and update space map information if
     * necessary. The undo is logged as a CLR, but the space map update is
     * logged using regular redo-only log record. According to Mohan, the space
     * map update must be logged before logging the page update to ensure
     * correct restart recovery.
     * @see UndoInsertTupleSegment
     */
    void undoInsertSegment(Transaction trx, Undoable undoable) {
        PageId pageId = undoable.getPageId();
        InsertTupleSegment logrec = (InsertTupleSegment) undoable;
        int spaceMapPage = logrec.spaceMapPage;

        Trace.event(127, pageId.getContainerId(), pageId.getPageNumber());
        BufferAccessBlock bab = bufmgr.fixExclusive(pageId, false, -1, 0);
        try {
            SlottedPage page = (SlottedPage) bab.getPage();
            Compensation clr = generateCompensation(undoable);
            clr.setUndoNextLsn(undoable.getPrevTrxLsn());

            /*
             * Since we need to log the space map update before logging the 
             * page update, we apply the log record, calculate the space map
             * changes, log the space map update, and then log the page update.
             */
            int spacebitsBefore = TwoBitSpaceCheckerImpl.mapFreeSpaceInfo(page
                .getSpace(), page.getFreeSpace());
            redo(page, clr);
            int spacebitsAfter = TwoBitSpaceCheckerImpl.mapFreeSpaceInfo(page
                .getSpace(), page.getFreeSpace());

            if (spacebitsAfter != spacebitsBefore) {
                /*
                 * Requires space map page update.
                 * We must obtain the latch on the free space map page while
                 * holding the latch on the data page.
                 * Also as per Mohan the space map update must be logged before the
                 * data page update, using redo only log record.
                 */
            	updateSpaceMapInfo(trx, pageId.getContainerId(), 
            			pageId.getPageNumber(), spaceMapPage, spacebitsAfter, spacebitsBefore);
                if (log.isDebugEnabled()) {
                    log.debug(
                        this.getClass().getName(),
                        "undoInsertSegment",
                        "SIMPLEDBM-DEBUG: Updated space map information from "
                                + spacebitsBefore + " to " + spacebitsAfter
                                + " for page " + pageId
                                + " in space map page " + spaceMapPage
                                + " as a result of " + clr);
                }
            }
            Lsn lsn = trx.logInsert(page, clr);
            bab.setDirty(lsn);
        } finally {
            bab.unfix();
        }
    }

    /**
     * Undo the delete of a tuple segment and update space map information if
     * necessary. The undo is logged as a CLR, but the space map update is
     * logged using regular redo-only log record. According to Mohan, the space
     * map update must be logged before logging the page update to ensure
     * correct restart recovery.
     */
    void undoDeleteSegment(Transaction trx, Undoable undoable) {
        PageId pageId = undoable.getPageId();
        UndoableReplaceSegmentLink logrec = (UndoableReplaceSegmentLink) undoable;
        BufferAccessBlock bab = bufmgr.fixExclusive(pageId, false, -1, 0);
        try {
            SlottedPage page = (SlottedPage) bab.getPage();
            Compensation clr = generateCompensation(undoable);
            clr.setUndoNextLsn(undoable.getPrevTrxLsn());

            TupleSegment ts = (TupleSegment) page.get(logrec.slotNumber, new TupleSegment.TupleSegmentFactory());
            int tslength = ts.getStoredLength();
            /*
             * Since we need to log the space map update before logging the 
             * page update, we apply the log record, calculate the space map
             * changes, log the space map update, and then log the page update.
             */
            int spacebitsBefore = TwoBitSpaceCheckerImpl.mapFreeSpaceInfo(page
                .getSpace(), page.getFreeSpace() + tslength);
            redo(page, clr);
            int spacebitsAfter = TwoBitSpaceCheckerImpl.mapFreeSpaceInfo(page
                .getSpace(), page.getFreeSpace());

            if (spacebitsAfter != spacebitsBefore) {
            	updateSpaceMapInfo(trx, page.getPageId().getContainerId(), page
						.getPageId().getPageNumber(), page
						.getSpaceMapPageNumber(), spacebitsAfter,
						spacebitsBefore);
				if (log.isDebugEnabled()) {
					log.debug(this.getClass().getName(), "undoDeleteSegment",
							"SIMPLEDBM-DEBUG: Updated space map information from "
									+ spacebitsBefore + " to " + spacebitsAfter
									+ " for page " + pageId
									+ " in space map page "
									+ page.getSpaceMapPageNumber()
									+ " as a result of " + clr);
				}
            }
            Lsn lsn = trx.logInsert(page, clr);
            bab.setDirty(lsn);
        } finally {
            bab.unfix();
        }
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.tx.BaseTransactionalModule#redo(org.simpledbm.rss.api.pm.Page, org.simpledbm.rss.api.tx.Redoable)
     */
    @Override
    public void redo(Page page, Redoable loggable) {
        SlottedPage p = (SlottedPage) page;
        p.dump();
        if (loggable instanceof DeleteSlot) {
            /*
             * Delete a slot within the page. We cannot use purge because slot
             * numbers are required to stay permanently allocated once used.
             * Note that this also handles UndoInsertTupleSegment.
             */
            DeleteSlot logrec = (DeleteSlot) loggable;
            p.delete(logrec.slotNumber);
        } else if (loggable instanceof BaseInsertTupleSegment) {
            /*
             * Note that this handles the redo of:
             * InsertTupleSegment
             * UndoUpdateTupleSegment
             * UndoableUpdateTupleSegment
             */
            BaseInsertTupleSegment logrec = (BaseInsertTupleSegment) loggable;
            p.insertAt(logrec.slotNumber, logrec.segment, true);
            if (p.getSpaceMapPageNumber() == -1) {
                p.setSpaceMapPageNumber(logrec.spaceMapPage);
            }
            p.setFlags(logrec.slotNumber, (short) logrec.segmentationFlags);
        } else if (loggable instanceof ExtendedUpdateSegmentLink) {
            /*
             * Note that this handles the redo of:
             * UndoableReplaceSegmentLink
             * UndoReplaceSegmentLink
             */
            ExtendedUpdateSegmentLink logrec = (ExtendedUpdateSegmentLink) loggable;
            TupleSegment ts = (TupleSegment) p.get(logrec.slotNumber, new TupleSegment.TupleSegmentFactory());
            ts.nextPageNumber = logrec.nextSegmentPage;
            ts.nextSlotNumber = logrec.nextSegmentSlotNumber;
            p.insertAt(logrec.slotNumber, ts, true);
            p.setFlags(logrec.slotNumber, (short) logrec.newSegmentationFlags);
        } else if (loggable instanceof UpdateSegmentLink) {
            UpdateSegmentLink logrec = (UpdateSegmentLink) loggable;
            TupleSegment ts = (TupleSegment) p.get(logrec.slotNumber, new TupleSegment.TupleSegmentFactory());
            ts.nextPageNumber = logrec.nextSegmentPage;
            ts.nextSlotNumber = logrec.nextSegmentSlotNumber;
            p.insertAt(logrec.slotNumber, ts, true);
        }
        p.dump();
    }

    /**
	 * Physically deletes any slots that are left over from previous
	 * deletes. The operation is logged.
	 */
    void reclaimDeletedTuples(Transaction trx, BufferAccessBlock bab) {
        SlottedPage page = (SlottedPage) bab.getPage();
        int slotsDeleted = 0;
        for (int slotNumber = 0; slotNumber < page.getNumberOfSlots(); slotNumber++) {
            /*
             * If slot is already deleted, then go to next one.
             */
            if (page.isSlotDeleted(slotNumber)) {
                continue;
            }
            /*
             * Is the slot logically deleted?
             */
            if (TupleHelper.isDeleted(page, slotNumber)) {
                TupleId location;

                if (!TupleHelper.isSegmented(page, slotNumber)
                        || TupleHelper.isFirstSegment(page, slotNumber)) {
                    location = new TupleId(page.getPageId(), slotNumber);
                } else {
                    /*
                     * If the slot is part of a tuple that is segmented, and
                     * if is not the first segment, then we need to read the
                     * tuple segment to determine the Location to which this
                     * segment belogs. The location is stored in the link field.
                     */
                    TupleSegment ts = (TupleSegment) page.get(slotNumber, new TupleSegment.TupleSegmentFactory());
                    PageId pageId = new PageId(page
                        .getPageId()
                        .getContainerId(), ts.nextPageNumber);
                    location = new TupleId(pageId, ts.nextSlotNumber);
                }
                /*
                 * Check if the location was locked by current transaction.
                 */
                LockMode mode = trx.hasLock(location);
                if (mode == LockMode.EXCLUSIVE) {
                    /*
                     * The current transaction holds an exclusive lock on
                     * the tuple. Hence the delete must have been executed
                     * within this transaction. Or the transaction is
                     * currently reusing this tuple location as part of an
                     * insert.
                     * Therefore the tuple segment cannot be reclaimed
                     */
                    if (log.isDebugEnabled()) {
                        log.debug(
                            this.getClass().getName(),
                            "reclaimDeletedTuples",
                            "SIMPLEDBM-DEBUG: Reclaim of tuple segment at (page="
                                    + page.getPageId() + ", slot="
                                    + slotNumber
                                    + ") skipped as the associated tuple "
                                    + location
                                    + " is locked in this transaction");
                    }
                    continue;
                }
                boolean reclaim = false;
                /*
                 * Test if we can acquire an exclusive lock on the tuple. If
                 * this test succeeds then the slot can be deleted, because
                 * it means that the original transaction that deleted the
                 * tuple has committed.
                 */
                try {
                    trx.acquireLockNowait(
                        location,
                        LockMode.EXCLUSIVE,
                        LockDuration.INSTANT_DURATION);
                    reclaim = true;
                } catch (LockException e) {
                    if (log.isDebugEnabled()) {
                        log
                            .debug(
                                this.getClass().getName(),
                                "reclaimDeletedTuples",
                                "SIMPLEDBM-DEBUG: Reclaim of tuple segment at (page="
                                        + page.getPageId()
                                        + ", slot="
                                        + slotNumber
                                        + ") skipped because cannot obtain lock on "
                                        + location,
                                e);
                    }
                }
                if (reclaim) {
                	/*
                	 * We successfully obtained an instant lock on the tuple 
                	 * so we know that this segment is committed and can be 
                	 * cleaned up.
                	 */
                    if (log.isDebugEnabled()) {
                        log.debug(
                            this.getClass().getName(),
                            "reclaimDeletedTuples",
                            "SIMPLEDBM-DEBUG: Reclaiming tuple segment at (page="
                                    + page.getPageId() + ", slot="
                                    + slotNumber
                                    + ") associated with tuple at "
                                    + location);
                    }
                    /*
                     * TODO: Instead of deleting one slot at a time, we should do them all
                     * in a batch.
                     */
                    DeleteSlot logrec = new DeleteSlot(
                        TupleManagerImpl.MODULE_ID,
                        TupleManagerImpl.TYPE_LOG_DELETESLOT);
                    logrec.slotNumber = slotNumber;
                    Lsn lsn = trx.logInsert(page, logrec);
                    redo(page, logrec);
                    bab.setDirty(lsn);
                    slotsDeleted++;
                }
            }
        }
    }

    /**
     * Holds the results of a free space scan.
     */
    static class FindPageResults {
    	/**
    	 * The page found is fixed exclusively. Will be null if 
    	 * the search was unsuccessful.
    	 */
		BufferAccessBlock bab = null;
		
		/**
		 * Page number of the page found by the search. If unsuccessful,
		 * this will be set to -1.
		 */
		int pageNumber;
		
		/**
		 * Space map page associated with above page.
		 */
		int spaceMapPage;
    }
    
    /**
     * Locate a page that has the required space. Space map pages are consulted,
     * but any candidate page is visited to verify the space available. This is necessary
     * due to the way deletes are handled.
     */
	FindPageResults locateEmptyPage(Transaction trx, int containerId, int requiredSpace) {
		int pageNumber = -1;
		
		/*
		 * We need some way of deciding whether we are in an endless loop.
		 * This can happen because pages may be marked as available in the free space
		 * map, but in reality be unusable due to uncommitted deletes. The mechanism
		 * we use simply uses the starting page as a signpost.
		 */
		int previouslySeenPageNumber = -1;
		BufferAccessBlock bab = null;
		int maxPageSpace = getPageSpace();
		int spaceMapPage;
		FreeSpaceCursor spcursor = spaceMgr
				.getPooledSpaceCursor(containerId);
		try {
			while (pageNumber == -1) {
				pageNumber = spcursor
						.findAndFixSpaceMapPageShared(new TwoBitSpaceCheckerImpl(
								maxPageSpace, requiredSpace));
				/*
				 * If we hit a page we have seen before then we are looping
				 * because space map may not be accurate due to uncommitted
				 * deletes.
				 */
				if ((previouslySeenPageNumber != -1 && pageNumber == previouslySeenPageNumber)
						|| pageNumber == -1) {
					// FIXME Test case needed
					spaceMgr.extendContainer(trx, containerId);
					pageNumber = spcursor
							.findAndFixSpaceMapPageShared(new TwoBitSpaceCheckerImpl(
									maxPageSpace, requiredSpace));
					if (pageNumber == -1) {
						exceptionHandler.errorThrow(this.getClass().getName(),
								"doCompleteInsert", new TupleException(mcat.getMessage("ET0004",
								containerId)));
					}
					previouslySeenPageNumber = -1;
				}
				if (pageNumber != -1 && previouslySeenPageNumber == -1) {
					previouslySeenPageNumber = pageNumber;
				}
				spaceMapPage = spcursor.getCurrentSpaceMapPage()
						.getPageId().getPageNumber();
				/*
				 * Latch on space map page needs to be released before we
				 * obtain a latch on the data page.
				 */
				spcursor.unfixCurrentSpaceMapPage();
				PageId pageId = new PageId(containerId, pageNumber);
				bab = bufmgr.fixExclusive(pageId, false, -1, 0);
				try {
					reclaimDeletedTuples(trx, bab);
					SlottedPage page = (SlottedPage) bab.getPage();
					if (page.getFreeSpace() < requiredSpace) {
						// FIXME Test case needed
						pageNumber = -1;
					}
					else {
						FindPageResults results = new FindPageResults();
						results.bab = bab;
						results.spaceMapPage = spaceMapPage;
						results.pageNumber = pageNumber;
						return results;
					}
				} finally {
					if (pageNumber == -1) {
						// FIXME Test case needed
						bab.unfix();
						bab = null;
					}
				}
			}
		} finally {
			spaceMgr.releaseSpaceCursor(spcursor);
		}
		/*
		 * Should never reach here.
		 */
		throw new TupleException();
	}

	/**
	 * Updates space map information for a particular page.
	 * The data page latch must be held when this is called.
	 * @param trx Transaction managing this update
	 * @param containerId The ID of the container being updated 
	 * @param pageNumber The page whose space usage has changed
	 * @param spaceMapPage The space map page that will maintain the space usage data
	 * @param spacebitsAfter The new space usage bits
	 * @param spacebitsBefore The old space usage bits
	 */
	void updateSpaceMapInfo(Transaction trx, int containerId, int pageNumber, int spaceMapPage, int spacebitsAfter, int spacebitsBefore) {
        /*
         * We must obtain the latch on the free space map page while
         * holding the latch on the data page.
         * As per Mohan the space map update must be logged before the
         * data page update, using redo only log record.
         */
    	Trace.event(128, containerId, spaceMapPage, pageNumber, spacebitsAfter);
		
		/*
		 * We get a new space cursor rather than a pooled on as we do not want this
		 * operation to affect the search for free space.
		 */
		FreeSpaceCursor spcursor = spaceMgr.getSpaceCursor(containerId);
        if (spacebitsAfter != spacebitsBefore) {
        	/*
        	 * We need to latch the space map page while holding the latch
        	 * on the data page - as per Mohan.
        	 */
            spcursor.fixSpaceMapPageExclusively(
                spaceMapPage,
                pageNumber);
            try {
                FreeSpaceMapPage smp = spcursor
                    .getCurrentSpaceMapPage();
                if (smp.getSpaceBits(pageNumber) != spacebitsAfter) {
                	/*
                	 * The space map update must be redo only, as per Mohan.
                	 */
                    spcursor.updateAndLogRedoOnly(
                        trx,
                        pageNumber,
                        spacebitsAfter);
                    if (log.isDebugEnabled()) {
                        log.debug(
                            this.getClass().getName(),
                            "doStartInsert",
                            "SIMPLEDBM-DEBUG: Updated space map information from "
                                    + spacebitsBefore + " to "
                                    + spacebitsAfter + " for page "
                                    + pageNumber
                                    + " in space map page "
                                    + spaceMapPage);
                    }
                }
            } finally {
                spcursor.unfixCurrentSpaceMapPage();
            }
        }
	}
	
    public void lockTupleContainer(Transaction trx, int containerId, LockMode mode) {
        trx.acquireLock(
            lockAdaptor.getLockableContainerId(containerId),
            mode,
            LockDuration.COMMIT_DURATION);        
    }
    
    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.tuple.TupleManager#createTupleContainer(org.simpledbm.rss.api.tx.Transaction, java.lang.String, int, int)
     */
    public void createTupleContainer(Transaction trx, String name,
            int containerId, int extentSize) {
    	lockTupleContainer(trx, containerId, LockMode.EXCLUSIVE);
        spaceMgr.createContainer(trx, name, containerId, 2, extentSize, spMgr
            .getPageType());
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.tuple.TupleManager#getTupleContainer(org.simpledbm.rss.api.tx.Transaction, int)
     */
    public TupleContainer getTupleContainer(Transaction trx, int containerId) {
    	lockTupleContainer(trx, containerId, LockMode.SHARED);
        return new TupleContainerImpl(this, containerId);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.tuple.TupleManager#getLocationFactory()
     */
    public LocationFactory getLocationFactory() {
        return locationFactory;
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.tuple.TupleManager#getLocationFactoryType()
     */
    public int getLocationFactoryType() {
        return TYPE_LOCATIONFACTORY;
    }

    /**
     * Handles the insert of a tuple. Creates tuple segments to hold
     * the tuple data and links the segments together in a singly linked list.
     */
    public static class TupleInserterImpl implements TupleInserter {

        final TupleContainerImpl tupleContainer;

        final TupleManagerImpl tuplemgr;
        
        final Transaction trx;

        /**
         * The tuple being inserted or updated.
         */
        final Storable tuple;

        final int containerId;

        /**
         * The location allocated to the tuple.
         */
        TupleId location;

        /**
         * The full length of the table row (tuple) we are inserting.
         */
        int tupleLength;

        /**
         * Tracks the offset within the tuple where insert is currently.
         */
        int currentPos = 0;

        /**
         * The tuple converted to a byte array. 
         */
        byte[] tupleData;

        int prevSegmentPage;

        int prevSegmentSlotNumber;

        /**
         * Savepoint created at the start of an insert. As inserts are a two-step
         * process, we need to remember the savepoint in case completeInsert() fails.
         */
        Savepoint savepoint;

        /**
         * The completeInsert() method may be invoked either due to an insert
         * or due to an update. This flag tells if this is an update. The behaviour is
         * slightly different for updates.
         */
        boolean updateMode;

        /**
         * Flag to indicate that insert was started successfully,
         * and that it is okay to complete the insert. Always set to
         * true when called from update().
         */
        boolean proceedWithInsert = false;

        /**
         * Maximum theoretical space in a page.
         */
        final int maxPageSpace;

        /**
         * Total Overhead in bytes per tuple segment.
         * Includes slot overhead and tuple segment overhead.
         */
        final int overhead;

        /**
         * Maximum possible tuple segment - assumes only segment in
         * a page.
         */
        final int maxSegmentSize;

        BufferAccessBlock bab = null;
        
        public TupleInserterImpl(TupleContainerImpl relation, Transaction trx, Storable tuple, boolean isUpdate) {
            this.tupleContainer = relation;
            this.tuplemgr = tupleContainer.tuplemgr;
            this.containerId = tupleContainer.containerId;
            this.maxPageSpace = relation.tuplemgr.getPageSpace();
            TupleSegment ts = new TupleSegment();
            this.overhead = ts.overhead() + relation.tuplemgr.getPageSlotOverhead();
            this.maxSegmentSize = maxPageSpace - overhead;
            this.updateMode = isUpdate;
            this.trx = trx;
            this.tuple = tuple;
        }

        /**
         * Allocates a new location for the tuple. If successful, the location will be
         * exclusively latched upon return of this method.
         * <p>
         * Sets {@link #location} if successful.
         * <p>
         * Before this method is called, bab must be set to the page where insert
         * should take place, and must be latched exclusively.
         */
        boolean allocateTupleLocation() {
            /*
             * When this is called, the new page should have been fixed exclusively.
             */
        	assert bab != null;
        	assert bab.isLatchedExclusively();
        	
            int slotNumber = -1;
            SlottedPage page = (SlottedPage) bab.getPage();
            int pageNumber = page.getPageId().getPageNumber();

            /*
             * Following loops around trying to use a slot within the page
             * that has been identified.
             */
            boolean done = false;
            boolean mustRestart = false;
            while (!done && !mustRestart) {

                /*
                 * Find a deleted slot or a new slot
                 */
                for (slotNumber = 0; slotNumber < page.getNumberOfSlots(); slotNumber++) {
                    if (page.isSlotDeleted(slotNumber)) {
                        break;
                    }
                }

                /*
                 * We have the tentative tuple id.
                 */
                location = new TupleId(page.getPageId(), slotNumber);
                if (tuplemgr.log.isDebugEnabled()) {
                	tuplemgr.log.debug(
                        this.getClass().getName(),
                        "startInsert",
                        "SIMPLEDBM-DEBUG: Tentative location for new tuple will be "
                                + location);
                }

                /*
                 * Try to get a conditional lock on the proposed tuple id.
                 */
                boolean locked = false;
                try {
                    trx.acquireLockNowait(
                        location,
                        LockMode.EXCLUSIVE,
                        LockDuration.COMMIT_DURATION);
                    locked = true;
                } catch (LockException e) {
                    if (tuplemgr.log.isDebugEnabled()) {
                    	tuplemgr.log.debug(
                            this.getClass().getName(),
                            "startInsert",
                            "SIMPLEDBM-DEBUG: Failed to obtain conditional lock on location "
                                    + location);
                    }
                }

                if (locked) {
                    done = true;
                } else {
                    /*
                     * Conditional lock failed. We need to get unconditional
                     * lock, but before that we need to release latches.
                     */
                    // FIXME Test case needed
                    Savepoint sp = trx.createSavepoint(false);
                    Lsn lsn = page.getPageLsn();

                    bab.unfix();
                    bab = null;

                    trx.acquireLock(
                        location,
                        LockMode.EXCLUSIVE,
                        LockDuration.COMMIT_DURATION);

                    PageId pageId = new PageId(containerId, pageNumber);
                    bab = tuplemgr.bufmgr.fixExclusive(pageId, false, -1, 0);
                    page = (SlottedPage) bab.getPage();
                    if (!page.getPageLsn().equals(lsn)) {
                        /*
                         * Page has changed in the meantime. Can we still
                         * continue with the insert?
                         */
                        if (page.getFreeSpace() >= overhead) {
                            /*
                             * There is still enough space in the page
                             */
                            if (slotNumber == page.getNumberOfSlots()
                                    || page.isSlotDeleted(slotNumber)) {
                                done = true;
                            } else {
                                /* 
                                 * Slot has changed in the meantime.
                                 * Release lock on previous slot and try another slot.
                                 */
                                trx.rollback(sp);
                                if (tuplemgr.log.isDebugEnabled()) {
                                	tuplemgr.log
                                        .debug(
                                            this.getClass().getName(),
                                            "startInsert",
                                            "SIMPLEDBM-DEBUG: Unable to continue insert of new tuple at "
                                                    + location
                                                    + " as page has changed in the meantime");
                                }
                                /*
                                 * Will try again.
                                 */
                            }
                        } else {
                            /*
                             * No more space in the page.
                             */
                            mustRestart = true;
                        }
                    } else {
                    	/*
                    	 * Page hasn't changed, so we are still okay.
                    	 */
                        done = true;
                    }
                }
            }

            if (mustRestart) {
                /*
                 * Cannot use the page or the slot originally identified due
                 * to lack of space. Must restart the insert
                 */
                if (tuplemgr.log.isDebugEnabled()) {
                	tuplemgr.log
                        .debug(
                            this.getClass().getName(),
                            "startInsert",
                            "SIMPLEDBM-DEBUG: Unable to continue insert of new tuple at "
                                    + location
                                    + " as page has changed in the meantime - Insert will be restarted");
                }
                return false;
            }
            return true;
        }
        
        /**
         * Start an insert. Gets the Location that has been reserved for the new
         * tuple. In case the tuple data extends to multiple pages, the Location
         * is a pointer to the first page/slot. Location will be locked
         * exclusively before this method returns.
         * <p>
         * Returns true if successful, false if client should retry operation.
         */
        boolean doStartInsert() {

            tupleLength = tuple.getStoredLength();
            boolean segmentationRequired;
            segmentationRequired = tupleLength > maxSegmentSize;

            int firstSegmentSize;
            if (segmentationRequired) {
                firstSegmentSize = maxSegmentSize;
            } else {
                firstSegmentSize = tupleLength;
            }
            int requiredSpace = overhead + firstSegmentSize;

            /*
             * Locate a page where the insert can go.
             */
            int spacebitsBefore = 0;

            FindPageResults fp = tuplemgr.locateEmptyPage(trx, containerId, requiredSpace);
            bab = fp.bab;
            int pageNumber = fp.pageNumber;
            int spaceMapPage = fp.spaceMapPage;
            
            assert bab != null;
            assert bab.isLatchedExclusively();
            
            if (tuplemgr.log.isDebugEnabled()) {
            	tuplemgr.log.debug(
                    this.getClass().getName(),
                    "startInsert",
                    "SIMPLEDBM-DEBUG: Page number " + pageNumber
                            + " has been selected for tuple's first segment");
            }

            try {
            	if (!allocateTupleLocation()) {
            		return false;
            	}

            	assert bab != null;
            	assert bab.isLatchedExclusively();
            	assert location != null;
            	assert location.getPageId().equals(bab.getPage().getPageId());
            	
            	SlottedPage page = (SlottedPage) bab.getPage();
            	int slotNumber = location.getSlotNumber();
                firstSegmentSize = page.getFreeSpace() - overhead;
                if (firstSegmentSize < tupleLength) {
                    segmentationRequired = true;
                } else {
                    segmentationRequired = false;
                    firstSegmentSize = tupleLength;
                }

                byte[] data = new byte[tupleLength];
                ByteBuffer bb = ByteBuffer.wrap(data);
                tuple.store(bb);

                tupleData = data;
                currentPos = 0;

                TupleSegment ts = new TupleSegment();
                ts.data = new byte[firstSegmentSize];
                System.arraycopy(
                    tupleData,
                    currentPos,
                    ts.data,
                    0,
                    firstSegmentSize);
                currentPos += firstSegmentSize;
                
                InsertTupleSegment logrec = new InsertTupleSegment(
                    TupleManagerImpl.MODULE_ID,
                    TupleManagerImpl.TYPE_LOG_INSERTTUPLESEGMENT);
                logrec.slotNumber = slotNumber;
                logrec.spaceMapPage = spaceMapPage;
                logrec.segment = ts;
                if (segmentationRequired) {
                    logrec.segmentationFlags = TupleHelper.TUPLE_SEGMENT
                            + TupleHelper.TUPLE_FIRST_SEGMENT;
                } else {
                    logrec.segmentationFlags = 0;
                }

                /*
                 * In order to decide whether the space map page needs to be updated,
                 * we need to compare the space usage before and after the insert of the
                 * tuple segment.
                 * Note that we don't need to worry about changes made by reclaim tuple
                 * logic as deletes update the space map page immediately.
                 */
                spacebitsBefore = TwoBitSpaceCheckerImpl.mapFreeSpaceInfo(page.getSpace(), 
                		page.getFreeSpace());  
                Lsn lsn = trx.logInsert(page, logrec);
                tuplemgr.redo(page, logrec);
                bab.setDirty(lsn);

                prevSegmentPage = page.getPageId().getPageNumber();
                prevSegmentSlotNumber = slotNumber;

                int spacebitsAfter = TwoBitSpaceCheckerImpl.mapFreeSpaceInfo(
                    maxPageSpace,
                    page.getFreeSpace());
                if (spacebitsAfter != spacebitsBefore) {
                    tuplemgr.updateSpaceMapInfo(trx, containerId, pageNumber, spaceMapPage, spacebitsAfter, spacebitsBefore);
                }
            } finally {
                if (bab != null) {
                    bab.unfix();
                    bab = null;
                }
            }
            return true;
        }

        /**
         * Completes insertion of tuple by generating additional segments where
         * necessary.
         */
        void doCompleteInsert() {

            if (currentPos == tupleLength) {
                /*
                 * There is no need to create additional segments.
                 */
                return;
            }

            TupleSegment ts = new TupleSegment();
            int pageNumber = -1;

            try {
                while (currentPos < tupleLength) {

                    int remaining = tupleLength - currentPos;

                    int nextSegmentSize;
                    if (remaining > maxSegmentSize) {
                        nextSegmentSize = maxSegmentSize;
                    } else {
                        nextSegmentSize = remaining;
                    }

                    int requiredSpace = overhead + nextSegmentSize;
                    
                    FindPageResults fp = tuplemgr.locateEmptyPage(trx, containerId, requiredSpace);
                    pageNumber = fp.pageNumber;
                    int spaceMapPage = fp.spaceMapPage;
                    bab = fp.bab;
                    int spacebitsBefore = -1;

                    assert bab != null;
                    assert bab.isLatchedExclusively();
                    
                    /*
                     * At this point the new page should be fixed.
                     */
                    SlottedPage page = (SlottedPage) bab.getPage();
                    nextSegmentSize = remaining;
                    if (nextSegmentSize > (page.getFreeSpace() - overhead)) {
                        nextSegmentSize = page.getFreeSpace() - overhead;
                    }

                    int slotNumber = -1;
                    for (slotNumber = 0; slotNumber < page.getNumberOfSlots(); slotNumber++) {
                        if (page.isSlotDeleted(slotNumber)) {
                            break;
                        }
                    }

                    InsertTupleSegment logrec = new InsertTupleSegment(
                        TupleManagerImpl.MODULE_ID,
                        TupleManagerImpl.TYPE_LOG_INSERTTUPLESEGMENT);
                    logrec.slotNumber = slotNumber;
                    logrec.spaceMapPage = spaceMapPage;
                    if (nextSegmentSize + currentPos == tupleLength) {
                        logrec.segmentationFlags = TupleHelper.TUPLE_LAST_SEGMENT
                                + TupleHelper.TUPLE_SEGMENT;
                    } else {
                        logrec.segmentationFlags = TupleHelper.TUPLE_SEGMENT;
                    }
                    ts.data = new byte[nextSegmentSize];
                    System.arraycopy(
                        tupleData,
                        currentPos,
                        ts.data,
                        0,
                        nextSegmentSize);
                    currentPos += nextSegmentSize;
                    logrec.segment = ts;

                    /*
                     * In order to determine whether the space map page needs to be updated
                     * we need to check the free space before and after the update.
                     * Note that we don't need to worry about the changes made by reclaim
                     * tuple logic because tuple deletes update the space map information
                     * immediately.
                     */
                    spacebitsBefore = TwoBitSpaceCheckerImpl.mapFreeSpaceInfo(page.getSpace(), 
                    		page.getFreeSpace());  

                    Lsn lsn = trx.logInsert(page, logrec);
                    tuplemgr.redo(page, logrec);
                    bab.setDirty(lsn);

                    int spacebitsAfter = TwoBitSpaceCheckerImpl.mapFreeSpaceInfo(
                        maxPageSpace,
                        page.getFreeSpace());
                    if (spacebitsAfter != spacebitsBefore) {
						tuplemgr.updateSpaceMapInfo(trx, containerId,
								pageNumber, spaceMapPage, spacebitsAfter,
								spacebitsBefore);
					}

                    bab.unfix();
                    bab = null;

                    /*
                     * Update previous segment.
                     */
                    updatePrevSegment(pageNumber, slotNumber);

                    prevSegmentPage = pageNumber;
                    prevSegmentSlotNumber = slotNumber;
                }
            } finally {
                if (bab != null) {
                    bab.unfix();
                    bab = null;
                }
            }
        }

        /**
         * Updates previous tuple segment so that it points to the next tuple
         * segment.
         */
        void updatePrevSegment(int nextSegmentPage, int nextSegmentSlotNumber) {

            if (prevSegmentPage != -1) {
                PageId pageId = new PageId(containerId, prevSegmentPage);
                BufferAccessBlock bab = tuplemgr.bufmgr.fixExclusive(
                    pageId,
                    false,
                    -1,
                    0);
                try {
                    SlottedPage page = (SlottedPage) bab.getPage();
                    Redoable loggable = null;
                    if (updateMode) {
                        updateMode = false;
                        UndoableReplaceSegmentLink logrec = new UndoableReplaceSegmentLink(
                            TupleManagerImpl.MODULE_ID,
                            TupleManagerImpl.TYPE_LOG_UNDOABLEREPLACESEGMENTLINK);
                        logrec.slotNumber = prevSegmentSlotNumber;
                        logrec.nextSegmentPage = nextSegmentPage;
                        logrec.nextSegmentSlotNumber = nextSegmentSlotNumber;
                        logrec.newSegmentationFlags = logrec.oldSegmentationFlags = page
                            .getFlags(prevSegmentSlotNumber);
                        if (TupleHelper
                            .isSegmented(page, prevSegmentSlotNumber)) {
                            TupleSegment ts = (TupleSegment) page.get(prevSegmentSlotNumber, new TupleSegment.TupleSegmentFactory());
                            logrec.oldNextSegmentPage = ts.nextPageNumber;
                            logrec.oldNextSegmentSlot = ts.nextSlotNumber;
                        }
                        loggable = logrec;
                    } else {
                        UpdateSegmentLink logrec = new UpdateSegmentLink(
                            TupleManagerImpl.MODULE_ID,
                            TupleManagerImpl.TYPE_LOG_UPDATESEGMENTLINK);
                    	logrec.nextSegmentPage = nextSegmentPage;
                        logrec.nextSegmentSlotNumber = nextSegmentSlotNumber;
                        logrec.slotNumber = prevSegmentSlotNumber;
                        loggable = logrec;
                    }
                    Lsn lsn = trx.logInsert(page, loggable);
                    tuplemgr.redo(page, loggable);
                    bab.setDirty(lsn);
                } finally {
                    bab.unfix();
                }
            }
        }

        /**
         * @see org.simpledbm.rss.api.tuple.TupleInserter#startInsert(org.simpledbm.rss.api.tx.Transaction,
         *      org.simpledbm.rss.api.tuple.Tuple)
         */
        Location startInsert() {
            savepoint = trx.createSavepoint(false);
            proceedWithInsert = false;
            try {
                while (!doStartInsert()) {
                }
                proceedWithInsert = true;
            } finally {
                if (!proceedWithInsert) {
                    // Need to rollback to the savepoint
                    trx.rollback(savepoint);
                    savepoint = null;
                }
            }
            return location;
        }

        /**
         * @see org.simpledbm.rss.api.tuple.TupleInserter#completeInsert()
         */
        public void completeInsert() {
            if (!proceedWithInsert) {
            	tuplemgr.exceptionHandler.errorThrow(this.getClass().getName(), "completInsert", 
                		new TupleException(tuplemgr.mcat.getMessage("ET0007")));
            }
            boolean success = false;
            try {
                doCompleteInsert();
                success = true;
            } finally {
                if (!success && savepoint != null) {
                    trx.rollback(savepoint);
                }
            }
        }

        public Location getLocation() {
            return new TupleId(location);
        }
    }

    public static class TupleContainerImpl implements TupleContainer {

        final TupleManagerImpl tuplemgr;

        final int containerId;

        public TupleContainerImpl(TupleManagerImpl relmgr, int containerId) {
            this.tuplemgr = relmgr;
            this.containerId = containerId;
        }

        public TupleInserter insert(Transaction trx, Storable tuple) {
            TupleInserterImpl inserter = new TupleInserterImpl(this, trx, tuple, false);
            inserter.startInsert();
            return inserter;
        }

        /**
         * Deletes a tuple by marking all segments of the tuple as deleted
         * (logical delete). Although the tuple segments are not physically
         * deleted, the free space map pages are updated as if the delete is
         * physical. The actual release of the tuple segments is deferred until
         * some other transaction visits the page and wants to use the space.
         * <p>
         * The tuple segments are modified so that their next pointer contains
         * the location of the tuple; this enables other transactions to
         * determine the location of the tuple, and test whether the tuple has
         * been committed.
         * 
         * @see TupleInserterImpl#reclaimDeletedTuples(Transaction,
         *      BufferAccessBlock)
         * @see TupleManagerImpl#redo(Page, Redoable)
         * @see TupleManagerImpl#undoDeleteSegment(Transaction, Undoable)
         */
        void doDelete(Transaction trx, Location location) {

            if (location.getContainerId() != containerId) {
            	tuplemgr.exceptionHandler.errorThrow(this.getClass().getName(), "doDelete", 
                		new TupleException(tuplemgr.mcat.getMessage("ET0005", location)));
            }

            BufferManager bufmgr = tuplemgr.bufmgr;
            TupleId tupleid = (TupleId) location;

            trx.acquireLock(
                location,
                LockMode.EXCLUSIVE,
                LockDuration.COMMIT_DURATION);

            PageId pageid = tupleid.getPageId();
            int slotNumber = tupleid.getSlotNumber();

            boolean validated = false;

            while (pageid != null) {
                int nextPage = -1;
                int nextSlot = -1;
                BufferAccessBlock bab = bufmgr.fixExclusive(
                    pageid,
                    false,
                    -1,
                    0);
                try {
                    SlottedPage page = (SlottedPage) bab.getPage();

                    if (!validated) {
                        if (page.getNumberOfSlots() <= slotNumber
                                || page.isSlotDeleted(slotNumber)
                                || TupleHelper.isDeleted(page, slotNumber)
                                || (TupleHelper.isSegmented(page, slotNumber) && !TupleHelper
                                    .isFirstSegment(page, slotNumber))) {
                        	tuplemgr.exceptionHandler.errorThrow(
                                this.getClass().getName(),
                                "doDelete",
                                new TupleException(tuplemgr.mcat.getMessage(
                                "ET0005",
                                location)));
                        }
                        validated = true;
                    }

                    UndoableReplaceSegmentLink logrec = new UndoableReplaceSegmentLink(
                        TupleManagerImpl.MODULE_ID,
                        TupleManagerImpl.TYPE_LOG_UNDOABLEREPLACESEGMENTLINK);
                    logrec.oldSegmentationFlags = page.getFlags(slotNumber);
                    logrec.newSegmentationFlags = page.getFlags(slotNumber)
                            | TupleHelper.TUPLE_DELETED;
                    logrec.slotNumber = slotNumber;

                    if (TupleHelper.isSegmented(page, slotNumber)) {
                    	TupleSegment ts = (TupleSegment) page.get(slotNumber, new TupleSegment.TupleSegmentFactory());
                        nextPage = logrec.oldNextSegmentPage = ts.nextPageNumber;
                        nextSlot = logrec.oldNextSegmentSlot = ts.nextSlotNumber;
                        logrec.nextSegmentPage = tupleid
                            .getPageId()
                            .getPageNumber();
                        logrec.nextSegmentSlotNumber = tupleid.getSlotNumber();
                    }

                    int tslength = page.getDataLength(slotNumber);

                    int freespace = page.getFreeSpace() + tslength;
                    int spacebitsBefore = TwoBitSpaceCheckerImpl
                        .mapFreeSpaceInfo(page.getSpace(), page.getFreeSpace());
                    int spacebitsAfter = TwoBitSpaceCheckerImpl
                        .mapFreeSpaceInfo(page.getSpace(), freespace);

                    Lsn lsn = trx.logInsert(page, logrec);
                    tuplemgr.redo(page, logrec);
                    bab.setDirty(lsn);

                    if (spacebitsBefore != spacebitsAfter) {
						tuplemgr.updateSpaceMapInfo(trx, containerId, page
								.getPageId().getPageNumber(), page
								.getSpaceMapPageNumber(), spacebitsAfter,
								spacebitsBefore);
					}
                } finally {
                    bab.unfix();
                }
                if (nextPage != -1) {
                    pageid = new PageId(pageid.getContainerId(), nextPage);
                    slotNumber = nextSlot;
                } else {
                    pageid = null;
                }
            }
        }

        public void delete(Transaction trx, Location location) {
            Savepoint savepoint = trx.createSavepoint(false);
            boolean success = false;
            try {
                doDelete(trx, location);
                success = true;
            } finally {
                if (!success) {
                    trx.rollback(savepoint);
                }
            }
        }

        public byte[] read(Location location) {

            if (location.getContainerId() != containerId) {
            	tuplemgr.exceptionHandler.errorThrow(this.getClass().getName(), "doRead", 
                		new TupleException(tuplemgr.mcat.getMessage("ET0005", location)));
            }

            BufferManager bufmgr = tuplemgr.bufmgr;
            TupleId tupleid = (TupleId) location;

            ByteArrayOutputStream os = new ByteArrayOutputStream();

            PageId pageid = tupleid.getPageId();
            int slotNumber = tupleid.getSlotNumber();

            boolean validated = false;

            while (pageid != null) {
                int nextPage = -1;
                int nextSlot = -1;
                BufferAccessBlock bab = bufmgr.fixShared(pageid, 0);
                try {
                    SlottedPage page = (SlottedPage) bab.getPage();

                    if (!validated) {
                        if (page.getNumberOfSlots() <= slotNumber
                                || page.isSlotDeleted(slotNumber)
                                || TupleHelper.isDeleted(page, slotNumber)
                                || (TupleHelper.isSegmented(page, slotNumber) && !TupleHelper
                                    .isFirstSegment(page, slotNumber))) {
                        	tuplemgr.exceptionHandler.errorThrow(this.getClass().getName(), "doRead", 
                            	new TupleException(tuplemgr.mcat.getMessage(
                                "ET0005",
                                location)));
                        }
                        validated = true;
                    }
                	TupleSegment ts = (TupleSegment) page.get(slotNumber, new TupleSegment.TupleSegmentFactory());

                    try {
                        os.write(ts.data);
                    } catch (IOException e) {
                    	tuplemgr.exceptionHandler.errorThrow(this.getClass().getName(), "doRead", 
                        		new TupleException(tuplemgr.mcat.getMessage("ET0006"), e));
                    }

                    if (TupleHelper.isSegmented(page, slotNumber)) {
                        nextPage = ts.nextPageNumber;
                        nextSlot = ts.nextSlotNumber;
                    }
                } finally {
                    bab.unfix();
                }
                if (nextPage != -1) {
                    pageid = new PageId(pageid.getContainerId(), nextPage);
                    slotNumber = nextSlot;
                } else {
                    pageid = null;
                }
            }
            return os.toByteArray();
        }

        /**
         * An update is carried out in two stages. In the first stage
         * existing tuple segments are updated with new data. If the new tuple is
         * smaller or equal to the size of existing tuple, then the process
         * ends here. If however, the new tuple is larger than the existing
         * tuple, then additional tuple segments are added, using the
         * tuple inserter mechanism used for inserting new tuples.
         * <p>
         * A drawback in current implementation is that existing tuple segments
         * are not resized. Therefore, when new segments are added for a tuple,
         * these segments may end up in pages that already contain other segments
         * of the same tuple. 
         * <p>
         * Another limitation of current implementation is that space occupied by
         * a tuple is not released until the tuple is deleted. Thus, a tuple that is
         * made smaller may end up with garbage bytes at the end - it is the
         * responsibility of the caller to know the useful size of the tuple.
         */
        void doUpdate(Transaction trx, Location location, Storable newTuple) {

            if (location.getContainerId() != containerId) {
            	tuplemgr.exceptionHandler.errorThrow(this.getClass().getName(), "doRead", 
                		new TupleException(tuplemgr.mcat.getMessage("ET0005", location)));
            }

            BufferManager bufmgr = tuplemgr.bufmgr;
            TupleId tupleid = (TupleId) location;

            trx.acquireLock(
                location,
                LockMode.EXCLUSIVE,
                LockDuration.COMMIT_DURATION);

            PageId pageid = tupleid.getPageId();
            int slotNumber = tupleid.getSlotNumber();

            int tupleLength = newTuple.getStoredLength();
            byte[] tupleData = new byte[tupleLength];
            ByteBuffer bb = ByteBuffer.wrap(tupleData);
            newTuple.store(bb);

            int remaining = tupleLength;
            int currentPos = 0;
            int segno = 0;

            int prevPage = -1;
            int prevSlot = -1;

            while (remaining > 0) {
                int nextPage = -1;
                int nextSlot = -1;
                BufferAccessBlock bab = bufmgr.fixExclusive(
                    pageid,
                    false,
                    -1,
                    0);
                try {
                    SlottedPage page = (SlottedPage) bab.getPage();

                	TupleSegment ts = (TupleSegment) page.get(slotNumber, new TupleSegment.TupleSegmentFactory());

                    if (TupleHelper.isSegmented(page, slotNumber)) {
                        nextPage = ts.nextPageNumber;
                        nextSlot = ts.nextSlotNumber;
                    }

                    int segmentSize = ts.data.length;

                    int copySize = segmentSize;
                    if (copySize > remaining) {
                        // FIXME Test case needed
                        copySize = remaining;
                    }
 
                    TupleSegment tsnew = new TupleSegment();
                    tsnew.data = new byte[segmentSize];
                    System.arraycopy(
                        tupleData,
                        currentPos,
                        tsnew.data,
                        0,
                        copySize);
                    currentPos += copySize;
                    remaining -= copySize;
                    tsnew.nextPageNumber = nextPage;
                    tsnew.nextSlotNumber = nextSlot;
 
                    UndoableUpdateTupleSegment logrec = new UndoableUpdateTupleSegment(
                        TupleManagerImpl.MODULE_ID,
                        TupleManagerImpl.TYPE_LOG_UNDOABLEUPDATETUPLESEGMENT);                   
                    logrec.slotNumber = slotNumber;
                    logrec.spaceMapPage = page.getSpaceMapPageNumber();
                    logrec.oldSegment = ts;
                    logrec.oldSegmentationFlags = page.getFlags(slotNumber);
                    logrec.segment = tsnew;

                    if (segno == 0 && remaining > 0) {
                        logrec.segmentationFlags = TupleHelper.TUPLE_SEGMENT
                                + TupleHelper.TUPLE_FIRST_SEGMENT;
                    } else if (segno > 0 && nextPage == -1 && remaining > 0) {
                        logrec.segmentationFlags = TupleHelper.TUPLE_SEGMENT;
                    } else {
                        logrec.segmentationFlags = logrec.oldSegmentationFlags;
                    }

                    Lsn lsn = trx.logInsert(page, logrec);
                    tuplemgr.redo(page, logrec);
                    bab.setDirty(lsn);

                } finally {
                    bab.unfix();
                }

                prevPage = pageid.getPageNumber();
                prevSlot = slotNumber;

                if (nextPage == -1 || remaining == 0) {
                    break;
                } else {
                    pageid = new PageId(pageid.getContainerId(), nextPage);
                    slotNumber = nextSlot;
                }
                segno++;
            }

            if (remaining > 0) {
                TupleInserterImpl inserter = new TupleInserterImpl(this, trx, newTuple, true);
                inserter.currentPos = currentPos;
                inserter.location = tupleid;
                inserter.prevSegmentPage = prevPage;
                inserter.prevSegmentSlotNumber = prevSlot;
                inserter.tupleData = tupleData;
                inserter.tupleLength = tupleLength;
                inserter.proceedWithInsert = true;
                inserter.completeInsert();
            }
        }

        public void update(Transaction trx, Location location, Storable newTuple) {
            Savepoint savepoint = trx.createSavepoint(false);
            boolean success = false;
            try {
                doUpdate(trx, location, newTuple);
                success = true;
            } finally {
                if (!success) {
                    trx.rollback(savepoint);
                }
            }
        }

        public TupleScan openScan(Transaction trx, boolean forUpdate) {
            return new TupleScanImpl(this, trx, forUpdate ? LockMode.UPDATE
                    : LockMode.SHARED);
        }
    }

    public static class TupleScanImpl implements TupleScan {

    	final TupleManagerImpl tuplemgr;
    	
        final TupleContainerImpl tupleContainer;

        final Transaction trx;

        final LockMode lockMode;

        int currentPage = -1;

        int currentSlot = -1;

        /**
         * A scan for finding used pages in the container.
         */
        FreeSpaceScan spaceScan;

        /**
         * This flag indicates whether EOF condition is true.
         */
        boolean eof = false;

        /**
         * Current tuple row, locked by this scan.
         */
        Location currentLocation = null;

        Location previousLocation = null;

        /**
         * Initializes the scan. 
         */
        void initScan() {
            if (spaceScan == null) {
                spaceScan = tupleContainer.tuplemgr.spaceMgr
                    .openScan(tupleContainer.containerId);
            }
        }

        public TupleScanImpl(TupleContainerImpl tupleContainer,
                Transaction trx, LockMode lockMode) {
            this.tupleContainer = tupleContainer;
            this.tuplemgr = tupleContainer.tuplemgr;
            this.trx = trx;
            this.lockMode = lockMode;
        }

        public boolean fetchNext() {
            if (previousLocation != null) {
                if (trx.getIsolationMode() == IsolationMode.CURSOR_STABILITY) {
                    LockMode lockMode = trx.hasLock(previousLocation);
                    if (lockMode == LockMode.SHARED
                            || lockMode == LockMode.UPDATE) {
                        if (tuplemgr.log.isDebugEnabled()) {
                        	tuplemgr.log.debug(
                                this.getClass().getName(),
                                "fetchNext",
                                "SIMPLEDBM-DEBUG: Releasing lock on previous row "
                                        + previousLocation);
                        }
                        trx.releaseLock(previousLocation);
                    }
                } else if ((trx.getIsolationMode() == IsolationMode.REPEATABLE_READ || trx
                    .getIsolationMode() == IsolationMode.SERIALIZABLE)
                        && lockMode == LockMode.UPDATE) {
                    /*
                     * This is an update mode cursor.
                     * In RR/SR mode, we need to downgrade UPDATE mode lock to SHARED lock when the cursor moves
                     * to the next row.
                     */
                    LockMode lockMode = trx.hasLock(previousLocation);
                    if (lockMode == LockMode.UPDATE) {
                        if (tuplemgr.log.isDebugEnabled()) {
                        	tuplemgr.log.debug(
                                this.getClass().getName(),
                                "fetchNext",
                                "SIMPLEDBM-DEBUG: Downgrading lock on previous row "
                                        + previousLocation + " to "
                                        + LockMode.SHARED);
                        }
                        trx.downgradeLock(previousLocation, LockMode.SHARED);
                    }
                }
            }
            boolean result = doFetchNext();
            if (!isEof() && currentLocation != null) {
                if (trx.getIsolationMode() == IsolationMode.READ_COMMITTED) {
                    LockMode lockMode = trx.hasLock(currentLocation);
                    if (lockMode == LockMode.SHARED
                            || lockMode == LockMode.UPDATE) {
                        trx.releaseLock(currentLocation);
                    }
                }
            }

            return result;
        }

        /**
         * Workhorse for performing the scan.
         */
        boolean doFetchNext() {
            if (eof) {
                /*
                 * Already hit EOF.
                 */
                // FIXME Test case needed
                return false;
            }
            initScan();
            Location nextLocation = null;

            final int FETCH_NEXT_PAGE = 1;
            final int FIND_NEXT_SLOT = 2;
            final int SLOT_FOUND = 3;
            final int LOCK_GRANTED = 4;

            int state;
            if (currentPage == -1) {
                state = FETCH_NEXT_PAGE;
            } else {
                state = FIND_NEXT_SLOT;
            }
            Lsn savedPageLsn = null;
            while (state != LOCK_GRANTED) {
                if (state == FETCH_NEXT_PAGE) {
                    if (spaceScan.fetchNext()) {
                        currentPage = spaceScan.getCurrentPage();
                        currentSlot = -1;
                        state = FIND_NEXT_SLOT;
                    } else {
                        eof = true;
                        return false;
                    }
                } else if (state == FIND_NEXT_SLOT) {
                    BufferAccessBlock bab = tupleContainer.tuplemgr.bufmgr
                        .fixShared(new PageId(
                            tupleContainer.containerId,
                            currentPage), 0);
                    try {
                        Page p = bab.getPage();
                        /*
                         * Since the space scan can return pages that are not data pages, we need
                         * to check that we have the correct page type.
                         */
                        if (p instanceof SlottedPage) {
                            SlottedPage page = (SlottedPage) p;
                            while (++currentSlot < page.getNumberOfSlots()) {
                                /*
                                 * Skip slots that are physcally deleted or that are trailing segments of 
                                 * a tuple.
                                 */
                                if (page.isSlotDeleted(currentSlot)
                                        || (TupleHelper.isSegmented(
                                            page,
                                            currentSlot) && !TupleHelper
                                            .isFirstSegment(page, currentSlot))) {
                                    continue;
                                }
                                /*
                                 * Potential candidate for scan
                                 */
                                nextLocation = new TupleId(
                                    page.getPageId(),
                                    currentSlot);
                                /*
                                 * Check if the tuple is logically deleted. If so, we need to lock the tuple to 
                                 * determine if the delete has been committed. In future, it may be worthwhile
                                 * to provide an option to skip deleted rows.
                                 */
                                if (TupleHelper.isDeleted(page, currentSlot)) {
                                    try {
                                        trx.acquireLockNowait(
                                            nextLocation,
                                            lockMode,
                                            LockDuration.INSTANT_DURATION);
                                        /*
                                         * Delete was committed. 
                                         */
                                        nextLocation = null;
                                        continue;
                                    } catch (LockException e) {
                                        /*
                                         * Delete is still uncommitted.
                                         */
                                        state = SLOT_FOUND;
                                    }
                                } else {
                                    try {
                                        /*
                                         * Try to acquire conditional lock because page is latched. If lock is
                                         * granted, we are done.
                                         */
                                        trx.acquireLockNowait(
                                            nextLocation,
                                            lockMode,
                                            LockDuration.MANUAL_DURATION);
                                        state = LOCK_GRANTED;
                                    } catch (LockException e) {
                                        /*
                                         * We need to wait for the lock unconditionally. However, before we can
                                         * do that, we must release the page latch.
                                         */
                                        state = SLOT_FOUND;
                                    }
                                }
                                savedPageLsn = page.getPageLsn();
                                break;
                            }
                            if (currentSlot == page.getNumberOfSlots()) {
                                state = FETCH_NEXT_PAGE;
                            }
                        } else {
                            // FIXME Test case needed
                            state = FETCH_NEXT_PAGE;
                        }
                    } finally {
                        bab.unfix();
                    }
                } else if (state == SLOT_FOUND) {
                    /*
                     * We have found a potential slot; this must be locked.
                     */
                    Savepoint sp = trx.createSavepoint(false);
                    //System.err.println("Found slot " + nextLocation + " is locked, so waiting");
                    trx.acquireLock(
                        nextLocation,
                        lockMode,
                        LockDuration.MANUAL_DURATION);
                    state = LOCK_GRANTED;
                    /*
                     * Since we released the page latch before acquiring the lock, we
                     * need to double check that the locked tuple is still eligible.
                     */
                    BufferAccessBlock bab = tupleContainer.tuplemgr.bufmgr
                        .fixShared(new PageId(
                            tupleContainer.containerId,
                            currentPage), 0);
                    try {
                        SlottedPage page = (SlottedPage) bab.getPage();
                        if (!savedPageLsn.equals(page.getPageLsn())) {
                            if (page.isSlotDeleted(currentSlot)
                                    || (TupleHelper.isSegmented(
                                        page,
                                        currentSlot) && !TupleHelper
                                        .isFirstSegment(page, currentSlot))
                                    || TupleHelper.isDeleted(page, currentSlot)) {
                                //System.err.println("Found slot " + nextLocation + " is no longer valid");
                                /*
                                 * The tuple is no longer valid, so we release the lock on the tuple
                                 * and start the search again.
                                 */
                                if (tuplemgr.log.isDebugEnabled()) {
                                	tuplemgr.log
                                        .debug(
                                            this.getClass().getName(),
                                            "doFetchNext",
                                            "SIMPLEDBM-DEBUG: The tuple at location "
                                                    + nextLocation
                                                    + " is no longer valid, hence it will be skipped and the cusror repositioned");
                                }
                                trx.rollback(sp);
                                state = FIND_NEXT_SLOT;
                            }
                        }
                    } finally {
                        bab.unfix();
                    }
                }
            }
            if (!eof) {
                assert nextLocation != null;
                previousLocation = currentLocation;
                currentLocation = nextLocation;
            }
            return !eof;
        }

        public byte[] getCurrentTuple() {
            if (currentLocation != null) {
                return tupleContainer.read(currentLocation);
            }
            return null;
        }

        public Location getCurrentLocation() {
            return currentLocation;
        }

        public boolean isEof() {
            return eof;
        }

        public void close() {
            if (!isEof() && currentLocation != null) {
                if (trx.getIsolationMode() == IsolationMode.READ_COMMITTED
                        || trx.getIsolationMode() == IsolationMode.CURSOR_STABILITY) {
                    LockMode lockMode = trx.hasLock(currentLocation);
                    if (lockMode == LockMode.SHARED
                            || lockMode == LockMode.UPDATE) {
                        if (tuplemgr.log.isDebugEnabled()) {
                        	tuplemgr.log
                                .debug(
                                    this.getClass().getName(),
                                    "close",
                                    "SIMPLEDBM-DEBUG: Releasing lock on current row "
                                            + currentLocation
                                            + " because isolation mode = CS or RC or (RR and EOF) and mode = SHARED or UPDATE");
                        }
                        trx.releaseLock(currentLocation);
                    }
                }
            }
            spaceScan.close();
        }

    }

    /**
     * Implements a two-bit Space Checker suitable for use in containers that
     * use two bits to maintain space information about individual pages. The
     * system assumes following:
     * <ol>
     * <li>0 indicates empty page. Possible only when entire space is
     * available.</li>
     * <li>1 indicates page that is up to one-third full.</li>
     * <li>2 indicates page that is up to two-thirds full.</li>
     * <li>3 indicates page that is full or has used more than two-thirds of
     * space.</li>
     * </ol>
     * 
     * @author Dibyendu Majumdar
     * @since 13-Dec-2005
     */
    public static class TwoBitSpaceCheckerImpl implements FreeSpaceChecker {

        /**
         * By default, searches will be for completely empty pages.
         */
        int searchValue = 0;

        /**
         * Creates an instance of space checker that will search for specified
         * amount of free space.
         * 
         * @param availableSpace
         *            Maximum space available in a page (including used space)
         * @param desiredFreeSpace
         *            Desired amount of free space
         */
        public TwoBitSpaceCheckerImpl(int availableSpace, int desiredFreeSpace) {
            initSearch(availableSpace, desiredFreeSpace);
        }

        /**
         * Default space checker will search for empty pages.
         */
        public TwoBitSpaceCheckerImpl() {
        }

        /**
         * Initialize a search based upon desired space information.
         * 
         * @param availableSpace
         *            Maximum space available in a page (including used space)
         * @param desiredFreeSpace
         *            Desired amount of free space
         */
        TwoBitSpaceCheckerImpl initSearch(int availableSpace,
                int desiredFreeSpace) {
            final int onethird = availableSpace / 3;
            final int twothird = onethird * 2;
            if (desiredFreeSpace > twothird) {
                searchValue = 0;
            } else if (desiredFreeSpace > onethird) {
                searchValue = 1;
            } else {
                searchValue = 2;
            }
            return this;
        }

        /**
         * Test whether specified value satisfies search condition.
         * 
         * @see org.simpledbm.rss.api.fsm.FreeSpaceChecker#hasSpace(int)
         */
        public boolean hasSpace(int value) {
            return value <= searchValue;
        }

        /**
         * Translate free space information into a value ranging from 0-3 (2
         * bits). This value is suitable as a space indicator within a Space Map
         * page.
         * <ol>
         * <li>0 indicates empty page. Possible only when entire space is
         * available.</li>
         * <li>1 indicates page that is up to one-third full.</li>
         * <li>2 indicates page that is up to two-thirds full.</li>
         * <li>3 indicates page that is full or has used more than two-thirds
         * of space.</li>
         * </ol>
         */
        static int mapFreeSpaceInfo(int availableSize, int freeSpace) {
            final int onethird = availableSize / 3;
            final int twothird = onethird * 2;
            if (freeSpace >= availableSize) {
                return 0;
            } else if (freeSpace > twothird) {
                return 1;
            } else if (freeSpace > onethird) {
                return 2;
            } else {
                return 3;
            }
        }
    }

    /**
     * Utility to help with flags associated with segments. A
     * {@link org.simpledbm.rss.api.sp.SlottedPage} allows flags to be associated with
     * each slot. This is taken advantage of here to maintain various status
     * flags for tuple segments.
     */
    public static class TupleHelper {

        /**
         * If set, indicates that this is a segment of a tuple.
         */
        static int TUPLE_SEGMENT = 1;

        /**
         * If set, indicates that this segment is the first segment of a tuple.
         * TUPLE_SEGMENT should also be set.
         */
        static int TUPLE_FIRST_SEGMENT = 2;

        /**
         * If set, indicates that this is the last segment of the tuple.
         * TUPLE_SEGMENT should also be set.
         */
        static int TUPLE_LAST_SEGMENT = 4;

        /**
         * If set, indicates that this tuple is marked for deletion.
         */
        static int TUPLE_DELETED = 8;

        /**
         * Checks whether a particular slot is marked as a tuple segment.
         */
        static boolean isSegmented(SlottedPage page, int slot) {
            int flags = page.getFlags(slot);
            return (flags & TUPLE_SEGMENT) != 0;
        }

        /**
         * Checks whether a particular slot is marked as the first segment of a
         * tuple.
         */
        static boolean isFirstSegment(SlottedPage page, int slot) {
            int flags = page.getFlags(slot);
            return (flags & TUPLE_FIRST_SEGMENT) != 0;
        }

        /**
         * Checks whether a particular slot is marked as the last segment of a
         * tuple.
         */
        static boolean isLastSegment(SlottedPage page, int slot) {
            int flags = page.getFlags(slot);
            return (flags & TUPLE_LAST_SEGMENT) != 0;
        }

        /**
         * Checks whether a particular slot is marked as a logically deleted
         * tuple.
         */
        static boolean isDeleted(SlottedPage page, int slot) {
            int flags = page.getFlags(slot);
            return (flags & TUPLE_DELETED) != 0;
        }
    }

    /**
     * A Tuple is split up into segments when it is stored in the
     * TupleContainer. This allows the system to support tuples that are larger
     * than a single page. Segments are linked together so that it is possible
     * to follow the chain of segments once the initial segment is found.
     * 
     * @author Dibyendu Majumdar
     * @since 08-Dec-2005
     */
    public static class TupleSegment implements Storable, Dumpable {

        /**
         * Used only in segmented tuples. Normally, points to the location of
         * next segment. If a tuple is deleted, this is updated to the page
         * number of the first segment.
         */
        int nextPageNumber = -1;

        /**
         * Used only in segmented tuples. Normally, points to the location of
         * next segment. If a tuple is deleted, this is updated to the slot
         * number of the first segment.
         */
        int nextSlotNumber = -1;

        /**
         * Data in this segment.
         */
        byte[] data = new byte[0];

        public TupleSegment() {
        }

        public TupleSegment(ByteBuffer bb) {
            nextPageNumber = bb.getInt();
            nextSlotNumber = bb.getShort();
            int length = bb.getShort();
            data = new byte[length];
            bb.get(data);
        }
        
        public TupleSegment(byte[] data, int nextPageNumber, int nextSlotNumber) {
            this.data = data;
            this.nextPageNumber = nextPageNumber;
            this.nextSlotNumber = nextSlotNumber;
        }

        public int overhead() {
            int n = TypeSize.INTEGER;
            n += TypeSize.SHORT * 2;
            return n;
        }

        public int usableSpace(int totalSpace) {
            return totalSpace - overhead();
        }

        public void store(ByteBuffer bb) {
            bb.putInt(nextPageNumber);
            bb.putShort((short) nextSlotNumber);
            bb.putShort((short) data.length);
            bb.put(data);
        }

        public final TupleSegment cloneMe() {
            TupleSegment clone = new TupleSegment();
            clone.nextPageNumber = nextPageNumber;
            clone.nextSlotNumber = nextSlotNumber;
            clone.data = new byte[data.length];
            System.arraycopy(data, 0, clone.data, 0, data.length);
            return clone;
        }

        public int getStoredLength() {
            return overhead() + data.length;
        }

        public final StringBuilder appendTo(StringBuilder sb) {
            sb.append("TupleSegment(len=").append(data.length).append(
                ", nextPointer(").append(nextPageNumber).append(", ").append(
                nextSlotNumber).append("))");
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
        static final class TupleSegmentFactory implements StorableFactory {

			public Storable getStorable(ByteBuffer buf) {
				return new TupleSegment(buf);
			}
        	
        }
    }

    /**
     * Logs the physical delete of a particular slot within the page.
     * <p>
     * When tuples are deleted, the slots used by the tuple are marked logically
     * deleted. The slots remain allocated until some other transaction needs
     * to use the space in the page, at which point, provided that the original
     * transaction performing the delete has committed, the slots are physically
     * deleted. 
     * 
     * @author Dibyendu Majumdar
     * @since 12-Dec-2005
     */
    public static class DeleteSlot extends BaseLoggable implements Redoable {

    	/*
    	 * TODO: Instead of deleting one slot at a time, we should delete
    	 * all slots in one operation.
    	 */
    	
        /**
         * Slot to be deleted.
         */
        int slotNumber;

        public DeleteSlot(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public DeleteSlot(ByteBuffer bb) {
			super(bb);
            slotNumber = bb.getShort();
		}

        @Override
        public int getStoredLength() {
            return super.getStoredLength() + TypeSize.SHORT;
        }

        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            bb.putShort((short) slotNumber);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("DeleteSlot(");
            super
                .appendTo(sb)
                .append(", slotNumber=")
                .append(slotNumber)
                .append(")");
            return sb;
        }

        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
        static final class DeleteSlotFactory implements ObjectFactory {

			public Class<?> getType() {
				return DeleteSlot.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new DeleteSlot(buf);
			}
        	
        }
    }

    /**
     * Logs the insertion of a tuple segment. Large tuples are broken down into
     * segments across pages.
     */
    public static class BaseInsertTupleSegment extends BaseLoggable {

        /**
         * Slot where the segment is to be inserted.
         */
        int slotNumber;

        /**
         * Segment data.
         */
        TupleSegment segment;

        /**
         * Flags associated with the segment.
         */
        int segmentationFlags = 0;

        /**
         * Update space map page info.
         */
        int spaceMapPage;

        protected BaseInsertTupleSegment(int moduleId, int typeCode) {
        	super(moduleId, typeCode);
        }
        
        protected BaseInsertTupleSegment(ByteBuffer bb) {
            super(bb);
            segment = new TupleSegment(bb);
            //segment.retrieve(bb);
            slotNumber = bb.getShort();
            segmentationFlags = bb.getShort();
            spaceMapPage = bb.getInt();
        }
        
        @Override
        public int getStoredLength() {
            return super.getStoredLength() + segment.getStoredLength()
                    + TypeSize.SHORT * 2 + TypeSize.INTEGER;
        }

        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            segment.store(bb);
            bb.putShort((short) slotNumber);
            bb.putShort((short) segmentationFlags);
            bb.putInt(spaceMapPage);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            super.appendTo(sb).append("slotNumber=").append(slotNumber).append(
                ", segmentationFlags=").append(segmentationFlags).append(
                ", spaceMapPage=").append(spaceMapPage);
            return sb;
        }

        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
    }

    /**
     * Logs the insertion of a tuple segment. Large tuples are broken down into
     * segments across pages.
     */
    public static class InsertTupleSegment extends BaseInsertTupleSegment
            implements Undoable, LogicalUndo {
    	
        public InsertTupleSegment(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public InsertTupleSegment(ByteBuffer bb) {
			super(bb);
		}

		public StringBuilder appendTo(StringBuilder sb) {
            sb.append("InsertTupleSegment(");
            super.appendTo(sb).append(")");
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
        static final class InsertTupleSegmentFactory implements ObjectFactory {

			public Class<?> getType() {
				return InsertTupleSegment.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new InsertTupleSegment(buf);
			}
        	
        }
    }

    /**
     * Logs the undo of a tuple segment insert. The undo causes the slot to be
     * physically deleted.
     */
    public static class UndoInsertTupleSegment extends DeleteSlot implements
            Compensation {
    	
        public UndoInsertTupleSegment(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public UndoInsertTupleSegment(ByteBuffer bb) {
			super(bb);
		}

		public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UndoInsertTupleSegment(");
            super.appendTo(sb).append(")");
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class UndoInsertTupleSegmentFactory implements ObjectFactory {

			public Class<?> getType() {
				return UndoInsertTupleSegment.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new UndoInsertTupleSegment(buf);
			}
        	
        }
    }

    /**
     * Logs the update of a tuple segment.
     */
    public static class UndoableUpdateTupleSegment extends
            BaseInsertTupleSegment implements Undoable {
        /**
         * Old value of segmentation flag is saved here.
         */
        int oldSegmentationFlags;

        /**
         * Old image of tuple segment is saved here.
         */
        TupleSegment oldSegment;

        public UndoableUpdateTupleSegment(int moduleId, int typeCode) {
        	super(moduleId, typeCode);
        }

        public UndoableUpdateTupleSegment(ByteBuffer bb) {
        	super(bb);
            oldSegmentationFlags = bb.getShort();
            oldSegment = new TupleSegment(bb);
            // oldSegment.retrieve(bb);
        }

        @Override
        public int getStoredLength() {
            return super.getStoredLength() + TypeSize.SHORT
                    + oldSegment.getStoredLength();
        }

        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            bb.putShort((short) oldSegmentationFlags);
            oldSegment.store(bb);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UndoableUpdateTupleSegment(");
            super.appendTo(sb).append(", oldSegmentationFlags=").append(
                oldSegmentationFlags).append(")");
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
        static final class UndoableUpdateTupleSegmentFactory implements ObjectFactory {

			public Class<?> getType() {
				return UndoableUpdateTupleSegment.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new UndoableUpdateTupleSegment(buf);
			}
        	
        }        
    }

    /**
     * Logs the undo of an update tuple segment operation.
     */
    public static class UndoUpdateTupleSegment extends BaseInsertTupleSegment
            implements Compensation {
    	
    	public UndoUpdateTupleSegment(int moduleId, int typeCode) {
    		super(moduleId, typeCode);
    	}

        public UndoUpdateTupleSegment(ByteBuffer bb) {
			super(bb);
		}

		public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UndoUpdateTupleSegment(");
            super.appendTo(sb).append(")");
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
        static final class UndoUpdateTupleSegmentFactory implements ObjectFactory {

			public Class<?> getType() {
				return UndoUpdateTupleSegment.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new UndoUpdateTupleSegment(buf);
			}
        }                
    }

    /**
     * Logs the link from one segment to another.
     * 
     * @author Dibyendu Majumdar
     * @since 12-Dec-2005
     */
    public static class BaseUpdateSegmentLink extends BaseLoggable {

        /**
         * Slot containing tuple segment that needs to be updated.
         */
        int slotNumber;

        /**
         * Pointer to next tuple segment's page.
         */
        int nextSegmentPage;

        /**
         * Pointer to next tuple segment's slot.
         */
        int nextSegmentSlotNumber;

        protected BaseUpdateSegmentLink(int moduleId, int typeCode) {
        	super(moduleId, typeCode);
        }
        
        protected BaseUpdateSegmentLink(ByteBuffer bb) {
            super(bb);
            slotNumber = bb.getShort();
            nextSegmentPage = bb.getInt();
            nextSegmentSlotNumber = bb.getShort();
        }
        
        @Override
        public int getStoredLength() {
            return super.getStoredLength() + TypeSize.INTEGER + TypeSize.SHORT
                    * 2;
        }

        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            bb.putShort((short) slotNumber);
            bb.putInt(nextSegmentPage);
            bb.putShort((short) nextSegmentSlotNumber);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            super.appendTo(sb);
            sb.append(", slotNumber=").append(slotNumber).append(
                ", nextSegmentPage=").append(nextSegmentPage).append(
                ", nextSegmentSlot=").append(nextSegmentSlotNumber);
            return sb;
        }

        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
    }

    public static class ExtendedUpdateSegmentLink extends BaseUpdateSegmentLink {
        int newSegmentationFlags;

        public ExtendedUpdateSegmentLink(int moduleId, int typeCode) {
        	super(moduleId, typeCode);
        }
        
        public ExtendedUpdateSegmentLink(ByteBuffer bb) {
            super(bb);
            newSegmentationFlags = bb.getShort();
        }
        
        @Override
        public int getStoredLength() {
            return super.getStoredLength() + TypeSize.SHORT;
        }

        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            bb.putShort((short) newSegmentationFlags);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            super.appendTo(sb);
            sb.append("newSegmentationFlags=").append(newSegmentationFlags);
            return sb;
        }

        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
    }

    /**
     * UndoableReplaceSegmentLink is used during tuple deletes to replace the segment link with
     * Location information and to mark the segment as logically deleted. It is also used during 
     * tuple updates to replace the old last segment's link - because this update must be
     * undoable, and also must update the segmentation flags.
     * @see TupleContainerImpl#doDelete(Transaction, Location)
     * @see TupleInserterImpl#updatePrevSegment(int, int)
     */
    public static class UndoableReplaceSegmentLink extends
            ExtendedUpdateSegmentLink implements Undoable, LogicalUndo {

        int oldNextSegmentPage;

        int oldNextSegmentSlot;

        int oldSegmentationFlags;

        public UndoableReplaceSegmentLink(int moduleId, int typeCode) {
        	super(moduleId, typeCode);
        }
        
        public UndoableReplaceSegmentLink(ByteBuffer bb) {
            super(bb);
            oldNextSegmentPage = bb.getInt();
            oldNextSegmentSlot = bb.getShort();
            oldSegmentationFlags = bb.getShort();
        }
        
        @Override
        public int getStoredLength() {
            return super.getStoredLength() + TypeSize.INTEGER + TypeSize.SHORT
                    * 2;
        }

        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(oldNextSegmentPage);
            bb.putShort((short) oldNextSegmentSlot);
            bb.putShort((short) oldSegmentationFlags);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UndoableReplaceSegmentLink(");
            super.appendTo(sb);
            sb.append(", oldSegmentationFlags=").append(oldSegmentationFlags);
            sb.append(", oldNextSegmentPage=").append(oldNextSegmentPage);
            sb.append(", oldNextSegmentSlot=").append(oldNextSegmentSlot);
            sb.append(")");
            return sb;
        }

        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class UndoableReplaceSegmentLinkFactory implements ObjectFactory {

			public Class<?> getType() {
				return UndoableReplaceSegmentLink.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new UndoableReplaceSegmentLink(buf);
			}
        	
        }
    }

    /**
     * UndoReplaceSegmentLink is used to log undo of link updates
     * caused during tuple deletes, and tuple updates.
     */
    public static class UndoReplaceSegmentLink extends
            ExtendedUpdateSegmentLink implements Compensation {

    	public UndoReplaceSegmentLink(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public UndoReplaceSegmentLink(ByteBuffer bb) {
			super(bb);
		}

		public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UndoReplaceSegmentLink(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        static final class UndoReplaceSegmentLinkFactory implements ObjectFactory {

			public Class<?> getType() {
				return UndoReplaceSegmentLink.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new UndoReplaceSegmentLink(buf);
			}
        }    
    }

    /**
     * During inserts, segment links are updated using redo-only log records, because
     * there is no need to undo these operations. During updates too, these log records are
     * used for linking new segments, except for the case where the old last segment is 
     * being linked to a new segment. In this special case, the UndoableReplaceSegmentLink 
     * is used, which also logs the segmentation flags.
     * <p>
     * TODO: Check what happens in failure scenarios similar to those of
     * space map updates.
     * @see TupleInserterImpl#updatePrevSegment(int, int)
     * @see UndoableReplaceSegmentLink
     */
    public static class UpdateSegmentLink extends BaseUpdateSegmentLink
            implements Redoable {

    	public UpdateSegmentLink(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public UpdateSegmentLink(ByteBuffer bb) {
			super(bb);
		}

		public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UpdateSegmentLink(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
        static final class UpdateSegmentLinkFactory implements ObjectFactory {

			public Class<?> getType() {
				return UpdateSegmentLink.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new UpdateSegmentLink(buf);
			}
        }
    }
}
