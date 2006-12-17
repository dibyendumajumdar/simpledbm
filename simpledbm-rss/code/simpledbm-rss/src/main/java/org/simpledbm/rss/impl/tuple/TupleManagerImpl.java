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
 *    Email  : dibyendu@mazumdar.demon.co.uk
 */
/*
 * Created on: 08-Dec-2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.rss.impl.tuple;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.simpledbm.rss.api.bm.BufferManager;
import org.simpledbm.rss.api.bm.BufferManagerException;
import org.simpledbm.rss.api.bm.BufferAccessBlock;
import org.simpledbm.rss.api.fsm.FreeSpaceChecker;
import org.simpledbm.rss.api.fsm.FreeSpaceCursor;
import org.simpledbm.rss.api.fsm.FreeSpaceManager;
import org.simpledbm.rss.api.fsm.FreeSpaceManagerException;
import org.simpledbm.rss.api.fsm.FreeSpaceMapPage;
import org.simpledbm.rss.api.fsm.FreeSpaceScan;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.loc.LocationFactory;
import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockMode;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.sp.SlottedPage;
import org.simpledbm.rss.api.sp.SlottedPageManager;
import org.simpledbm.rss.api.st.Storable;
import org.simpledbm.rss.api.tuple.TupleContainer;
import org.simpledbm.rss.api.tuple.TupleException;
import org.simpledbm.rss.api.tuple.TupleInserter;
import org.simpledbm.rss.api.tuple.TupleManager;
import org.simpledbm.rss.api.tuple.TupleScan;
import org.simpledbm.rss.api.tx.BaseLoggable;
import org.simpledbm.rss.api.tx.BaseTransactionalModule;
import org.simpledbm.rss.api.tx.Compensation;
import org.simpledbm.rss.api.tx.LoggableFactory;
import org.simpledbm.rss.api.tx.LogicalUndo;
import org.simpledbm.rss.api.tx.Redoable;
import org.simpledbm.rss.api.tx.Savepoint;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.api.tx.TransactionalModuleRegistry;
import org.simpledbm.rss.api.tx.TransactionException;
import org.simpledbm.rss.api.tx.Undoable;
import org.simpledbm.rss.api.wal.Lsn;
import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.rss.util.logging.Logger;

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
 * location will fail. The segments wil get eventually reused, when the transaction
 * that locked the tuple commits.</li>
 * </ol>
 * 
 * @author Dibyendu Majumdar
 * @since 08-Dec-2005
 */
public class TupleManagerImpl extends BaseTransactionalModule implements TupleManager {

	static final String LOG_CLASS_NAME = TupleManagerImpl.class.getName();

	static final Logger log = Logger.getLogger(TupleManagerImpl.class.getPackage().getName());

	private static final short MODULE_ID = 6;

	private static final short TYPE_BASE = MODULE_ID * 100;

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

	final FreeSpaceManager spaceMgr;

	final BufferManager bufmgr;

	final SlottedPageManager spMgr;

	final PageFactory pageFactory;

	final TupleIdFactory locationFactory = new TupleIdFactory();

	public TupleManagerImpl(ObjectRegistry objectFactory, LoggableFactory loggableFactory, FreeSpaceManager spaceMgr, BufferManager bufMgr, SlottedPageManager spMgr, TransactionalModuleRegistry moduleRegistry, PageFactory pageFactory) {
		this.objectFactory = objectFactory;
		this.loggableFactory = loggableFactory;
		this.spaceMgr = spaceMgr;
		this.bufmgr = bufMgr;
		this.spMgr = spMgr;
		this.pageFactory = pageFactory;

		moduleRegistry.registerModule(MODULE_ID, this);

		objectFactory.register(TYPE_LOCATIONFACTORY, locationFactory);
		objectFactory.register(TYPE_LOG_DELETESLOT, DeleteSlot.class.getName());
		objectFactory.register(TYPE_LOG_INSERTTUPLESEGMENT, InsertTupleSegment.class.getName());
		objectFactory.register(TYPE_LOG_UNDOINSERTTUPLESEGMENT, UndoInsertTupleSegment.class.getName());
		objectFactory.register(TYPE_LOG_UPDATESEGMENTLINK, UpdateSegmentLink.class.getName());
		objectFactory.register(TYPE_LOG_UNDOABLEREPLACESEGMENTLINK, UndoableReplaceSegmentLink.class.getName());
		objectFactory.register(TYPE_LOG_UNDOREPLACESEGMENTLINK, UndoReplaceSegmentLink.class.getName());
		objectFactory.register(TYPE_LOG_UNDOABLEUPDATETUPLESEGMENT, UndoableUpdateTupleSegment.class.getName());
		objectFactory.register(TYPE_LOG_UNDOUPDATETUPLESEGMENT, UndoUpdateTupleSegment.class.getName());
	}

	@Override
	public Compensation generateCompensation(Undoable undoable) throws Exception {
		if (undoable instanceof InsertTupleSegment) {
			InsertTupleSegment logrec = (InsertTupleSegment) undoable;
			UndoInsertTupleSegment clr = (UndoInsertTupleSegment) loggableFactory.getInstance(MODULE_ID, TYPE_LOG_UNDOINSERTTUPLESEGMENT);
			clr.slotNumber = logrec.slotNumber;
			return clr;
		} else if (undoable instanceof UndoableReplaceSegmentLink) {
			UndoableReplaceSegmentLink logrec = (UndoableReplaceSegmentLink) undoable;
			UndoReplaceSegmentLink clr = (UndoReplaceSegmentLink) loggableFactory.getInstance(MODULE_ID, TYPE_LOG_UNDOREPLACESEGMENTLINK);
			clr.slotNumber = logrec.slotNumber;
			clr.newSegmentationFlags = logrec.oldSegmentationFlags;
			clr.nextSegmentPage = logrec.oldNextSegmentPage;
			clr.nextSegmentSlotNumber = logrec.oldNextSegmentSlot;
			return clr;
		} else if (undoable instanceof UndoableUpdateTupleSegment) {
			// FIXME Test case needed
			UndoableUpdateTupleSegment logrec = (UndoableUpdateTupleSegment) undoable;
			UndoUpdateTupleSegment clr = (UndoUpdateTupleSegment) loggableFactory.getInstance(MODULE_ID, TYPE_LOG_UNDOUPDATETUPLESEGMENT);
			clr.slotNumber = logrec.slotNumber;
			clr.spaceMapPage = logrec.spaceMapPage;
			clr.segment = logrec.oldSegment;
			clr.segmentationFlags = logrec.oldSegmentationFlags;
			return clr;
		}
		return null;
	}

	@Override
	public void undo(Transaction trx, Undoable undoable) throws Exception {
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
	public void undoInsertSegment(Transaction trx, Undoable undoable) throws Exception {
		PageId pageId = undoable.getPageId();
		InsertTupleSegment logrec = (InsertTupleSegment) undoable;
		int spaceMapPage = logrec.spaceMapPage;

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
			int spacebitsBefore = TwoBitSpaceCheckerImpl.mapFreeSpaceInfo(page.getSpace(), page.getFreeSpace());
			redo(page, clr);
			int spacebitsAfter = TwoBitSpaceCheckerImpl.mapFreeSpaceInfo(page.getSpace(), page.getFreeSpace());

			if (spacebitsAfter != spacebitsBefore) {
				/*
				 * Requires space map page update.
				 */
				FreeSpaceCursor spcursor = spaceMgr.getSpaceCursor(pageId.getContainerId());
				spcursor.fixSpaceMapPageExclusively(spaceMapPage, pageId.getPageNumber());
				try {
					spcursor.updateAndLogRedoOnly(trx, pageId.getPageNumber(), spacebitsAfter);
					if (log.isDebugEnabled()) {
						log.debug(LOG_CLASS_NAME, "undoInsertSegment", "SIMPLEDBM-LOG: Updated space map information from " + spacebitsBefore + " to " + spacebitsAfter + " for page " + pageId + " in space map page " + spaceMapPage + " as a result of " + clr);
					}
				} finally {
					spcursor.unfixCurrentSpaceMapPage();
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
	public void undoDeleteSegment(Transaction trx, Undoable undoable) throws Exception {
		PageId pageId = undoable.getPageId();
		UndoableReplaceSegmentLink logrec = (UndoableReplaceSegmentLink) undoable;
		BufferAccessBlock bab = bufmgr.fixExclusive(pageId, false, -1, 0);
		try {
			SlottedPage page = (SlottedPage) bab.getPage();
			Compensation clr = generateCompensation(undoable);
			clr.setUndoNextLsn(undoable.getPrevTrxLsn());

			TupleSegment ts = new TupleSegment();
			page.get(logrec.slotNumber, ts);
			int tslength = ts.getStoredLength();
			/*
			 * Since we need to log the space map update before logging the 
			 * page update, we apply the log record, calculate the space map
			 * changes, log the space map update, and then log the page update.
			 */
			int spacebitsBefore = TwoBitSpaceCheckerImpl.mapFreeSpaceInfo(page.getSpace(), page.getFreeSpace() + tslength);
			redo(page, clr);
			int spacebitsAfter = TwoBitSpaceCheckerImpl.mapFreeSpaceInfo(page.getSpace(), page.getFreeSpace());

			if (spacebitsAfter != spacebitsBefore) {
				/*
				 * Requires space map page update.
				 */
				FreeSpaceCursor spcursor = spaceMgr.getSpaceCursor(pageId.getContainerId());
				spcursor.fixSpaceMapPageExclusively(page.getSpaceMapPageNumber(), pageId.getPageNumber());
				try {
					spcursor.updateAndLogRedoOnly(trx, pageId.getPageNumber(), spacebitsAfter);
					if (log.isDebugEnabled()) {
						log.debug(LOG_CLASS_NAME, "undoDeleteSegment", "Updated space map information from " + spacebitsBefore + " to " + spacebitsAfter + " for page " + pageId + " in space map page " + page.getSpaceMapPageNumber() + " as a result of " + clr);
					}
				} finally {
					spcursor.unfixCurrentSpaceMapPage();
				}
			}

			Lsn lsn = trx.logInsert(page, clr);
			bab.setDirty(lsn);
		} finally {
			bab.unfix();
		}
	}

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
			TupleSegment ts = new TupleSegment();
			p.get(logrec.slotNumber, ts);
			ts.nextPageNumber = logrec.nextSegmentPage;
			ts.nextSlotNumber = logrec.nextSegmentSlotNumber;
			p.insertAt(logrec.slotNumber, ts, true);
			p.setFlags(logrec.slotNumber, (short) logrec.newSegmentationFlags);
		} else if (loggable instanceof UpdateSegmentLink) {
			UpdateSegmentLink logrec = (UpdateSegmentLink) loggable;
			TupleSegment ts = new TupleSegment();
			p.get(logrec.slotNumber, ts);
			ts.nextPageNumber = logrec.nextSegmentPage;
			ts.nextSlotNumber = logrec.nextSegmentSlotNumber;
			p.insertAt(logrec.slotNumber, ts, true);
		}
		p.dump();
	}

	public void createTupleContainer(Transaction trx, String name, int containerId, int extentSize) {
		// TODO Auto-generated method stub
	}

	public TupleContainer getTupleContainer(int containerId) {
		return new TupleContainerImpl(this, containerId);
	}

	public LocationFactory getLocationFactory() {
		return locationFactory;
	}

	public int getLocationFactoryType() {
		return TYPE_LOCATIONFACTORY;
	}

	/**
	 * Handles the insert of a tuple. Creates tuple segments to hold
	 * the tuple data and links the segments together in a singly linked list.
	 */
	public static class TupleInserterImpl implements TupleInserter {

		private static String LOG_CLASS_NAME = TupleInserter.class.getName();
		
		TupleContainerImpl tupleContainer;

		Transaction trx;

		Storable tuple;

		TupleId location;

		int containerId;

		int tupleLength;

		int spaceMapPage;

		int currentPos = 0;

		byte[] tupleData;

		int prevSegmentPage;

		int prevSegmentSlotNumber;

		boolean updateMode = false;

		public TupleInserterImpl(TupleContainerImpl relation) {
			this.tupleContainer = relation;
		}

		/**
		 * Physically deletes any slots that are left over from previous
		 * deletes. The operation is logged.
		 */
		void reclaimDeletedTuples(Transaction trx, BufferAccessBlock bab) throws TransactionException {
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
					// FIXME Test case needed
					TupleId location;

					if (!TupleHelper.isSegmented(page, slotNumber) || TupleHelper.isFirstSegment(page, slotNumber)) {
						location = new TupleId(page.getPageId(), slotNumber);
					} else {
						/*
						 * If the slot is part of a tuple that is segmented, and
						 * if is not the first segment, then we need to read the
						 * tuple segment to determine the Location to which this
						 * segment belogs. The location is stored in the link field.
						 */
						TupleSegment ts = new TupleSegment();
						page.get(slotNumber, ts);
						PageId pageId = new PageId(page.getPageId().getContainerId(), ts.nextPageNumber);
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
						 * within this transaction, and therefore the tuple
						 * segment cannot be reclaimed. Or the transaction is
						 * currently reusing this tuple location as part of an
						 * insert.
						 */
						if (log.isDebugEnabled()) {
							log.debug(LOG_CLASS_NAME, "reclaimDeletedTuples", "Reclaim of tuple segment at (page=" + page.getPageId() + ", slot=" + slotNumber + ") skipped as the associated tuple " + location + " is locked in this transaction");
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
						trx.acquireLockNowait(location, LockMode.EXCLUSIVE, LockDuration.INSTANT_DURATION);
						reclaim = true;
					} catch (TransactionException.LockException e) {
						if (log.isDebugEnabled()) {
							log.debug(LOG_CLASS_NAME, "reclaimDeletedTuples", "Reclaim of tuple segment at (page=" + page.getPageId() + ", slot=" + slotNumber + ") skipped because cannot obtain lock on " + location, e);
						}
					}
					if (reclaim) {
						if (log.isDebugEnabled()) {
							log.debug(LOG_CLASS_NAME, "reclaimDeletedTuples", "Reclaiming tuple segment at (page=" + page.getPageId() + ", slot=" + slotNumber + ") associated with tuple at " + location);
						}
						DeleteSlot logrec = (DeleteSlot) tupleContainer.tuplemgr.loggableFactory.getInstance(TupleManagerImpl.MODULE_ID, TupleManagerImpl.TYPE_LOG_DELETESLOT);
						logrec.slotNumber = slotNumber;
						Lsn lsn = trx.logInsert(page, logrec);
						tupleContainer.tuplemgr.redo(page, logrec);
						bab.setDirty(lsn);
						slotsDeleted++;
					}
				}
			}
		}

		/**
		 * Start an insert. Gets the Location that has been reserved for the new
		 * tuple. In case the tuple data extends to multiple pages, the Location
		 * is a pointer to the first page/slot. Location will be locked
		 * exclusively before this method returns.
		 * <p>
		 * Returns true if successful, false if client should retry operation.
		 */
		boolean doStartInsert() throws FreeSpaceManagerException, TupleException, BufferManagerException, TransactionException {

			TupleManagerImpl relmgr = tupleContainer.tuplemgr;

			containerId = tupleContainer.containerId;

			SlottedPage emptyPage = tupleContainer.emptyPage;

			FreeSpaceManager spacemgr = relmgr.spaceMgr;
			FreeSpaceCursor spcursor = spacemgr.getSpaceCursor(containerId);

			tupleLength = tuple.getStoredLength();

			int maxPageSpace = emptyPage.getSpace();

			TupleSegment ts = new TupleSegment();
			int overhead = ts.overhead() + emptyPage.getSlotOverhead();

			int maxSegmentSize = maxPageSpace - overhead;

			boolean segmentationRequired;
			segmentationRequired = tupleLength > maxSegmentSize;
			BufferAccessBlock bab = null;

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
			int pageNumber = -1;
			int spaceMapPage = -1;
			int spacebitsBefore = 0;
			while (pageNumber == -1) {
				pageNumber = spcursor.findAndFixSpaceMapPageExclusively(new TwoBitSpaceCheckerImpl(maxPageSpace, requiredSpace));
				if (pageNumber == -1) {
					// FIXME Test case needed
					spacemgr.extendContainer(trx, containerId);
					pageNumber = spcursor.findAndFixSpaceMapPageExclusively(new TwoBitSpaceCheckerImpl(maxPageSpace, requiredSpace));
					if (pageNumber == -1) {
						throw new TupleException();
					}
				}
				spaceMapPage = spcursor.getCurrentSpaceMapPage().getPageId().getPageNumber();
				spacebitsBefore = spcursor.getCurrentSpaceMapPage().getSpaceBits(pageNumber);
				PageId pageId = new PageId(containerId, pageNumber);
				bab = relmgr.bufmgr.fixExclusive(pageId, false, -1, 0);
				spcursor.unfixCurrentSpaceMapPage();
				try {
					reclaimDeletedTuples(trx, bab);
					SlottedPage page = (SlottedPage) bab.getPage();
					// TODO perhaps we should allow for some storage space below
					if (page.getFreeSpace() < overhead) {
						// FIXME Test case needed
						pageNumber = -1;
					}
				} finally {
					if (pageNumber == -1) {
						// FIXME Test case needed
						bab.unfix();
						bab = null;
					}
				}
			}
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "startInsert", "SIMPLEDBM-LOG: Page number " + pageNumber + " has been selected for tuple's first segment");
			}

			try {

				/*
				 * At this point the the new page should be fixed.
				 */
				int slotNumber = -1;
				SlottedPage page = (SlottedPage) bab.getPage();

				/*
				 * Following loops around trying to use a slot within the page
				 * that has been identified.
				 */
				boolean canContinue = false;
				boolean mustRestart = false;
				while (!canContinue && !mustRestart) {

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
					if (log.isDebugEnabled()) {
						log.debug(LOG_CLASS_NAME, "startInsert", "SIMPLEDBM-LOG: Tentative location for new tuple will be " + location);
					}

					/*
					 * Try to get a conditional lock on the proposed tuple id.
					 */
					boolean locked = false;
					try {
						trx.acquireLockNowait(location, LockMode.EXCLUSIVE, LockDuration.COMMIT_DURATION);
						locked = true;
					} catch (TransactionException.LockException e) {
						if (log.isDebugEnabled()) {
							log.debug(LOG_CLASS_NAME, "startInsert", "SIMPLEDBM-LOG: Failed to obtain conditional lock on location " + location);
						}
					}

					if (locked) {
						canContinue = true;
					} else {
						/*
						 * Conditional lock failed. We need to get unconditional
						 * lock, but before that we need to release latches.
						 */
						// FIXME Test case needed
						Savepoint sp = trx.createSavepoint();
						Lsn lsn = page.getPageLsn();

						bab.unfix();
						bab = null;

						trx.acquireLock(location, LockMode.EXCLUSIVE, LockDuration.COMMIT_DURATION);

						PageId pageId = new PageId(containerId, pageNumber);
						bab = relmgr.bufmgr.fixExclusive(pageId, false, -1, 0);
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
								if (slotNumber == page.getNumberOfSlots() || page.isSlotDeleted(slotNumber)) {
									canContinue = true;
								} else {
									/*
									 * Release lock on previous slot.
									 */
									trx.rollback(sp);
									if (log.isDebugEnabled()) {
										log.debug(LOG_CLASS_NAME, "startInsert", "SIMPLEDBM-LOG: Unable to continue insert of new tuple at " + location + " as page has changed in the meantime");
									}
								}
							} else {
								/*
								 * No more space in the page.
								 */
								mustRestart = true;
							}
						} else {
							canContinue = true;
						}
					}
				}

				if (mustRestart) {
					/*
					 * Cannot use the page or the slot originally identified due
					 * to lack of space. Must restart the insert
					 */
					if (log.isDebugEnabled()) {
						log.debug(LOG_CLASS_NAME, "startInsert", "SIMPLEDBM-LOG: Unable to continue insert of new tuple at " + location + " as page has changed in the meantime - Insert will be restarted");
					}
					return false;
				}

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

				InsertTupleSegment logrec = (InsertTupleSegment) relmgr.loggableFactory.getInstance(TupleManagerImpl.MODULE_ID, TupleManagerImpl.TYPE_LOG_INSERTTUPLESEGMENT);
				logrec.slotNumber = slotNumber;
				logrec.spaceMapPage = spaceMapPage;
				ts.data = new byte[firstSegmentSize];
				System.arraycopy(tupleData, currentPos, ts.data, 0, firstSegmentSize);
				currentPos += firstSegmentSize;
				logrec.segment = ts;
				if (segmentationRequired) {
					logrec.segmentationFlags = TupleHelper.TUPLE_SEGMENT + TupleHelper.TUPLE_FIRST_SEGMENT;
				} else {
					logrec.segmentationFlags = 0;
				}

				Lsn lsn = trx.logInsert(page, logrec);
				relmgr.redo(page, logrec);
				bab.setDirty(lsn);

				prevSegmentPage = page.getPageId().getPageNumber();
				prevSegmentSlotNumber = slotNumber;

				int spacebits = TwoBitSpaceCheckerImpl.mapFreeSpaceInfo(maxPageSpace, page.getFreeSpace());
				if (spacebits != spacebitsBefore) {
					spcursor.fixSpaceMapPageExclusively(spaceMapPage, pageNumber);
					try {
						FreeSpaceMapPage smp = spcursor.getCurrentSpaceMapPage();
						if (smp.getSpaceBits(pageNumber) != spacebits) {
							spcursor.updateAndLogRedoOnly(trx, pageNumber, spacebits);
							if (log.isDebugEnabled()) {
								log.debug(LOG_CLASS_NAME, "doStartInsert", "Updated space map information from " + spacebitsBefore + " to " + spacebits + " for page " + page.getPageId() + " in space map page " + page.getSpaceMapPageNumber() + " as a result of " + logrec);
							}
						}
					} finally {
						spcursor.unfixCurrentSpaceMapPage();
					}
				}
			} finally {
				if (bab != null) {
					bab.unfix();
				}
				if (spcursor != null) {
					spcursor.unfixCurrentSpaceMapPage();
				}
			}
			return true;
		}

		/**
		 * Completes insertion of tuple by generating additional segments where
		 * necessary.
		 */
		void doCompleteInsert() throws FreeSpaceManagerException, TransactionException, BufferManagerException, TupleException {

			if (currentPos == tupleLength) {
				/*
				 * There is no need to create additional segments.
				 */
				return;
			}

			TupleManagerImpl relmgr = tupleContainer.tuplemgr;

			SlottedPage emptyPage = tupleContainer.emptyPage;

			FreeSpaceManager spacemgr = relmgr.spaceMgr;
			FreeSpaceCursor spcursor = spacemgr.getSpaceCursor(containerId);

			final int maxPageSpace = emptyPage.getSpace();

			TupleSegment ts = new TupleSegment();
			final int overhead = ts.overhead() + emptyPage.getSlotOverhead();

			final int maxSegmentSize = maxPageSpace - overhead;

			int pageNumber = -1;
			BufferAccessBlock bab = null;

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
					pageNumber = -1;
					int spaceMapPage = -1;
					int spacebitsBefore = -1;

					while (pageNumber == -1) {
						pageNumber = spcursor.findAndFixSpaceMapPageExclusively(new TwoBitSpaceCheckerImpl(maxPageSpace, requiredSpace));
						if (pageNumber == -1) {
							// FIXME Test case needed
							spacemgr.extendContainer(trx, containerId);
							pageNumber = spcursor.findAndFixSpaceMapPageExclusively(new TwoBitSpaceCheckerImpl(maxPageSpace, requiredSpace));
							if (pageNumber == -1) {
								throw new TupleException();
							}
						}
						spaceMapPage = spcursor.getCurrentSpaceMapPage().getPageId().getPageNumber();
						spacebitsBefore = spcursor.getCurrentSpaceMapPage().getSpaceBits(pageNumber);
						PageId pageId = new PageId(containerId, pageNumber);
						bab = relmgr.bufmgr.fixExclusive(pageId, false, -1, 0);
						spcursor.unfixCurrentSpaceMapPage();
						try {
							reclaimDeletedTuples(trx, bab);
							SlottedPage page = (SlottedPage) bab.getPage();
							if (page.getFreeSpace() < requiredSpace) {
								// FIXME Test case needed
								pageNumber = -1;
							}
						} finally {
							if (pageNumber == -1) {
								// FIXME Test case needed
								bab.unfix();
							}
						}
					}

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

					InsertTupleSegment logrec = (InsertTupleSegment) relmgr.loggableFactory.getInstance(TupleManagerImpl.MODULE_ID, TupleManagerImpl.TYPE_LOG_INSERTTUPLESEGMENT);
					logrec.slotNumber = slotNumber;
					logrec.spaceMapPage = spaceMapPage;
					if (nextSegmentSize + currentPos == tupleLength) {
						logrec.segmentationFlags = TupleHelper.TUPLE_LAST_SEGMENT + TupleHelper.TUPLE_SEGMENT;
					} else {
						logrec.segmentationFlags = TupleHelper.TUPLE_SEGMENT;
					}
					ts.data = new byte[nextSegmentSize];
					System.arraycopy(tupleData, currentPos, ts.data, 0, nextSegmentSize);
					currentPos += nextSegmentSize;
					logrec.segment = ts;

					Lsn lsn = trx.logInsert(page, logrec);
					relmgr.redo(page, logrec);
					bab.setDirty(lsn);

					int spacebits = TwoBitSpaceCheckerImpl.mapFreeSpaceInfo(maxPageSpace, page.getFreeSpace());
					if (spacebits != spacebitsBefore) {
						spcursor.fixSpaceMapPageExclusively(spaceMapPage, pageNumber);
						try {
							FreeSpaceMapPage smp = spcursor.getCurrentSpaceMapPage();
							if (smp.getSpaceBits(pageNumber) != spacebits) {
								spcursor.updateAndLogRedoOnly(trx, pageNumber, spacebits);
								if (log.isDebugEnabled()) {
									log.debug(LOG_CLASS_NAME, "doCompleteInsert", "Updated space map information from " + spacebitsBefore + " to " + spacebits + " for page " + page.getPageId() + " in space map page " + page.getSpaceMapPageNumber() + " as a result of " + logrec);
								}
							}
						} finally {
							spcursor.unfixCurrentSpaceMapPage();
						}
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
				spcursor = null;
			} finally {
				if (bab != null) {
					bab.unfix();
				}
				if (spcursor != null) {
					spcursor.unfixCurrentSpaceMapPage();
				}
			}
		}

		/**
		 * Updates previous tuple segment so that it points to the next tuple
		 * segment.
		 */
		void updatePrevSegment(int nextSegmentPage, int nextSegmentSlotNumber) throws BufferManagerException, TransactionException {

			TupleManagerImpl relmgr = tupleContainer.tuplemgr;
			if (prevSegmentPage != -1) {
				PageId pageId = new PageId(containerId, prevSegmentPage);
				BufferAccessBlock bab = relmgr.bufmgr.fixExclusive(pageId, false, -1, 0);
				try {
					SlottedPage page = (SlottedPage) bab.getPage();
					Redoable loggable = null;
					if (updateMode) {
						updateMode = false;
						UndoableReplaceSegmentLink logrec = (UndoableReplaceSegmentLink) relmgr.loggableFactory.getInstance(TupleManagerImpl.MODULE_ID, TupleManagerImpl.TYPE_LOG_UNDOABLEREPLACESEGMENTLINK);
						logrec.slotNumber = prevSegmentSlotNumber;
						logrec.nextSegmentPage = nextSegmentPage;
						logrec.nextSegmentSlotNumber = nextSegmentSlotNumber;
						logrec.newSegmentationFlags = logrec.oldSegmentationFlags = page.getFlags(prevSegmentSlotNumber);
						if (TupleHelper.isSegmented(page, prevSegmentSlotNumber)) {
							TupleSegment ts = new TupleSegment();
							page.get(prevSegmentSlotNumber, ts);
							logrec.oldNextSegmentPage = ts.nextPageNumber;
							logrec.oldNextSegmentSlot = ts.nextSlotNumber;
						}
						loggable = logrec;
					} else {
						UpdateSegmentLink logrec = (UpdateSegmentLink) relmgr.loggableFactory.getInstance(TupleManagerImpl.MODULE_ID, TupleManagerImpl.TYPE_LOG_UPDATESEGMENTLINK);
						logrec.nextSegmentPage = nextSegmentPage;
						logrec.nextSegmentSlotNumber = nextSegmentSlotNumber;
						logrec.slotNumber = prevSegmentSlotNumber;
						loggable = logrec;
					}
					Lsn lsn = trx.logInsert(page, loggable);
					relmgr.redo(page, loggable);
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
		public Location startInsert(Transaction trx, Storable tuple) throws TupleException {
			this.trx = trx;
			this.tuple = tuple;
			try {
				while (!doStartInsert()) {
				}
			} catch (FreeSpaceManagerException e) {
				throw new TupleException.SpaceMgrException(e);
			} catch (BufferManagerException e) {
				throw new TupleException.BufMgrException(e);
			} catch (TransactionException e) {
				throw new TupleException.TrxException(e);
			}
			return location;
		}

		/**
		 * @see org.simpledbm.rss.api.tuple.TupleInserter#completeInsert()
		 */
		public void completeInsert() throws TupleException {
			try {
				doCompleteInsert();
			} catch (FreeSpaceManagerException e) {
				throw new TupleException.SpaceMgrException(e);
			} catch (BufferManagerException e) {
				throw new TupleException.BufMgrException(e);
			} catch (TransactionException e) {
				throw new TupleException.TrxException(e);
			}
		}

		public Location getLocation() {
			return new TupleId(location);
		}
	}

	public static class TupleContainerImpl implements TupleContainer {

		private static String LOG_CLASS_NAME = TupleContainerImpl.class.getName();
		
		final TupleManagerImpl tuplemgr;

		final int containerId;

		SlottedPage emptyPage;

		public TupleContainerImpl(TupleManagerImpl relmgr, int containerId) {
			this.tuplemgr = relmgr;
			this.containerId = containerId;
			this.emptyPage = (SlottedPage) relmgr.pageFactory.getInstance(relmgr.spMgr.getPageType(), new PageId());
			this.emptyPage.init();
		}

		public TupleInserter insert(Transaction trx, Storable tuple) throws TupleException {
			TupleInserterImpl inserter = new TupleInserterImpl(this);
			inserter.startInsert(trx, tuple);
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
		 * @throws TupleException
		 * 
		 * @see TupleInserterImpl#reclaimDeletedTuples(Transaction,
		 *      BufferAccessBlock)
		 * @see TupleManagerImpl#redo(Page, Redoable)
		 * @see TupleManagerImpl#undoDeleteSegment(Transaction, Undoable)
		 */
		public void doDelete(Transaction trx, Location location) throws TransactionException, BufferManagerException, FreeSpaceManagerException, TupleException {

			BufferManager bufmgr = tuplemgr.bufmgr;
			TupleId tupleid = (TupleId) location;

			FreeSpaceManager spacemgr = tuplemgr.spaceMgr;
			FreeSpaceCursor spcursor = spacemgr.getSpaceCursor(containerId);

			trx.acquireLock(location, LockMode.EXCLUSIVE, LockDuration.COMMIT_DURATION);

			PageId pageid = tupleid.getPageId();
			int slotNumber = tupleid.getSlotNumber();

			boolean validated = false;

			while (pageid != null) {
				int nextPage = -1;
				int nextSlot = -1;
				BufferAccessBlock bab = bufmgr.fixExclusive(pageid, false, -1, 0);
				try {
					SlottedPage page = (SlottedPage) bab.getPage();

					if (!validated) {
						if (page.getNumberOfSlots() <= slotNumber || page.isSlotDeleted(slotNumber) || TupleHelper.isDeleted(page, slotNumber) || (TupleHelper.isSegmented(page, slotNumber) && !TupleHelper.isFirstSegment(page, slotNumber))) {
							throw new TupleException("Invalid tuple location " + location);
						}
						validated = true;
					}

					UndoableReplaceSegmentLink logrec = (UndoableReplaceSegmentLink) tuplemgr.loggableFactory.getInstance(TupleManagerImpl.MODULE_ID, TupleManagerImpl.TYPE_LOG_UNDOABLEREPLACESEGMENTLINK);
					logrec.oldSegmentationFlags = page.getFlags(slotNumber);
					logrec.newSegmentationFlags = page.getFlags(slotNumber) | TupleHelper.TUPLE_DELETED;
					logrec.slotNumber = slotNumber;

					if (TupleHelper.isSegmented(page, slotNumber)) {
						TupleSegment ts = new TupleSegment();
						page.get(slotNumber, ts);
						nextPage = logrec.oldNextSegmentPage = ts.nextPageNumber;
						nextSlot = logrec.oldNextSegmentSlot = ts.nextSlotNumber;
						logrec.nextSegmentPage = tupleid.getPageId().getPageNumber();
						logrec.nextSegmentSlotNumber = tupleid.getSlotNumber();
					}

					int tslength = page.getDataLength(slotNumber);

					int freespace = page.getFreeSpace() + tslength;
					int spacebitsBefore = TwoBitSpaceCheckerImpl.mapFreeSpaceInfo(page.getSpace(), page.getFreeSpace());
					int spacebitsAfter = TwoBitSpaceCheckerImpl.mapFreeSpaceInfo(page.getSpace(), freespace);

					Lsn lsn = trx.logInsert(page, logrec);
					tuplemgr.redo(page, logrec);
					bab.setDirty(lsn);

					if (spacebitsBefore != spacebitsAfter) {
						spcursor.fixSpaceMapPageExclusively(page.getSpaceMapPageNumber(), page.getPageId().getPageNumber());
						try {
							FreeSpaceMapPage smp = spcursor.getCurrentSpaceMapPage();
							if (smp.getSpaceBits(page.getPageId().getPageNumber()) != spacebitsAfter) {
								spcursor.updateAndLogRedoOnly(trx, page.getPageId().getPageNumber(), spacebitsAfter);
								if (log.isDebugEnabled()) {
									log.debug(LOG_CLASS_NAME, "doDelete", "Updated space map information from " + spacebitsBefore + " to " + spacebitsAfter + " for page " + page.getPageId() + " in space map page " + page.getSpaceMapPageNumber() + " as a result of " + logrec);
								}
							}
						} finally {
							spcursor.unfixCurrentSpaceMapPage();
						}
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

		public void delete(Transaction trx, Location location) throws TupleException {
			try {
				doDelete(trx, location);
			} catch (TransactionException e) {
				throw new TupleException.TrxException(e);
			} catch (BufferManagerException e) {
				throw new TupleException.BufMgrException(e);
			} catch (FreeSpaceManagerException e) {
				throw new TupleException.SpaceMgrException(e);
			}
		}

		byte[] doRead(Location location) throws BufferManagerException, IOException, TupleException {
			BufferManager bufmgr = tuplemgr.bufmgr;
			TupleId tupleid = (TupleId) location;

			ByteArrayOutputStream os = new ByteArrayOutputStream();

			PageId pageid = tupleid.getPageId();
			int slotNumber = tupleid.getSlotNumber();

			boolean validated = false;

			while (pageid != null) {
				int nextPage = -1;
				int nextSlot = -1;
				BufferAccessBlock bab = bufmgr.fixExclusive(pageid, false, -1, 0);
				try {
					SlottedPage page = (SlottedPage) bab.getPage();

					if (!validated) {
						if (page.getNumberOfSlots() <= slotNumber || page.isSlotDeleted(slotNumber) || TupleHelper.isDeleted(page, slotNumber) || (TupleHelper.isSegmented(page, slotNumber) && !TupleHelper.isFirstSegment(page, slotNumber))) {
							throw new TupleException("Invalid tuple location " + location);
						}
						validated = true;
					}
					TupleSegment ts = new TupleSegment();
					page.get(slotNumber, ts);

					os.write(ts.data);

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

		public byte[] read(Location location) throws TupleException {
			try {
				return doRead(location);
			} catch (BufferManagerException e) {
				throw new TupleException.BufMgrException(e);
			} catch (IOException e) {
				throw new TupleException(e);
			}
		}

		/**
		 * An update is carried out in two stages. In the first stage
		 * exsiting tuple segments are updated with new data. If the new tuple is
		 * smaller or equal to the size of existing tuple, then the process
		 * ends here. If however, the new tuple is larger than the existing
		 * tuple, then additional tuple segments are added, using the
		 * tuple inserter mechanism used for inserting new tuples.
		 * <p>
		 * A drawback in current implementation is that existing tuple segments
		 * are not resized. Therefore, when new segments are added for a tuple,
		 * these segments may end up in pages that already contain oher segments
		 * of the same tuple. 
		 * <p>
		 * Another limitation of current implementation is that space occupied by
		 * a tuple is not released until the tuple is deleted. Thus, a tuple that is
		 * made smaller may end up with garbage bytes at the end - it is the
		 * responsibility of the caller to know the useful size of the tuple.
		 */
		void doUpdate(Transaction trx, Location location, Storable newTuple) throws TransactionException, BufferManagerException, TupleException {

			BufferManager bufmgr = tuplemgr.bufmgr;
			TupleId tupleid = (TupleId) location;

			trx.acquireLock(location, LockMode.EXCLUSIVE, LockDuration.COMMIT_DURATION);

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
				BufferAccessBlock bab = bufmgr.fixExclusive(pageid, false, -1, 0);
				try {
					SlottedPage page = (SlottedPage) bab.getPage();

					TupleSegment ts = new TupleSegment();
					page.get(slotNumber, ts);

					if (TupleHelper.isSegmented(page, slotNumber)) {
						nextPage = ts.nextPageNumber;
						nextSlot = ts.nextSlotNumber;
					}

					// int segmentSize = ts.getStoredLength();
					int segmentSize = ts.data.length;

					int copySize = segmentSize;
					if (copySize > remaining) {
						// FIXME Test case needed
						copySize = remaining;
					}

					UndoableUpdateTupleSegment logrec = (UndoableUpdateTupleSegment) tuplemgr.loggableFactory.getInstance(TupleManagerImpl.MODULE_ID, TupleManagerImpl.TYPE_LOG_UNDOABLEUPDATETUPLESEGMENT);
					logrec.slotNumber = slotNumber;
					logrec.spaceMapPage = page.getSpaceMapPageNumber();
					logrec.oldSegment = ts;
					logrec.oldSegmentationFlags = page.getFlags(slotNumber);

					TupleSegment tsnew = new TupleSegment();
					tsnew.data = new byte[segmentSize];
					System.arraycopy(tupleData, currentPos, tsnew.data, 0, copySize);
					currentPos += copySize;
					remaining -= copySize;
					tsnew.nextPageNumber = nextPage;
					tsnew.nextSlotNumber = nextSlot;
					logrec.segment = tsnew;

					if (segno == 0 && remaining > 0) {
						logrec.segmentationFlags = TupleHelper.TUPLE_SEGMENT + TupleHelper.TUPLE_FIRST_SEGMENT;
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
				TupleInserterImpl inserter = new TupleInserterImpl(this);
				inserter.containerId = containerId;
				inserter.currentPos = currentPos;
				inserter.location = tupleid;
				inserter.prevSegmentPage = prevPage;
				inserter.prevSegmentSlotNumber = prevSlot;
				inserter.spaceMapPage = -1;
				inserter.trx = trx;
				inserter.tuple = newTuple;
				inserter.tupleData = tupleData;
				inserter.tupleLength = tupleLength;
				inserter.updateMode = true;
				inserter.completeInsert();
			}
		}

		public void update(Transaction trx, Location location, Storable newTuple) throws TupleException {
			try {
				doUpdate(trx, location, newTuple);
			} catch (TransactionException e) {
				throw new TupleException.TrxException(e);
			} catch (BufferManagerException e) {
				throw new TupleException.BufMgrException(e);
			}
		}
		
		public TupleScan openScan(Transaction trx, LockMode lockMode) {
			return new TupleScanImpl(this, trx, lockMode);
		}

	}

	public static class TupleScanImpl implements TupleScan {
		
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
		void initScan() throws TupleException {
			if (spaceScan == null) {
				try {
					spaceScan = tupleContainer.tuplemgr.spaceMgr.openScan(tupleContainer.containerId);
				} catch (FreeSpaceManagerException e) {
					throw new TupleException.SpaceMgrException(e);
				}
			}
		}
		
		public TupleScanImpl(TupleContainerImpl tupleContainer, Transaction trx, LockMode lockMode) {
			this.tupleContainer = tupleContainer;
			this.trx = trx;
			this.lockMode = lockMode;
		}
		
		public boolean fetchNext() throws TupleException {
			try {
				return doFetchNext();
			} catch (FreeSpaceManagerException e) {
				throw new TupleException.SpaceMgrException(e);
			} catch (BufferManagerException e) {
				throw new TupleException.BufMgrException(e);
			} catch (TransactionException e) {
				throw new TupleException.TrxException(e);
			}
		}

		/**
		 * Workhorse for performing the scan.
		 */
		boolean doFetchNext() throws TupleException, FreeSpaceManagerException, BufferManagerException, TransactionException {
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
					BufferAccessBlock bab = tupleContainer.tuplemgr.bufmgr.fixShared(new PageId(tupleContainer.containerId, currentPage), 0);
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
								if (page.isSlotDeleted(currentSlot) || (TupleHelper.isSegmented(page, currentSlot) && !TupleHelper.isFirstSegment(page, currentSlot))) {
									continue;
								}
								/*
								 * Potential candidate for scan
								 */
								nextLocation = new TupleId(page.getPageId(), currentSlot);
								/*
								 * Check if the tuple is logically deleted. If so, we need to lock the tuple to 
								 * determine if the delete has been committed. In future, it may be worthwhile
								 * to provide an option to skip deleted rows.
								 */
								if (TupleHelper.isDeleted(page, currentSlot)) {
									// FIXME Test case needed
									try {
										trx.acquireLockNowait(nextLocation, lockMode, LockDuration.INSTANT_DURATION);
										/*
										 * Delete was committed. 
										 */
										nextLocation = null;
										continue;
									} catch (TransactionException.LockException e) {
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
										trx.acquireLockNowait(nextLocation, lockMode, LockDuration.MANUAL_DURATION);
										state = LOCK_GRANTED;
									} catch (TransactionException.LockException e) {
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
					Savepoint sp = trx.createSavepoint();
					//System.err.println("Found slot " + nextLocation + " is locked, so waiting");
					trx.acquireLock(nextLocation, lockMode, LockDuration.MANUAL_DURATION);
					state = LOCK_GRANTED;
					/*
					 * Since we released the page latch before acquiring the lock, we
					 * need to double check that the locked tuple is still eligible.
					 */
					BufferAccessBlock bab = tupleContainer.tuplemgr.bufmgr.fixShared(new PageId(tupleContainer.containerId, currentPage), 0);
					SlottedPage page = (SlottedPage) bab.getPage();
					try {
						if (!savedPageLsn.equals(page.getPageLsn())) {
							if (page.isSlotDeleted(currentSlot) || (TupleHelper.isSegmented(page, currentSlot) && !TupleHelper.isFirstSegment(page, currentSlot)) || TupleHelper.isDeleted(page, currentSlot)) {
								//System.err.println("Found slot " + nextLocation + " is no longer valid");
								/*
								 * The tuple is no longer valid, so we release the lock on the tuple
								 * and start the search again.
								 */
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

		public byte[] getCurrentTuple() throws TupleException {
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

		public void close() throws TupleException {
			try {
				spaceScan.close();
			} catch (FreeSpaceManagerException e) {
				throw new TupleException.SpaceMgrException(e);
			}
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
		public TwoBitSpaceCheckerImpl initSearch(int availableSpace, int desiredFreeSpace) {
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
		public static int mapFreeSpaceInfo(int availableSize, int freeSpace) {
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
	 * {@link org.simpledbm.sp.SlottedPage} allows flags to be associated with
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
		public static boolean isSegmented(SlottedPage page, int slot) {
			int flags = page.getFlags(slot);
			return (flags & TUPLE_SEGMENT) != 0;
		}

		/**
		 * Checks whether a particular slot is marked as the first segment of a
		 * tuple.
		 */
		public static boolean isFirstSegment(SlottedPage page, int slot) {
			int flags = page.getFlags(slot);
			return (flags & TUPLE_FIRST_SEGMENT) != 0;
		}

		/**
		 * Checks whether a particular slot is marked as the last segment of a
		 * tuple.
		 */
		public static boolean isLastSegment(SlottedPage page, int slot) {
			int flags = page.getFlags(slot);
			return (flags & TUPLE_LAST_SEGMENT) != 0;
		}

		/**
		 * Checks whether a particular slot is marked as a logically deleted
		 * tuple.
		 */
		public static boolean isDeleted(SlottedPage page, int slot) {
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
	public static class TupleSegment implements Storable {

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

		public void retrieve(ByteBuffer bb) {
			nextPageNumber = bb.getInt();
			nextSlotNumber = bb.getShort();
			int length = bb.getShort();
			data = new byte[length];
			bb.get(data);
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
		
		public String toString() {
			return "TupleSegment(len=" + data.length + ", nextPointer(" + nextPageNumber + ", " + nextSlotNumber + "))";
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

		/**
		 * Slot to be deleted.
		 */
		int slotNumber;

		@Override
		public void init() {
		}

		@Override
		public int getStoredLength() {
			return super.getStoredLength() + TypeSize.SHORT;
		}

		@Override
		public void retrieve(ByteBuffer bb) {
			super.retrieve(bb);
			slotNumber = bb.getShort();
		}

		@Override
		public void store(ByteBuffer bb) {
			super.store(bb);
			bb.putShort((short) slotNumber);
		}

		@Override
		public String toString() {
			return "DeleteSlot(slotNumber=" + slotNumber + ", " + super.toString() + ")";
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

		@Override
		public void init() {
		}

		@Override
		public int getStoredLength() {
			return super.getStoredLength() + segment.getStoredLength() + TypeSize.SHORT * 2 + TypeSize.INTEGER;
		}

		@Override
		public void retrieve(ByteBuffer bb) {
			super.retrieve(bb);
			segment = new TupleSegment();
			segment.retrieve(bb);
			slotNumber = bb.getShort();
			segmentationFlags = bb.getShort();
			spaceMapPage = bb.getInt();
		}

		@Override
		public void store(ByteBuffer bb) {
			super.store(bb);
			segment.store(bb);
			bb.putShort((short) slotNumber);
			bb.putShort((short) segmentationFlags);
			bb.putInt(spaceMapPage);
		}

		@Override
		public String toString() {
			return "slotNumber=" + slotNumber + ", segmentationFlags=" + segmentationFlags + ", spaceMapPage=" + spaceMapPage + ", " + super.toString() + ")";
		}
	}

	/**
	 * Logs the insertion of a tuple segment. Large tuples are broken down into
	 * segments across pages.
	 */
	public static class InsertTupleSegment extends BaseInsertTupleSegment implements Undoable, LogicalUndo {
		public String toString() { return "InsertTupleSegment(" + super.toString() + ")"; }
	}

	/**
	 * Logs the undo of a tuple segment insert. The undo causes the slot to be
	 * physically deleted.
	 */
	public static class UndoInsertTupleSegment extends DeleteSlot implements Compensation {
		public String toString() { return "UndoInsertTupleSegment(" + super.toString() + ")"; }
	}

	/**
	 * Logs the update of a tuple segment.
	 */
	public static class UndoableUpdateTupleSegment extends BaseInsertTupleSegment implements Undoable {
		/**
		 * Old value of segmentation flag is saved here.
		 */
		int oldSegmentationFlags;

		/**
		 * Old image of tuple segment is saved here.
		 */
		TupleSegment oldSegment;

		@Override
		public int getStoredLength() {
			return super.getStoredLength() + TypeSize.SHORT + oldSegment.getStoredLength();
		}

		@Override
		public void retrieve(ByteBuffer bb) {
			super.retrieve(bb);
			oldSegmentationFlags = bb.getShort();
			oldSegment = new TupleSegment();
			oldSegment.retrieve(bb);
		}

		@Override
		public void store(ByteBuffer bb) {
			super.store(bb);
			bb.putShort((short) oldSegmentationFlags);
			oldSegment.store(bb);
		}
		
		public String toString() {
			return "UndoableUpdateTupleSegment(oldSegmentationFlags=" + oldSegmentationFlags + super.toString() + ")";
		}
	}

	/**
	 * Logs the undo of an update tuple segment operation.
	 */
	public static class UndoUpdateTupleSegment extends BaseInsertTupleSegment implements Compensation {
		public String toString() { return "UndoUpdateTupleSegment(" + super.toString() + ")"; }
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

		@Override
		public void init() {
		}

		@Override
		public int getStoredLength() {
			return super.getStoredLength() + TypeSize.INTEGER + TypeSize.SHORT * 2;
		}

		@Override
		public void retrieve(ByteBuffer bb) {
			super.retrieve(bb);
			slotNumber = bb.getShort();
			nextSegmentPage = bb.getInt();
			nextSegmentSlotNumber = bb.getShort();
		}

		@Override
		public void store(ByteBuffer bb) {
			super.store(bb);
			bb.putShort((short) slotNumber);
			bb.putInt(nextSegmentPage);
			bb.putShort((short) nextSegmentSlotNumber);
		}

		@Override
		public String toString() {
			return "slotNumber=" + slotNumber + ", nextSegmentPage=" + nextSegmentPage + ", nextSegmentSlot=" + nextSegmentSlotNumber + ", " + super.toString();
		}
	}

	public static class ExtendedUpdateSegmentLink extends BaseUpdateSegmentLink {
		int newSegmentationFlags;

		@Override
		public int getStoredLength() {
			return super.getStoredLength() + TypeSize.SHORT;
		}

		@Override
		public void retrieve(ByteBuffer bb) {
			super.retrieve(bb);
			newSegmentationFlags = bb.getShort();
		}

		@Override
		public void store(ByteBuffer bb) {
			super.store(bb);
			bb.putShort((short) newSegmentationFlags);
		}

		@Override
		public String toString() {
			return "newSegmentationFlags=" + newSegmentationFlags + ", " + super.toString();
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
	public static class UndoableReplaceSegmentLink extends ExtendedUpdateSegmentLink implements Undoable, LogicalUndo {

		int oldNextSegmentPage;

		int oldNextSegmentSlot;

		int oldSegmentationFlags;

		@Override
		public int getStoredLength() {
			return super.getStoredLength() + TypeSize.INTEGER + TypeSize.SHORT * 2;
		}

		@Override
		public void retrieve(ByteBuffer bb) {
			super.retrieve(bb);
			oldNextSegmentPage = bb.getInt();
			oldNextSegmentSlot = bb.getShort();
			oldSegmentationFlags = bb.getShort();
		}

		@Override
		public void store(ByteBuffer bb) {
			super.store(bb);
			bb.putInt(oldNextSegmentPage);
			bb.putShort((short) oldNextSegmentSlot);
			bb.putShort((short) oldSegmentationFlags);
		}

		@Override
		public String toString() {
			return "UndoableReplaceSegmentLink(oldSegmentationFlags=" + oldSegmentationFlags + ", oldNextSegmentPage=" + oldNextSegmentPage + ", oldNextSegmentSlot=" + oldNextSegmentSlot + ", " + super.toString() + ")";
		}
	}

	/**
	 * UndoReplaceSegmentLink is used to log undo of link updates
	 * caused during tuple deletes, and tuple updates.
	 */
	public static class UndoReplaceSegmentLink extends ExtendedUpdateSegmentLink implements Compensation {
		public String toString() {
			return "UndoReplaceSegmentLink(" + super.toString() + ")";
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
	public static class UpdateSegmentLink extends BaseUpdateSegmentLink implements Redoable {
		public String toString() {
			return "UpdateSegmentLink(" + super.toString() + ")";
		}
	}
}
