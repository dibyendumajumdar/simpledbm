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
package org.simpledbm.rss.impl.fsm;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.simpledbm.rss.api.bm.BufferAccessBlock;
import org.simpledbm.rss.api.bm.BufferManager;
import org.simpledbm.rss.api.fsm.FreeSpaceChecker;
import org.simpledbm.rss.api.fsm.FreeSpaceCursor;
import org.simpledbm.rss.api.fsm.FreeSpaceManager;
import org.simpledbm.rss.api.fsm.FreeSpaceManagerException;
import org.simpledbm.rss.api.fsm.FreeSpaceMapPage;
import org.simpledbm.rss.api.fsm.FreeSpaceScan;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.registry.ObjectFactory;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.api.tx.BaseLoggable;
import org.simpledbm.rss.api.tx.BaseTransactionalModule;
import org.simpledbm.rss.api.tx.Compensation;
import org.simpledbm.rss.api.tx.ContainerDeleteOperation;
import org.simpledbm.rss.api.tx.Loggable;
import org.simpledbm.rss.api.tx.LoggableFactory;
import org.simpledbm.rss.api.tx.NonTransactionRelatedOperation;
import org.simpledbm.rss.api.tx.PageFormatOperation;
import org.simpledbm.rss.api.tx.PostCommitAction;
import org.simpledbm.rss.api.tx.Redoable;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.api.tx.TransactionManager;
import org.simpledbm.rss.api.tx.TransactionalModuleRegistry;
import org.simpledbm.rss.api.tx.Undoable;
import org.simpledbm.rss.api.wal.LogManager;
import org.simpledbm.rss.api.wal.Lsn;
import org.simpledbm.rss.util.ByteString;
import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;

public final class FreeSpaceManagerImpl extends BaseTransactionalModule
        implements FreeSpaceManager {
	
	/*
	 * When new pages are added, we could set the space map pointer so that
	 * clients can always work out the space map page associated with a page.
	 * Currently this is not possible because the base Page class does not hold
	 * the space map pointer - only the sub-class SlottedPage does. This module
	 * is meant to be generic. A solution might be to add the space map pointer
	 * to the base Page class.
	 */
	
    static final Logger log = Logger.getLogger(FreeSpaceManagerImpl.class
        .getPackage()
        .getName());

    static final MessageCatalog mcat = new MessageCatalog();

    final PageManager pageFactory;

    final BufferManager bufmgr;

    private final StorageManager storageManager;

    private final StorageContainerFactory storageFactory;

    final LoggableFactory loggableFactory;

    private final TransactionManager trxmgr;

    final FreeSpaceCursorCache cursorCache = new FreeSpaceCursorCache(this);
    
    private static final short MODULE_ID = 2;

    private static final short TYPE_BASE = 20;
    static final short TYPE_HEADERPAGE = TYPE_BASE + 1;
    static final short TYPE_ONEBITSPACEMAPPAGE = TYPE_BASE + 2;
    static final short TYPE_TWOBITSPACEMAPPAGE = TYPE_BASE + 3;
    static final short TYPE_CREATECONTAINER = TYPE_BASE + 4;
    static final short TYPE_OPENCONTAINER = TYPE_BASE + 5;
    static final short TYPE_FORMATHEADERPAGE = TYPE_BASE + 6;
    static final short TYPE_FORMATSPACEMAPPAGE = TYPE_BASE + 7;
    static final short TYPE_UPDATESPACEMAPPAGE = TYPE_BASE + 8;
    static final short TYPE_UPDATEHEADERPAGE = TYPE_BASE + 9;
    static final short TYPE_UNDOUPDATEHEADERPAGE = TYPE_BASE + 10;
    static final short TYPE_LINKSPACEMAPPAGE = TYPE_BASE + 11;
    static final short TYPE_UNDOLINKSPACEMAPPAGE = TYPE_BASE + 12;
    static final short TYPE_FORMATRAWPAGE = TYPE_BASE + 13;
    static final short TYPE_UNDOCREATECONTAINER = TYPE_BASE + 14;
    static final short TYPE_UNDOABLEUPDATESPACEMAPPAGE = TYPE_BASE + 15;
    static final short TYPE_UNDOSPACEMAPPAGEUPDATE = TYPE_BASE + 16;
    static final short TYPE_DROPCONTAINER = TYPE_BASE + 17;

    static final int HEADER_PAGE = 0;

    static final int FIRST_SPACE_MAP_PAGE = 1;

    static final int FIRST_USER_PAGE = 2;

    private int Testing = 0;

    public FreeSpaceManagerImpl(ObjectRegistry objectFactory,
            PageManager pageFactory, LogManager logmgr, BufferManager bufmgr,
            StorageManager storageManager,
            StorageContainerFactory storageFactory,
            LoggableFactory loggableFactory, TransactionManager trxmgr,
            TransactionalModuleRegistry moduleRegistry,
            Properties p) {
        this.pageFactory = pageFactory;
        this.bufmgr = bufmgr;
        this.storageManager = storageManager;
        this.storageFactory = storageFactory;
        this.loggableFactory = loggableFactory;
        this.trxmgr = trxmgr;

        moduleRegistry.registerModule(MODULE_ID, this);

        objectFactory.registerSingleton(
        	TYPE_HEADERPAGE, new HeaderPage.HeaderPageFactory(pageFactory));
        objectFactory.registerSingleton(
            TYPE_ONEBITSPACEMAPPAGE, new OneBitSpaceMapPage.OneBitSpaceMapPageFactory(pageFactory));
        objectFactory.registerSingleton(
            TYPE_TWOBITSPACEMAPPAGE, new TwoBitSpaceMapPage.TwoBitSpaceMapPageFactory(pageFactory));
        objectFactory.registerObjectFactory(
        	TYPE_CREATECONTAINER, new CreateContainer.CreateContainerFactory());
        objectFactory.registerObjectFactory(
        	TYPE_OPENCONTAINER, new OpenContainer.OpenContainerFactory());
        objectFactory.registerObjectFactory(
            TYPE_FORMATHEADERPAGE, new FormatHeaderPage.FormatHeaderPageFactory());
        objectFactory.registerObjectFactory(
            TYPE_FORMATSPACEMAPPAGE, new FormatSpaceMapPage.FormatSpaceMapPageFactory());
        objectFactory.registerObjectFactory( 
            TYPE_UPDATESPACEMAPPAGE, new UpdateSpaceMapPage.UpdateSpaceMapPageFactory());
        objectFactory.registerObjectFactory(
            TYPE_UPDATEHEADERPAGE, new UpdateHeaderPage.UpdateHeaderPageFactory());
        objectFactory.registerObjectFactory(
            TYPE_UNDOUPDATEHEADERPAGE, new UndoUpdateHeaderPage.UndoUpdateHeaderPageFactory());
        objectFactory.registerObjectFactory(
            TYPE_LINKSPACEMAPPAGE, new LinkSpaceMapPage.LinkSpaceMapPageFactory());
        objectFactory.registerObjectFactory(
            TYPE_UNDOLINKSPACEMAPPAGE, new UndoLinkSpaceMapPage.UndoLinkSpaceMapPageFactory());
        objectFactory.registerObjectFactory(
        	TYPE_FORMATRAWPAGE, new FormatRawPage.FormatRawPageFactory());
        objectFactory.registerObjectFactory(
            TYPE_UNDOCREATECONTAINER, new UndoCreateContainer.UndoCreateContainerFactory());
        objectFactory.registerObjectFactory(
            TYPE_UNDOABLEUPDATESPACEMAPPAGE, new UndoableUpdateSpaceMapPage.UndoableUpdateSpaceMapPageFactory());
        objectFactory.registerObjectFactory(
            TYPE_UNDOSPACEMAPPAGEUPDATE, new UndoSpaceMapPageUpdate.UndoSpaceMapUpdateFactory());
        objectFactory.registerObjectFactory(
        	TYPE_DROPCONTAINER, new DropContainer.DropContainerFactory());
    }

    public final void setTesting(int level) {
        Testing = level;
    }

    public final FreeSpaceCursor getSpaceCursor(int containerId) {
        return new SpaceCursorImpl(this, containerId);
    }

    public FreeSpaceCursor getPooledSpaceCursor(int containerId) {
		return cursorCache.getFreeSpaceCursor(containerId);
	}

	public void releaseSpaceCursor(FreeSpaceCursor fsc) {
		cursorCache.returnFreeSpaceCursor(fsc);
	}

	@Override
    public final Compensation generateCompensation(Undoable undoable) {
        if (undoable instanceof CreateContainer) {
            UndoCreateContainer clr = new UndoCreateContainer(
                    MODULE_ID,
                    FreeSpaceManagerImpl.TYPE_UNDOCREATECONTAINER);
            CreateContainer logrec = (CreateContainer) undoable;
            clr.containerId = logrec.containerId;
            clr.name = logrec.name;
            return clr;
        } else if (undoable instanceof UpdateHeaderPage) {
            UndoUpdateHeaderPage clr = new UndoUpdateHeaderPage(
                MODULE_ID,
                FreeSpaceManagerImpl.TYPE_UNDOUPDATEHEADERPAGE);            
            UpdateHeaderPage logrec = (UpdateHeaderPage) undoable;
            clr.oldLastSpaceMapPage = logrec.oldLastSpaceMapPage;
            clr.oldNumberOfExtents = logrec.oldNumberOfExtents;
            clr.oldNumberOfSpaceMapPages = logrec.oldNumberOfSpaceMapPages;
            return clr;
        } else if (undoable instanceof UndoableUpdateSpaceMapPage) {
            UndoSpaceMapPageUpdate clr = new UndoSpaceMapPageUpdate(
                MODULE_ID,
                FreeSpaceManagerImpl.TYPE_UNDOSPACEMAPPAGEUPDATE);
            UndoableUpdateSpaceMapPage logrec = (UndoableUpdateSpaceMapPage) undoable;
            clr.pageNumber = logrec.pageNumber;
            clr.spaceValue = logrec.oldSpaceValue;
            return clr;
        } else if (undoable instanceof LinkSpaceMapPage) {
            LinkSpaceMapPage logrec = (LinkSpaceMapPage) undoable;
            UndoLinkSpaceMapPage clr = new UndoLinkSpaceMapPage(
                MODULE_ID,
                FreeSpaceManagerImpl.TYPE_UNDOLINKSPACEMAPPAGE);
            clr.oldNextSpaceMapPage = logrec.oldNextSpaceMapPage;
            return clr;
        }
        log.error(FreeSpaceManagerImpl.class.getName(), "generateCompensation", mcat.getMessage(
            "EF0003",
            undoable));
        throw new FreeSpaceManagerException(mcat.getMessage("EF0003", undoable));
    }

    @Override
    public final void redo(Page page, Redoable loggable) {
        if (loggable instanceof CreateContainer) {
            // TODO we should ideally check that this is done and not redo.
            CreateContainer logrec = (CreateContainer) loggable;
            StorageContainer sc = storageFactory.create(logrec.getName());
            storageManager.register(logrec.getContainerId(), sc);
        } else if (loggable instanceof UndoCreateContainer) {
            UndoCreateContainer logrec = (UndoCreateContainer) loggable;
            // Before dropping the container we need to ensure that the
            // Buffer Manager does not hold references to any pages that are
            // part of this container.
            bufmgr.invalidateContainer(logrec.getContainerId());
            storageManager.remove(logrec.getContainerId());
            // storageFactory.delete(logrec.getName());
            cursorCache.removeContainerId(logrec.getContainerId());
        } else if (loggable instanceof FormatHeaderPage) {
            HeaderPage hdrPage = (HeaderPage) page;
            FormatHeaderPage logrec = (FormatHeaderPage) loggable;
            hdrPage.setExtentSize(logrec.getExtentSize());
            hdrPage.setFirstSpaceMapPage(logrec.getFirstSpaceMapPage());
            hdrPage.setLastSpaceMapPage(logrec.getLastSpaceMapPage());
            hdrPage.setNumberOfExtents(logrec.getNumberOfExtents());
            hdrPage.setNumberOfSpaceMapPages(logrec.getNumberOfSpaceMapPages());
            hdrPage.setSpaceMapPageType(logrec.getSpaceMapPageType());
            hdrPage.setDataPageType(logrec.getDataPageType());
            hdrPage.setPageLsn(logrec.getLsn());
            /*
             * Externalize the page immediately
             */
            pageFactory.store(hdrPage);
        } else if (loggable instanceof UpdateHeaderPage) {
            HeaderPage hdrPage = (HeaderPage) page;
            UpdateHeaderPage logrec = (UpdateHeaderPage) loggable;
            hdrPage.setLastSpaceMapPage(logrec.newLastSpaceMapPage);
            hdrPage.setNumberOfExtents(logrec.newNumberOfExtents);
            hdrPage.setNumberOfSpaceMapPages(logrec.newNumberOfSpaceMapPages);
        } else if (loggable instanceof UndoUpdateHeaderPage) {
            HeaderPage hdrPage = (HeaderPage) page;
            UndoUpdateHeaderPage logrec = (UndoUpdateHeaderPage) loggable;
            hdrPage.setLastSpaceMapPage(logrec.oldLastSpaceMapPage);
            hdrPage.setNumberOfExtents(logrec.oldNumberOfExtents);
            hdrPage.setNumberOfSpaceMapPages(logrec.oldNumberOfSpaceMapPages);
        } else if (loggable instanceof FormatSpaceMapPage) {
            SpaceMapPageImpl smpPage = (SpaceMapPageImpl) page;
            FormatSpaceMapPage logrec = (FormatSpaceMapPage) loggable;
            smpPage.init();
            smpPage.setFirstPageNumber(logrec.getFirstPageNumber());
            smpPage.setNextSpaceMapPage(logrec.getNextSpaceMapPage());
            smpPage.setPageLsn(logrec.getLsn());
            /*
             * Externalize the page immediately
             */
            pageFactory.store(smpPage);
        } else if (loggable instanceof LinkSpaceMapPage) {
            SpaceMapPageImpl smpPage = (SpaceMapPageImpl) page;
            LinkSpaceMapPage logrec = (LinkSpaceMapPage) loggable;
            smpPage.setNextSpaceMapPage(logrec.newNextSpaceMapPage);
        } else if (loggable instanceof UndoLinkSpaceMapPage) {
            SpaceMapPageImpl smpPage = (SpaceMapPageImpl) page;
            UndoLinkSpaceMapPage logrec = (UndoLinkSpaceMapPage) loggable;
            smpPage.setNextSpaceMapPage(logrec.oldNextSpaceMapPage);
        } else if (loggable instanceof UpdateSpaceMapPage) {
            SpaceMapPageImpl smpPage = (SpaceMapPageImpl) page;
            UpdateSpaceMapPage logrec = (UpdateSpaceMapPage) loggable;
            smpPage
                .setSpaceBits(logrec.getPageNumber(), logrec.getSpaceValue());
        } else if (loggable instanceof UndoableUpdateSpaceMapPage) {
            SpaceMapPageImpl smpPage = (SpaceMapPageImpl) page;
            UndoableUpdateSpaceMapPage logrec = (UndoableUpdateSpaceMapPage) loggable;
            smpPage.setSpaceBits(logrec.pageNumber, logrec.spaceValue);
        } else if (loggable instanceof UndoSpaceMapPageUpdate) {
            SpaceMapPageImpl smpPage = (SpaceMapPageImpl) page;
            UndoSpaceMapPageUpdate logrec = (UndoSpaceMapPageUpdate) loggable;
            // System.out.println("UNDO space map update: page number = " + logrec.pageNumber + ", value = " + logrec.spaceValue);
            smpPage.setSpaceBits(logrec.pageNumber, logrec.spaceValue);
        } else if (loggable instanceof FormatRawPage) {
            page.setPageLsn(loggable.getLsn());
            pageFactory.store(page);
        }
    }

    @Override
    public final void redo(Loggable loggable) {
        if (loggable instanceof OpenContainer) {
            OpenContainer logrec = (OpenContainer) loggable;
            StorageContainer sc = storageManager.getInstance(logrec
                .getContainerId());
            if (sc == null) {
                sc = storageFactory.open(logrec.getName());
                storageManager.register(logrec.getContainerId(), sc);
            }
        } else if (loggable instanceof DropContainer) {
            DropContainer logrec = (DropContainer) loggable;
            bufmgr.invalidateContainer(logrec.getContainerId());
            storageManager.remove(logrec.getContainerId());
            // storageFactory.delete(logrec.getName());
            // Following is now handled by the transaction manager
            // trxmgr.logNonTransactionRelatedOperation(logrec);
            cursorCache.removeContainerId(logrec.getContainerId());
        }
    }

    /**
     * Pre-condition 1 - caller must have acquired exclusive lock on the container id.
     * Pre-condition 2 - There must be an open container with ID = 0. This container must contain at
     * least 1 page.
     */
    public final void createContainer(Transaction trx, String containerName,
            int containerid, int spaceBits, int extentSize, int dataPageType) {
        int spaceMapType = FreeSpaceManagerImpl.TYPE_ONEBITSPACEMAPPAGE;
        if (spaceBits == 2) {
            spaceMapType = FreeSpaceManagerImpl.TYPE_TWOBITSPACEMAPPAGE;
        } else if (spaceBits != 1) {
            log.error(FreeSpaceManagerImpl.class.getName(), "createContainer", mcat.getMessage(
                "EF0001",
                spaceBits));
            throw new FreeSpaceManagerException(mcat.getMessage(
                "EF0001",
                spaceBits));
        }
        SpaceMapPageImpl smpPage = (SpaceMapPageImpl) pageFactory.getInstance(
            spaceMapType,
            new PageId());
        if (extentSize > smpPage.getCount()) {
            extentSize = smpPage.getCount();
        } else if (extentSize < 4) {
            extentSize = 4;
        }

        // Start a nested top action
        trx.startNestedTopAction();
        boolean commitNTA = false;
        try {
            CreateContainer createContainerLog = new CreateContainer(
                MODULE_ID,
                FreeSpaceManagerImpl.TYPE_CREATECONTAINER);
            createContainerLog.setContainerId(containerid);
            createContainerLog.setName(containerName);
            // We log this operation against the bootstrap container at ID 0. 
            BufferAccessBlock bab = bufmgr.fixExclusive(
                new PageId(0, 0),
                false,
                -1,
                0);
            try {
                Page page = bab.getPage();
                Lsn lsn = trx.logInsert(page, createContainerLog);
                redo(page, createContainerLog);
                bab.setDirty(lsn);
            } finally {
                bab.unfix();
            }

            if (Testing == 2) {
                Testing = 0;
                throw new FreeSpaceManagerException.TestException();
            }

            // Log an open container operation. This is a redoable action that will always be
            // redone.
            OpenContainer openContainerLog = new OpenContainer(MODULE_ID, 
            		FreeSpaceManagerImpl.TYPE_OPENCONTAINER);
            openContainerLog.setContainerId(containerid);
            openContainerLog.setName(containerName);
            trxmgr.logNonTransactionRelatedOperation(openContainerLog);

            //Format the header page
            FormatHeaderPage formatHeaderPageLog = new FormatHeaderPage(
                MODULE_ID,
                FreeSpaceManagerImpl.TYPE_FORMATHEADERPAGE);
            formatHeaderPageLog.setExtentSize(extentSize);
            formatHeaderPageLog.setFirstSpaceMapPage(FIRST_SPACE_MAP_PAGE);
            formatHeaderPageLog.setLastSpaceMapPage(FIRST_SPACE_MAP_PAGE);
            formatHeaderPageLog.setNumberOfExtents(1);
            formatHeaderPageLog.setNumberOfSpaceMapPages(1);
            formatHeaderPageLog.setSpaceMapPageType(spaceMapType);
            formatHeaderPageLog.setDataPageType(dataPageType);
            bab = bufmgr.fixExclusive(
                new PageId(containerid, HEADER_PAGE),
                true,
                FreeSpaceManagerImpl.TYPE_HEADERPAGE,
                0);
            try {
                Page page = bab.getPage();
                Lsn lsn = trx.logInsert(page, formatHeaderPageLog);
                redo(page, formatHeaderPageLog);
                bab.setDirty(lsn);
            } finally {
                bab.unfix();
            }

            // Format the first Space Map page in the container and
            // mark the header page and the space map page as "in use".
            FormatSpaceMapPage formatSMPPageLog = new FormatSpaceMapPage(
                MODULE_ID,
                FreeSpaceManagerImpl.TYPE_FORMATSPACEMAPPAGE);
            formatSMPPageLog.setFirstPageNumber(HEADER_PAGE);
            formatSMPPageLog.setNextSpaceMapPage(0);
            bab = bufmgr.fixExclusive(new PageId(
                containerid,
                FIRST_SPACE_MAP_PAGE), true, spaceMapType, 0);
            try {
                Page page = bab.getPage();
                Lsn lsn = trx.logInsert(page, formatSMPPageLog);
                redo(page, formatSMPPageLog);
                page.setPageLsn(lsn);

                // Mark header page in use
                UpdateSpaceMapPage updateSpaceMapLog = new UpdateSpaceMapPage(
                    MODULE_ID,
                    FreeSpaceManagerImpl.TYPE_UPDATESPACEMAPPAGE);
                updateSpaceMapLog.setPageNumber(HEADER_PAGE);
                updateSpaceMapLog.setSpaceValue(smpPage.fullValue());
                lsn = trx.logInsert(page, updateSpaceMapLog);
                redo(page, updateSpaceMapLog);
                page.setPageLsn(lsn);

                // Mark space map page in use
                updateSpaceMapLog = new UpdateSpaceMapPage(
                    MODULE_ID,
                    FreeSpaceManagerImpl.TYPE_UPDATESPACEMAPPAGE);
                updateSpaceMapLog.setPageNumber(FIRST_SPACE_MAP_PAGE);
                updateSpaceMapLog.setSpaceValue(smpPage.fullValue());
                lsn = trx.logInsert(page, updateSpaceMapLog);
                redo(page, updateSpaceMapLog);
                bab.setDirty(lsn);
            } finally {
                bab.unfix();
            }

            // Format the new pages in the extent. This is necessary to ensure that the correct
            // page types are created before the page is accessed by any clients.
            for (int pageNumber = FIRST_SPACE_MAP_PAGE + 1; pageNumber < extentSize; pageNumber++) {
                bab = bufmgr.fixExclusive(
                    new PageId(containerid, pageNumber),
                    true,
                    dataPageType,
                    0);
                try {
                    Page page = bab.getPage();
                    FormatRawPage formatRawPageLog = new FormatRawPage(
                        MODULE_ID,
                        FreeSpaceManagerImpl.TYPE_FORMATRAWPAGE);
                    formatRawPageLog.setDataPageType(dataPageType);
                    Lsn lsn = trx.logInsert(page, formatRawPageLog);
                    redo(page, formatRawPageLog);
                    bab.setDirty(lsn);
                } finally {
                    bab.unfix();
                }
            }
            if (Testing != 0) {
                Testing = 0;
                throw new FreeSpaceManagerException.TestException();
            }
            commitNTA = true;
        } finally {
            /*
             * End nested top action
             */
            if (commitNTA) {
                trx.completeNestedTopAction();
                if (log.isDebugEnabled()) {
                    log.debug(
                    		FreeSpaceManagerImpl.class.getName(),
                        "createContainer",
                        "SIMPLEDBM-DEBUG: Created container " + containerName);
                }
            } else {
                trx.resetNestedTopAction();
            }
        }
    }

    /**
     * Devides x by y and rounds up the result to next multiple of y.
     */
    private final int DivideXByYAndRoundup(int x, int y) {
        return (x + y - 1) / y;
    }

    public final void extendContainer(Transaction trx, int containerId) {
        doExtendContainer(trx, containerId);
    }

    /**
     * Assumed that caller has an SHARED lock on the container.
     * 
     */
    public final void doExtendContainer(Transaction trx, int containerId) {

        BufferAccessBlock headerBab = bufmgr.fixExclusive(new PageId(
            containerId,
            HEADER_PAGE), false, -1, 0);
        try {
            HeaderPage hdrPage = (HeaderPage) headerBab.getPage();

            int dataPageType = hdrPage.getDataPageType();

            UpdateHeaderPage updateHeaderPage = new UpdateHeaderPage(
                MODULE_ID,
                FreeSpaceManagerImpl.TYPE_UPDATEHEADERPAGE);
            updateHeaderPage.newLastSpaceMapPage = updateHeaderPage.oldLastSpaceMapPage = hdrPage.lastSpaceMapPage;
            updateHeaderPage.newNumberOfExtents = updateHeaderPage.oldNumberOfExtents = hdrPage.lastSpaceMapPage;
            updateHeaderPage.newNumberOfSpaceMapPages = updateHeaderPage.oldNumberOfSpaceMapPages = hdrPage.numberOfSpaceMapPages;

            /* Calculate number of pages and fsips */
            int requiredExtents = hdrPage.numberOfExtents + 1;
            int requiredPages = requiredExtents * hdrPage.extentSize;
            int currentPages = hdrPage.numberOfExtents * hdrPage.extentSize;

            SpaceMapPageImpl smpPage = (SpaceMapPageImpl) pageFactory
                .getInstance(hdrPage.getSpaceMapPageType(), new PageId());
            int numberOfSpaceMapPagesRequired = DivideXByYAndRoundup(
                requiredPages,
                smpPage.getCount());

            int startingPageNumber = currentPages;
            // Initialize the new pages of the extent.
            for (int pageNumber = startingPageNumber; pageNumber < requiredPages; pageNumber++) {
                BufferAccessBlock bab = bufmgr.fixExclusive(new PageId(
                    containerId,
                    pageNumber), true, dataPageType, 0);
                try {
                    Page page = bab.getPage();
                    FormatRawPage formatRawPageLog = new FormatRawPage(
                        MODULE_ID,
                        FreeSpaceManagerImpl.TYPE_FORMATRAWPAGE);
                    formatRawPageLog.setDataPageType(dataPageType);
                    Lsn lsn = trx.logInsert(page, formatRawPageLog);
                    redo(page, formatRawPageLog);
                    bab.setDirty(lsn);
                } finally {
                    bab.unfix();
                }
            }
            updateHeaderPage.newNumberOfExtents = requiredExtents;

            trx.startNestedTopAction();
            boolean commitNTA = false;
            try {

                if (numberOfSpaceMapPagesRequired > hdrPage.numberOfSpaceMapPages) {
                    assert numberOfSpaceMapPagesRequired == hdrPage.numberOfSpaceMapPages + 1;

                    BufferAccessBlock lastSmpBab = bufmgr.fixExclusive(
                        new PageId(containerId, hdrPage.lastSpaceMapPage),
                        false,
                        -1,
                        0);
                    try {
                        SpaceMapPageImpl lastSmp = (SpaceMapPageImpl) lastSmpBab
                            .getPage();

                        int firstPageInNewSMP = lastSmp.getLastPageNumber() + 1;
                        int newSpaceMapPageNumber = firstPageInNewSMP;

                        updateHeaderPage.newLastSpaceMapPage = newSpaceMapPageNumber;
                        updateHeaderPage.newNumberOfSpaceMapPages = numberOfSpaceMapPagesRequired;

                        PageId newSmpPageId = new PageId(
                            containerId,
                            newSpaceMapPageNumber);
                        BufferAccessBlock newSmpBab = bufmgr.fixExclusive(
                            newSmpPageId,
                            true,
                            hdrPage.getSpaceMapPageType(),
                            0);
                        try {
                            SpaceMapPageImpl newSmp = (SpaceMapPageImpl) newSmpBab
                                .getPage();

                            FormatSpaceMapPage formatSMPPageLog = new FormatSpaceMapPage(
                                MODULE_ID,
                                FreeSpaceManagerImpl.TYPE_FORMATSPACEMAPPAGE);
                            formatSMPPageLog
                                .setFirstPageNumber(firstPageInNewSMP);
                            formatSMPPageLog.setNextSpaceMapPage(0);
                            Lsn lsn = trx.logInsert(newSmp, formatSMPPageLog);
                            redo(newSmp, formatSMPPageLog);

                            UpdateSpaceMapPage updateSpaceMapLog = new UpdateSpaceMapPage(
                                MODULE_ID,
                                FreeSpaceManagerImpl.TYPE_UPDATESPACEMAPPAGE);

                            updateSpaceMapLog
                                .setPageNumber(newSpaceMapPageNumber);
                            updateSpaceMapLog
                                .setSpaceValue(smpPage.fullValue());
                            lsn = trx.logInsert(newSmp, updateSpaceMapLog);
                            redo(newSmp, updateSpaceMapLog);
                            newSmpBab.setDirty(lsn);

                            LinkSpaceMapPage linkSpaceMapPageLog = new LinkSpaceMapPage(
                                MODULE_ID,
                                FreeSpaceManagerImpl.TYPE_LINKSPACEMAPPAGE);
                            linkSpaceMapPageLog.newNextSpaceMapPage = newSpaceMapPageNumber;
                            linkSpaceMapPageLog.oldNextSpaceMapPage = 0;
                            linkSpaceMapPageLog.setPageId(
                                lastSmp.getType(),
                                lastSmp.getPageId());
                            lsn = trx.logInsert(lastSmp, linkSpaceMapPageLog);
                            redo(lastSmp, linkSpaceMapPageLog);
                            lastSmpBab.setDirty(lsn);
                        } finally {
                            newSmpBab.unfix();
                        }
                    } finally {
                        lastSmpBab.unfix();
                    }
                }

                updateHeaderPage.setPageId(hdrPage.getType(), hdrPage
                    .getPageId());
                Lsn lsn = trx.logInsert(hdrPage, updateHeaderPage);
                redo(hdrPage, updateHeaderPage);
                headerBab.setDirty(lsn);
                commitNTA = true;
            } finally {
                if (commitNTA) {
                    trx.completeNestedTopAction();
                } else {
                    trx.resetNestedTopAction();
                }
            }
        } finally {
            headerBab.unfix();
        }
    }

    /**
     * Drops the container specified. Caller must hold an exclusive lock on the
     * container ID prior to this call. 
     * @see org.simpledbm.rss.api.fsm.FreeSpaceManager#dropContainer(org.simpledbm.rss.api.tx.Transaction, int)
     */
    public void dropContainer(Transaction trx, int containerId) {
        DropContainer logrec = new DropContainer(
                MODULE_ID,
                TYPE_DROPCONTAINER);
        StorageContainer sc;
        sc = storageManager.getInstance(containerId);
        if (sc == null) {
            log.error(FreeSpaceManagerImpl.class.getName(), "dropContainer", mcat.getMessage(
                "EF0002",
                containerId));
            throw new FreeSpaceManagerException(mcat.getMessage(
                "EF0002",
                containerId));
        } else {
            logrec.setContainerId(containerId);
            logrec.setName(sc.getName());
            trx.schedulePostCommitAction(logrec);
        }
    }

    public FreeSpaceScan openScan(int containerId) {
        return new SpaceScanImpl(this, containerId);
    }

    /**
     * Base class for container operations.
     */
    static abstract class ContainerOperation extends BaseLoggable {

        int containerId;
        ByteString name;

        protected ContainerOperation(int moduleId, int typeCode) {
			super(moduleId, typeCode);
            containerId = -1;
            name = new ByteString("");
		}

		protected ContainerOperation(ByteBuffer bb) {
			super(bb);
            containerId = bb.getInt();
            name = new ByteString(bb);
		}

        public final int getContainerId() {
            return containerId;
        }

        public final void setContainerId(int containerId) {
            this.containerId = containerId;
        }

        public final String getName() {
            return name.toString();
        }

        public final void setName(String name) {
            this.name = new ByteString(name);
        }

        @Override
        public int getStoredLength() {
            return super.getStoredLength() + TypeSize.INTEGER
                    + name.getStoredLength();
        }

        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(containerId);
            name.store(bb);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("containerId=").append(containerId).append(", name=").append(name.toString())
                    .append(",");
            super.appendTo(sb);
            return sb;
        }
        
        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
    }

    /**
     * Log for recording the creation of a container. This is an Undoable operation, 
     * therefore if the transaction that created the container aborts, the newly created
     * container will be dropped.
     */
    public static final class CreateContainer extends ContainerOperation
            implements Undoable {
        
        public CreateContainer(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public CreateContainer(ByteBuffer bb) {
			super(bb);
		}

		public StringBuilder appendTo(StringBuilder sb) {
            sb.append("CreateContainer(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }

        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
        static final class CreateContainerFactory implements ObjectFactory {
			public Class<?> getType() {
				return CreateContainer.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new CreateContainer(buf);
			}
        }
    }

    /**
     * Log for undoing a container create operation. After this is executed, all pages in the 
     * Buffer Manager for this container should be marked invalid.
     */
    public static final class UndoCreateContainer extends ContainerOperation
            implements Compensation, ContainerDeleteOperation {

        public UndoCreateContainer(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public UndoCreateContainer(ByteBuffer bb) {
			super(bb);
		}

		public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UndoCreateContainer(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
        
        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
        static final class UndoCreateContainerFactory implements ObjectFactory {

			public Class<?> getType() {
				return UndoCreateContainer.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new UndoCreateContainer(buf);
			}
        	
        }
    }

    /**
     * Logs the fact that a new container has been created and must be opened. 
     * This ensures that that the container is reopened during the redo phase.
     */
    public static final class OpenContainer extends ContainerOperation
            implements NonTransactionRelatedOperation {

        public OpenContainer(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public OpenContainer(ByteBuffer bb) {
			super(bb);
		}

		public StringBuilder appendTo(StringBuilder sb) {
            sb.append("OpenContainer(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
        
        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
        static final class OpenContainerFactory implements ObjectFactory {
			public Class<?> getType() {
				return OpenContainer.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new OpenContainer(buf);
			}
        }
    }

    /**
     * Log record for container delete operation.  
     */
    public static final class DropContainer extends ContainerOperation
            implements PostCommitAction, ContainerDeleteOperation {

        /**
         * Unique transaction specific action id to track the status of the operation.
         */
        int actionId;

        public DropContainer(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public DropContainer(ByteBuffer bb) {
			super(bb);
            actionId = bb.getInt();
		}

		public final int getActionId() {
            return actionId;
        }

        public final void setActionId(int actionId) {
            this.actionId = actionId;
        }

        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(actionId);
        }

        @Override
        public int getStoredLength() {

            int retValue;

            retValue = super.getStoredLength();
            retValue += TypeSize.INTEGER;
            return retValue;
        }

        @Override
        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("DropContainer(actionId=").append(actionId).append(", ");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
      
        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class DropContainerFactory implements ObjectFactory {

			public Class<?> getType() {
				return DropContainer.class;
			}

			public Object newInstance(ByteBuffer buf) {
				return new DropContainer(buf);
			}
        	
        }
        
    }

    /**
     * Log record for formatting a new page. 
     */
    public static final class FormatRawPage extends BaseLoggable implements
            Redoable, PageFormatOperation {

        int dataPageType;

        public FormatRawPage(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public FormatRawPage(ByteBuffer bb) {
			super(bb);
            dataPageType = bb.getInt();
		}

        public final int getDataPageType() {
            return dataPageType;
        }

        public final void setDataPageType(int dataPageType) {
            this.dataPageType = dataPageType;
        }

        @Override
        public final int getStoredLength() {
            return super.getStoredLength() + TypeSize.INTEGER;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(dataPageType);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("FormatRawPage(dataPageType=").append(dataPageType).append(", ");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
        
        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
        static final class FormatRawPageFactory implements ObjectFactory {

			public Class<?> getType() {
				return FormatRawPage.class;
			}

			public Object newInstance(ByteBuffer buf) {
				return new FormatRawPage(buf);
			}
        	
        }
    }

    /**
     * Formats the header page of a new container. This is a redo-only action. The container create
     * operation is handled as a nested top action - this means that if there is a problem, the 
     * container will be dropped. Hence, there is no need to undo the formatting of inidividual pages.
     */
    public static final class FormatHeaderPage extends BaseLoggable implements
            Redoable, PageFormatOperation {

        /**
         * Number of extents allocated to this container 
         */
        int numberOfExtents = 0;

        /**
         * Extent size in number of pages
         */
        int extentSize = 16;

        /**
         * Number of fsip pages allocated
         */
        int numberOfSpaceMapPages = 0;

        /**
         * Last space map page.
         */
        int lastSpaceMapPage = -1;

        /**
         * First space map page.
         */
        int firstSpaceMapPage = -1;

        /**
         * Type of space map page;
         */
        int spaceMapPageType = -1;

        /**
         * Type of data pages.
         */
        int dataPageType = -1;

        public FormatHeaderPage(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public FormatHeaderPage(ByteBuffer bb) {
			super(bb);
            numberOfExtents = bb.getInt();
            extentSize = bb.getInt();
            numberOfSpaceMapPages = bb.getInt();
            lastSpaceMapPage = bb.getInt();
            firstSpaceMapPage = bb.getInt();
            spaceMapPageType = bb.getInt();
            dataPageType = bb.getInt();
		}

        public final int getExtentSize() {
            return extentSize;
        }

        public final void setExtentSize(int extentSize) {
            this.extentSize = extentSize;
        }

        public final int getFirstSpaceMapPage() {
            return firstSpaceMapPage;
        }

        public final void setFirstSpaceMapPage(int firstSpaceMapPage) {
            this.firstSpaceMapPage = firstSpaceMapPage;
        }

        public final int getLastSpaceMapPage() {
            return lastSpaceMapPage;
        }

        public final void setLastSpaceMapPage(int lastSpaceMapPage) {
            this.lastSpaceMapPage = lastSpaceMapPage;
        }

        public final int getNumberOfExtents() {
            return numberOfExtents;
        }

        public final void setNumberOfExtents(int numberOfExtents) {
            this.numberOfExtents = numberOfExtents;
        }

        public final int getNumberOfSpaceMapPages() {
            return numberOfSpaceMapPages;
        }

        public final void setNumberOfSpaceMapPages(int numberOfSpaceMapPages) {
            this.numberOfSpaceMapPages = numberOfSpaceMapPages;
        }

        public final int getSpaceMapPageType() {
            return spaceMapPageType;
        }

        public final void setSpaceMapPageType(int spaceMapPageType) {
            this.spaceMapPageType = spaceMapPageType;
        }

        public final int getDataPageType() {
            return dataPageType;
        }

        public final void setDataPageType(int dataPageType) {
            this.dataPageType = dataPageType;
        }

        @Override
        public final int getStoredLength() {
            return super.getStoredLength() + TypeSize.INTEGER * 7;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(numberOfExtents);
            bb.putInt(extentSize);
            bb.putInt(numberOfSpaceMapPages);
            bb.putInt(lastSpaceMapPage);
            bb.putInt(firstSpaceMapPage);
            bb.putInt(spaceMapPageType);
            bb.putInt(dataPageType);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("FormatHeaderPage(numberOfExtents=").append(numberOfExtents)
                    .append(", extentSize=").append(extentSize)
                    .append(", numberOfSpaceMapPages=").append(numberOfSpaceMapPages)
                    .append(", lastSpaceMapPage=").append(lastSpaceMapPage)
                    .append(", firstSpaceMapPage=").append(firstSpaceMapPage);
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
        
        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
        static final class FormatHeaderPageFactory implements ObjectFactory {
			public Class<?> getType() {
				return FormatHeaderPage.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new FormatHeaderPage(buf);
			}
        }
    }

    /**
     * Log record for updating the header page. 
     */
    public static final class UpdateHeaderPage extends BaseLoggable implements
            Undoable {

        /**
         * Number of extents allocated to this container 
         */
        int oldNumberOfExtents = 0;
        int newNumberOfExtents = 0;

        /**
         * Number of fsip pages allocated
         */
        int oldNumberOfSpaceMapPages = 0;
        int newNumberOfSpaceMapPages = 0;

        /**
         * Last space map page.
         */
        int oldLastSpaceMapPage = -1;
        int newLastSpaceMapPage = -1;

        public UpdateHeaderPage(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public UpdateHeaderPage(ByteBuffer bb) {
			super(bb);
            oldNumberOfExtents = bb.getInt();
            newNumberOfExtents = bb.getInt();
            oldNumberOfSpaceMapPages = bb.getInt();
            newNumberOfSpaceMapPages = bb.getInt();
            oldLastSpaceMapPage = bb.getInt();
            newLastSpaceMapPage = bb.getInt();
		}

        @Override
        public final int getStoredLength() {
            return super.getStoredLength() + TypeSize.INTEGER * 6;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(oldNumberOfExtents);
            bb.putInt(newNumberOfExtents);
            bb.putInt(oldNumberOfSpaceMapPages);
            bb.putInt(newNumberOfSpaceMapPages);
            bb.putInt(oldLastSpaceMapPage);
            bb.putInt(newLastSpaceMapPage);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UpdateHeaderPage(oldNumberOfExtents=").append(oldNumberOfExtents);
            sb.append(", newNumberOfExtents=").append(newNumberOfExtents);
            sb.append(", oldNumberOfSpaceMapPages=").append(oldNumberOfSpaceMapPages);
            sb.append(", newNumberOfSpaceMapPages=").append(newNumberOfSpaceMapPages);
            sb.append(", oldLastSpaceMapPage=").append(oldLastSpaceMapPage);
            sb.append(", newLastSpaceMapPage=").append(newLastSpaceMapPage);
            sb.append(", ");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
        
        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
        static final class UpdateHeaderPageFactory implements ObjectFactory {
			public Class<?> getType() {
				return UpdateHeaderPage.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new UpdateHeaderPage(buf);
			}
        	
        }
    }

    public static final class UndoUpdateHeaderPage extends BaseLoggable
            implements Compensation {

        /**
         * Number of extents allocated to this container 
         */
        int oldNumberOfExtents = 0;

        /**
         * Number of fsip pages allocated
         */
        int oldNumberOfSpaceMapPages = 0;

        /**
         * Last space map page.
         */
        int oldLastSpaceMapPage = -1;

        public UndoUpdateHeaderPage(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public UndoUpdateHeaderPage(ByteBuffer bb) {
			super(bb);
            oldNumberOfExtents = bb.getInt();
            oldNumberOfSpaceMapPages = bb.getInt();
            oldLastSpaceMapPage = bb.getInt();
		}

        @Override
        public final int getStoredLength() {
            return super.getStoredLength() + TypeSize.INTEGER * 3;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(oldNumberOfExtents);
            bb.putInt(oldNumberOfSpaceMapPages);
            bb.putInt(oldLastSpaceMapPage);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UndoUpdateHeaderPage(oldNumberOfExtents=").append(oldNumberOfExtents);
            sb.append(", oldNumberOfSpaceMapPages=").append(oldNumberOfSpaceMapPages);
            sb.append(", oldLastSpaceMapPage=").append(oldLastSpaceMapPage);
            sb.append(", ");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
        
        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
        static final class UndoUpdateHeaderPageFactory implements ObjectFactory {

			public Class<?> getType() {
				return UndoUpdateHeaderPage.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new UndoUpdateHeaderPage(buf);
			}
        	
        }
    }

    /**
     * Log record for formatting a new space map page. This is redo-only. 
     */
    public static final class FormatSpaceMapPage extends BaseLoggable implements
            Redoable, PageFormatOperation {

        /**
         * First page in this FSIP page
         */
        int firstPageNumber = -1;

        /**
         * Pointer to next FSIP page
         */
        int nextSpaceMapPage = -1;

        public FormatSpaceMapPage(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public FormatSpaceMapPage(ByteBuffer bb) {
			super(bb);
            firstPageNumber = bb.getInt();
            nextSpaceMapPage = bb.getInt();
		}

		public final int getFirstPageNumber() {
            return firstPageNumber;
        }

        public final void setFirstPageNumber(int firstPageNumber) {
            this.firstPageNumber = firstPageNumber;
        }

        public final int getNextSpaceMapPage() {
            return nextSpaceMapPage;
        }

        public final void setNextSpaceMapPage(int nextSpaceMapPage) {
            this.nextSpaceMapPage = nextSpaceMapPage;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(firstPageNumber);
            bb.putInt(nextSpaceMapPage);
        }

        @Override
        public final int getStoredLength() {
            return super.getStoredLength() + TypeSize.INTEGER * 2;
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("FormatSpaceMapPage(firstPageNumber=").append(firstPageNumber);
            sb.append(", nextSpaceMapPage=").append(nextSpaceMapPage);
            sb.append(", ");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
        
        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class FormatSpaceMapPageFactory implements ObjectFactory {
			public Class<?> getType() {
				return FormatSpaceMapPage.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new FormatSpaceMapPage(buf);
			}
        }
    }

    static abstract class BaseUpdateSpaceMapPage extends BaseLoggable {

        int pageNumber;
        int spaceValue;

        public BaseUpdateSpaceMapPage(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public BaseUpdateSpaceMapPage(ByteBuffer bb) {
			super(bb);
            pageNumber = bb.getInt();
            spaceValue = bb.getInt();
		}

        public final int getPageNumber() {
            return pageNumber;
        }

        public final void setPageNumber(int index) {
            this.pageNumber = index;
        }

        public final int getSpaceValue() {
            return spaceValue;
        }

        public final void setSpaceValue(int newValue) {
            this.spaceValue = newValue;
        }

        @Override
        public int getStoredLength() {
            return super.getStoredLength() + TypeSize.INTEGER * 2;
        }

        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(pageNumber);
            bb.putInt(spaceValue);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("pageNumber=").append(pageNumber);
            sb.append(", spaceValue=").append(spaceValue);
            sb.append(", ");
            super.appendTo(sb);
            return sb;
        }
        
        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }

    }

    static abstract class BaseUndoableUpdateSpaceMapPage extends
            BaseUpdateSpaceMapPage {

        int oldSpaceValue;
        
        public BaseUndoableUpdateSpaceMapPage(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public BaseUndoableUpdateSpaceMapPage(ByteBuffer bb) {
			super(bb);
            oldSpaceValue = bb.getInt();
		}

        @Override
        public final int getStoredLength() {
            return super.getStoredLength() + TypeSize.INTEGER;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(oldSpaceValue);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("oldSpaceValue=").append(oldSpaceValue).append(", ");
            super.appendTo(sb);
            return sb;
        }
        
        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }

    }

    /**
     * A redo-only update of the space map page information.
     */
    public static final class UpdateSpaceMapPage extends BaseUpdateSpaceMapPage
            implements Redoable {

        public UpdateSpaceMapPage(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public UpdateSpaceMapPage(ByteBuffer bb) {
			super(bb);
		}

		public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UpdateSpaceMapPage(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
        
        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
        static final class UpdateSpaceMapPageFactory implements ObjectFactory {
			public Class<?> getType() {
				return UpdateSpaceMapPage.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new UpdateSpaceMapPage(buf);
			}
        }
    }

    /**
     * Undoable update of space map page information.
     */
    public static final class UndoableUpdateSpaceMapPage extends
            BaseUndoableUpdateSpaceMapPage implements Undoable {
    	
        public UndoableUpdateSpaceMapPage(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public UndoableUpdateSpaceMapPage(ByteBuffer bb) {
			super(bb);
		}

		public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UndoableUpdateSpaceMapPage(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
        
        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
        static final class UndoableUpdateSpaceMapPageFactory implements ObjectFactory {

			public Class<?> getType() {
				return UndoableUpdateSpaceMapPage.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new UndoableUpdateSpaceMapPage(buf);
			}
        	
        }
    }

    /**
     * Log record for undoing a space map page update.
     */
    public static final class UndoSpaceMapPageUpdate extends
            BaseUpdateSpaceMapPage implements Compensation {

        public UndoSpaceMapPageUpdate(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public UndoSpaceMapPageUpdate(ByteBuffer bb) {
			super(bb);
		}

		public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UndoSpaceMapPageUpdate(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
        
        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class UndoSpaceMapUpdateFactory implements ObjectFactory {

			public Class<?> getType() {
				return UndoSpaceMapPageUpdate.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new UndoSpaceMapPageUpdate(buf);
			}
        	
        }
    }

    /**
     * Log record for linking a new space map page to the linked list of 
     * space map pages.
     */
    public static final class LinkSpaceMapPage extends BaseLoggable implements
            Undoable {

        /**
         * First page in this FSIP page
         */
        int oldNextSpaceMapPage = -1;

        /**
         * Pointer to next FSIP page
         */
        int newNextSpaceMapPage = -1;

        public LinkSpaceMapPage(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public LinkSpaceMapPage(ByteBuffer bb) {
			super(bb);
            oldNextSpaceMapPage = bb.getInt();
            newNextSpaceMapPage = bb.getInt();
		}

		@Override
        public final int getStoredLength() {
            return super.getStoredLength() + TypeSize.INTEGER * 2;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(oldNextSpaceMapPage);
            bb.putInt(newNextSpaceMapPage);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("LinkSpaceMapPage(oldNirstPageNumber = ").append(oldNextSpaceMapPage);
            sb.append(", newNextSpaceMapPage=").append(newNextSpaceMapPage);
            sb.append(", ");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
        
        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class LinkSpaceMapPageFactory implements ObjectFactory {

			public Class<?> getType() {
				return LinkSpaceMapPage.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new LinkSpaceMapPage(buf);
			}
        	
        }
    }

    /**
     * Undo the linking of a new space map page to the linked list of space map pages.
     */
    public static final class UndoLinkSpaceMapPage extends BaseLoggable
            implements Compensation {

        /**
         * First page in this FSIP page
         */
        int oldNextSpaceMapPage = -1;

        public UndoLinkSpaceMapPage(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public UndoLinkSpaceMapPage(ByteBuffer bb) {
			super(bb);
            oldNextSpaceMapPage = bb.getInt();
		}

		@Override
        public final int getStoredLength() {
            return super.getStoredLength() + TypeSize.INTEGER;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(oldNextSpaceMapPage);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("UndoLinkSpaceMapPage(oldNextSpaceMapPage=").append(oldNextSpaceMapPage);
            sb.append(", ");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
        
        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class UndoLinkSpaceMapPageFactory implements ObjectFactory {

			public Class<?> getType() {
				return UndoLinkSpaceMapPage.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new UndoLinkSpaceMapPage(buf);
			}
        	
        }
    }

    /**
     * Implementation of the header page.
     */
    public static final class HeaderPage extends Page {

        /**
         * Number of extents allocated to this container 
         */
        int numberOfExtents = 0;

        /**
         * Extent size in number of pages
         */
        int extentSize = 16;

        /**
         * Number of fsip pages allocated
         */
        int numberOfSpaceMapPages = 0;

        /**
         * Last space map page.
         */
        int lastSpaceMapPage = -1;

        /**
         * First space map page.
         */
        int firstSpaceMapPage = -1;

        /**
         * Type of space map page. 
         */
        int spaceMapPageType = -1;

        /**
         * The type of pages stored in this container. All pages apart from
         * the header page and the space map pages will be of this type.
         */
        int dataPageType = -1;

        public HeaderPage(PageManager pageFactory, int type, PageId pageId) {
			super(pageFactory, type, pageId);
		}

		public HeaderPage(PageManager pageFactory, PageId pageId, ByteBuffer bb) {
			super(pageFactory, pageId, bb);
            numberOfExtents = bb.getInt();
            extentSize = bb.getInt();
            numberOfSpaceMapPages = bb.getInt();
            lastSpaceMapPage = bb.getInt();
            firstSpaceMapPage = bb.getInt();
            spaceMapPageType = bb.getInt();
            dataPageType = bb.getInt();
		}

		public final int getExtentSize() {
            return extentSize;
        }

        public final void setExtentSize(int extentSize) {
            this.extentSize = extentSize;
        }

        public final int getFirstSpaceMapPage() {
            return firstSpaceMapPage;
        }

        public final void setFirstSpaceMapPage(int firstSpaceMapPage) {
            this.firstSpaceMapPage = firstSpaceMapPage;
        }

        public final int getLastSpaceMapPage() {
            return lastSpaceMapPage;
        }

        public final void setLastSpaceMapPage(int lastSpaceMapPage) {
            this.lastSpaceMapPage = lastSpaceMapPage;
        }

        public final int getNumberOfExtents() {
            return numberOfExtents;
        }

        public final void setNumberOfExtents(int numberOfExtents) {
            this.numberOfExtents = numberOfExtents;
        }

        public final int getNumberOfSpaceMapPages() {
            return numberOfSpaceMapPages;
        }

        public final int getMaximumPageNumber() {
            /*
             * Since page numbers start from 0.
             */
            return (numberOfExtents * extentSize) - 1;
        }

        public final void setNumberOfSpaceMapPages(int numberOfSpaceMapPages) {
            this.numberOfSpaceMapPages = numberOfSpaceMapPages;
        }

        public final int getSpaceMapPageType() {
            return spaceMapPageType;
        }

        public final void setSpaceMapPageType(int spaceMapPageType) {
            this.spaceMapPageType = spaceMapPageType;
        }

        public final int getDataPageType() {
            return dataPageType;
        }

        public final void setDataPageType(int dataPageType) {
            this.dataPageType = dataPageType;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(numberOfExtents);
            bb.putInt(extentSize);
            bb.putInt(numberOfSpaceMapPages);
            bb.putInt(lastSpaceMapPage);
            bb.putInt(firstSpaceMapPage);
            bb.putInt(spaceMapPageType);
            bb.putInt(dataPageType);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("HeaderPage(numberOfExtents=").append(numberOfExtents);
            sb.append(", extentSize=").append(extentSize);
            sb.append(", numberOfSpaceMapPages=").append(numberOfSpaceMapPages);
            sb.append(", lastSpaceMapPage=").append(lastSpaceMapPage);
            sb.append(", firstSpaceMapPage=").append(firstSpaceMapPage);
            sb.append(", spaceMapPageType=").append(spaceMapPageType);
            sb.append(", dataPageType=").append(dataPageType);
            sb.append(", ");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
        
        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
        static class HeaderPageFactory implements PageFactory {
        	final PageManager pageFactory;
        	public HeaderPageFactory(PageManager pageFactory) {
        		this.pageFactory = pageFactory;
        	}
			public Page getInstance(int type, PageId pageId) {
				return new HeaderPage(pageFactory, type, pageId);
			}
			public Page getInstance(PageId pageId, ByteBuffer bb) {
				return new HeaderPage(pageFactory, pageId, bb);
			}
			public int getPageType() {
				return FreeSpaceManagerImpl.TYPE_HEADERPAGE;
			}
        }
    }

    /**
     * Base class for all space map page implementations. 
     */
    public static abstract class SpaceMapPageImpl extends FreeSpaceMapPage {

        static final int OVERHEAD = (Integer.SIZE / Byte.SIZE) * 2;

        static final int SIZE = Page.SIZE + OVERHEAD;        
        
        public static boolean TESTING = false;

        /**
         * First page in this FSIP page
         */
        int firstPageNumber = -1;

        /**
         * Last page in this FSIP page
         * NON-Persistent.
         */
        int lastPageNumber = -1;

        /**
         * Pointer to next FSIP page
         */
        int nextSpaceMapPage = -1;

        byte[] bits;

		protected SpaceMapPageImpl(PageManager pageFactory, int type,
				PageId pageId) {
			super(pageFactory, type, pageId);
			bits = new byte[getSpace()];
		}

		protected SpaceMapPageImpl(PageManager pageFactory, PageId pageId,
				ByteBuffer bb) {
			super(pageFactory, pageId, bb);
            firstPageNumber = bb.getInt();
            nextSpaceMapPage = bb.getInt();
            lastPageNumber = firstPageNumber + getCount() - 1;
            bits = new byte[getSpace()];
            bb.get(bits);
		}

		public abstract int get(int offset);

        public abstract void set(int offset, int value);

        public abstract int getCount();

        public abstract int fullValue();

        public abstract int emptyValue();

        public final int getFirstPageNumber() {
            return firstPageNumber;
        }

        public final void setFirstPageNumber(int firstPageNumber) {
            this.firstPageNumber = firstPageNumber;
            lastPageNumber = firstPageNumber + getCount() - 1;
        }

        public final int getLastPageNumber() {
            return lastPageNumber;
        }

        public final int getNextSpaceMapPage() {
            return nextSpaceMapPage;
        }

        public final void setNextSpaceMapPage(int nextSpaceMapPage) {
            this.nextSpaceMapPage = nextSpaceMapPage;
        }

        public final boolean contains(int pageNumber) {
            return pageNumber >= firstPageNumber
                    && pageNumber <= lastPageNumber;
        }

        final int convertPageNumberToOffset(int pageNumber) {
            return pageNumber - firstPageNumber;
        }

        @Override
        public final int getSpaceBits(int pageNumber) {
            if (contains(pageNumber)) {
                return get(convertPageNumberToOffset(pageNumber));
            }
            log.error(SpaceMapPageImpl.class.getName(), "getSpaceBits", mcat.getMessage(
                "EF0004",
                pageNumber,
                this));
            throw new FreeSpaceManagerException(mcat.getMessage(
                "EF0004",
                pageNumber,
                this));
        }

        @Override
        public final void setSpaceBits(int pageNumber, int value) {
            if (contains(pageNumber)) {
                set(convertPageNumberToOffset(pageNumber), value);
            } else {
                log.error(SpaceMapPageImpl.class.getName(), "setSpaceBits", mcat.getMessage(
                    "EF0004",
                    pageNumber,
                    this));
                throw new FreeSpaceManagerException(mcat.getMessage(
                    "EF0004",
                    pageNumber,
                    this));
            }
        }

        final int getSpace() {
//            return super.getStoredLength() - SIZE;
        	return getAvailableLength() - OVERHEAD;
        }

        final void init() {
            bits = new byte[getSpace()];
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(firstPageNumber);
            bb.putInt(nextSpaceMapPage);
            bb.put(bits);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("SpaceMapPageImpl(firstPageNumber=").append(firstPageNumber);
            sb.append(", nextSpaceMapPage=").append(nextSpaceMapPage);
            sb.append(", pageCount=").append(getCount());
            sb.append(", ");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }
        
        
        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }
    }

    /**
     * Implementation of a space map page that uses 2 bits of storage per page. 
     * This allows upto 4 values (0-3) to be stored against each page.
     */
    public static final class TwoBitSpaceMapPage extends SpaceMapPageImpl {
	
		TwoBitSpaceMapPage(PageManager pageFactory, int type, PageId pageId) {
			super(pageFactory, type, pageId);
		}

		TwoBitSpaceMapPage(PageManager pageFactory, PageId pageId, ByteBuffer bb) {
			super(pageFactory, pageId, bb);
		}

		@Override
        public final int get(int map_index) {
            int offset = map_index / 4;
            int part = (map_index % 4) * 2;

            int value;

            value = bits[offset];
            value = value & (3 << part); /* Extract value */
            value = value >> part; /* Right justify it */

            return value;
        }

        @Override
        public final void set(int map_index, int newvalue) {
            int offset = map_index / 4;
            int part = (map_index % 4) * 2;

            int value;

            value = bits[offset];
            value = value & ~(3 << part); /* First clear out existing value */
            value = value | (newvalue << part); /* Now set new value */
            bits[offset] = (byte) value;
        }

        @Override
        public final int getCount() {
            if (TESTING)
                return 10;
            return getSpace() * Byte.SIZE / 2;
        }

        @Override
        public final int fullValue() {
            return 3;
        }

        @Override
        public final int emptyValue() {
            return 0;
        }
        
        static final class TwoBitSpaceMapPageFactory implements PageFactory {
        	private final PageManager pageFactory;
        	public TwoBitSpaceMapPageFactory(PageManager pageFactory) {
        		this.pageFactory = pageFactory;
        	}
			public Page getInstance(int type, PageId pageId) {
				return new TwoBitSpaceMapPage(pageFactory, type, pageId);
			}
			public Page getInstance(PageId pageId, ByteBuffer bb) {
				return new TwoBitSpaceMapPage(pageFactory, pageId, bb);
			}
			public int getPageType() {
				return FreeSpaceManagerImpl.TYPE_TWOBITSPACEMAPPAGE;
			}
        }
    }

    /**
     * Implementation of a space map page that uses 1 bit of storage per page. 
     * This allows upto 2 values (0-1) to be stored against each page.
     */
    public static final class OneBitSpaceMapPage extends SpaceMapPageImpl {

        final int ESIZE = Byte.SIZE;

		OneBitSpaceMapPage(PageManager pageFactory, int type, PageId pageId) {
			super(pageFactory, type, pageId);
		}

		OneBitSpaceMapPage(PageManager pageFactory, PageId pageId, ByteBuffer bb) {
			super(pageFactory, pageId, bb);
		}

		@Override
        public final int get(int bit) {
            int n = bit / ESIZE;
            int part = bit % ESIZE;
            if ((bits[n] & (1 << part)) != 0) {
                return 1;
            }
            return 0;
        }

        @Override
        public final void set(int bit, int value) {
            int n = bit / ESIZE;
            int part = bit % ESIZE;
            if (value == 0) {
                bits[n] &= ~(1 << part);
            } else {
                bits[n] |= (1 << part);
            }
        }

        @Override
        public final int getCount() {
            if (TESTING)
                return 10;
            return getSpace() * ESIZE;
        }

        @Override
        public final int fullValue() {
            return 1;
        }

        @Override
        public final int emptyValue() {
            return 0;
        }
        
        static final class OneBitSpaceMapPageFactory implements PageFactory {
        	private final PageManager pageFactory;
        	OneBitSpaceMapPageFactory(PageManager pageFactory) {
        		this.pageFactory = pageFactory;
        	}
			public Page getInstance(int type, PageId pageId) {
				return new OneBitSpaceMapPage(pageFactory, type, pageId);
			}
			public Page getInstance(PageId pageId, ByteBuffer bb) {
				return new OneBitSpaceMapPage(pageFactory, pageId, bb);
			}
			public int getPageType() {
				return FreeSpaceManagerImpl.TYPE_ONEBITSPACEMAPPAGE;
			}
        }
    }

    public static final class SpaceScanImpl implements FreeSpaceScan {

        final SpaceCursorImpl cursor;

        boolean eof = false;

        int pageNumber = 0;

        /**
         * Tracks the wrapCount of the SpaceCursor so that the
         * scan can detect EOF. SpaceCursor is designed to wrap around
         * when it reaches the end of container as long as it can find
         * a page that matches the search criteria.
         */
        int startWrapCount = 0;

        public SpaceScanImpl(FreeSpaceManagerImpl spacemgr, int containerId) {
            this.cursor = (SpaceCursorImpl) spacemgr
                .getSpaceCursor(containerId);
            this.startWrapCount = cursor.getWrapCount();
        }

        public boolean fetchNext() {
            if (isEof()) {
                return false;
            }
            try {
                pageNumber = cursor.doFindAndFixSMP(new FreeSpaceChecker() {
                    public boolean hasSpace(int value) {
                        return value >= 0;
                    }
                }, false);
            } finally {
                cursor.unfixCurrentSpaceMapPage();
            }
            if (pageNumber == -1 || cursor.getWrapCount() > startWrapCount) {
                eof = true;
                return false;
            }
            return true;
        }

        public int getCurrentPage() {
            return pageNumber;
        }

        public boolean isEof() {
            return eof;
        }

        public void close() {
        }

    }

    public static final class SpaceCursorImpl implements FreeSpaceCursor {

        final FreeSpaceManagerImpl spacemgr;

        final int containerId;

        int currentSMP = FreeSpaceManagerImpl.FIRST_SPACE_MAP_PAGE;

        int currentPageNumber = FreeSpaceManagerImpl.FIRST_USER_PAGE;

        /**
         * Cached value of last SMP.
         */
        int lastSMP;

        /**
         * Cached value of maximum page number.
         */
        int maxPageNumber;

        BufferAccessBlock bab = null;

        SpaceMapPageImpl smpPage;

        int spaceMapType = -1;

        int dataPageType = -1;

        int wrapCount = 0;

        public final int getWrapCount() {
            return wrapCount;
        }

        void initScan() {
            if (spaceMapType == -1) {
                readHeaderPage();
            }
        }

        public SpaceCursorImpl(FreeSpaceManagerImpl spacemgr, int containerId) {
            this.spacemgr = spacemgr;
            this.containerId = containerId;
        }

        final void readHeaderPage() {
            BufferAccessBlock bab;

            bab = spacemgr.bufmgr.fixShared(new PageId(
                containerId,
                FreeSpaceManagerImpl.HEADER_PAGE), 0);
            try {
                HeaderPage page = (HeaderPage) bab.getPage();
                lastSMP = page.getLastSpaceMapPage();
                maxPageNumber = page.getMaximumPageNumber();
                if (spaceMapType == -1) {
                    spaceMapType = page.getSpaceMapPageType();
                    smpPage = (SpaceMapPageImpl) spacemgr.pageFactory
                        .getInstance(spaceMapType, new PageId());
                }
                if (dataPageType == -1) {
                    dataPageType = page.getDataPageType();
                }
            } finally {
                bab.unfix();
            }
        }

        /* (non-Javadoc)
         * @see org.simpledbm.rss.sm.SpaceCursor#findAndFixSpaceMapPageExclusively(org.simpledbm.rss.sm.SpaceChecker)
         */
        public final int findAndFixSpaceMapPageExclusively(
                FreeSpaceChecker checker) {
            return doFindAndFixSMP(checker, true);
        }

        public int findAndFixSpaceMapPageShared(FreeSpaceChecker checker) {
            return doFindAndFixSMP(checker, false);
		}

		final int doFindAndFixSMP(FreeSpaceChecker checker, boolean exclusive) {

            initScan();

            /* 
             * We start the search from where the last search ended. This is so that
             * we avoid searching from the beginning every time.
             */

            int stopSMP = lastSMP;

            int numPasses = 0;
            if (currentPageNumber == FreeSpaceManagerImpl.FIRST_USER_PAGE
                    || currentPageNumber > maxPageNumber) {
                numPasses = 1;
                if (currentPageNumber > maxPageNumber) {
                    currentSMP = FreeSpaceManagerImpl.FIRST_SPACE_MAP_PAGE;
                    currentPageNumber = FreeSpaceManagerImpl.FIRST_USER_PAGE;
                    wrapCount++;
                }
            } else {
                numPasses = 2;
            }

            if (log.isDebugEnabled()) {
                log.debug(
                    SpaceCursorImpl.class.getName(),
                    "findAndFixSMPExclusively",
                    "SIMPLEDBM-DEBUG: Starting search for empty page with currentSMP="
                            + currentSMP + " currentPageNumber="
                            + currentPageNumber + " numPasses=" + numPasses);
            }
            /* 
             * The search is executed in two passes. The first pass begins searching from
             * the last remembered FSIP page. If this search fails, the second pass starts
             * from the beginning.
             */
            PageId smpPageId = new PageId(containerId, currentSMP);
            boolean found = false;
            search: for (int pass = 0; pass < numPasses && !found; pass++) {

                if (log.isDebugEnabled()) {
                    log.debug(
                    		SpaceCursorImpl.class.getName(),
                        "findAndFixSMPExclusively",
                        "SIMPLEDBM-DEBUG: Pass=" + pass
                                + " StartingPageNumber=" + currentPageNumber
                                + "currentSMP=" + smpPageId.getPageNumber()
                                + " stopSMP=" + stopSMP);
                }

                while (pass < numPasses) {

                    /*
                     * We have to latch exclusively - it would be better to latch in update mode,
                     * and upgrade to exclusive if the change is made.
                     */
                    if (exclusive) {
                        bab = spacemgr.bufmgr.fixExclusive(
                            smpPageId,
                            false,
                            -1,
                            0);
                    } else {
                        bab = spacemgr.bufmgr.fixShared(smpPageId, 0);
                    }

                    int nextSMP = 0;
                    int lastPageNumber = 0;
                    try {

                        SpaceMapPageImpl smpPage = (SpaceMapPageImpl) bab
                            .getPage();

                        if (log.isDebugEnabled()) {
                            log.debug(
                            		SpaceCursorImpl.class.getName(),
                                "findAndFixSMPExclusively",
                                "SIMPLEDBM-DEBUG: Fixed SpaceMap Page "
                                        + smpPage);
                        }

                        lastPageNumber = smpPage.getLastPageNumber();
                        for (; currentPageNumber <= lastPageNumber
                                && currentPageNumber <= maxPageNumber; currentPageNumber++) {

                            int space = smpPage.getSpaceBits(currentPageNumber);
                            if (checker.hasSpace(space)) {
                                /*
                                 * Found a page with enough space.
                                 */
                                if (log.isDebugEnabled()) {
                                    log.debug(
                                    		SpaceCursorImpl.class.getName(),
                                        "findAndFixSMPExclusively",
                                        "SIMPLEDBM-DEBUG: Found requested space "
                                                + space + " in page "
                                                + currentPageNumber);
                                }
                                found = true;
                                currentSMP = smpPageId.getPageNumber();
                                return currentPageNumber++;
                            }
                        }
                        nextSMP = smpPage.getNextSpaceMapPage();
                    } finally {
                        if (!found) {
                            bab.unfix();
                            bab = null;
                        }
                    }

                    if (nextSMP > lastSMP) {
                        readHeaderPage();
                    } else if (nextSMP == 0) {
                        /*
                         * Bumped EOF. Refresh header.
                         */
                        int savedMaxPageNumber = maxPageNumber;
                        readHeaderPage();
                        if (maxPageNumber > savedMaxPageNumber) {
                            /*
                             * More pages have been added.
                             * We cannot do following because the next SMP may not be the
                             * last one, and if we skip an SMP then page number will
                             * be incorrect and it will fail when trying to access space
                             * info.
                             *  
                             * if (currentPageNumber > lastPageNumber) {
                             *  smpPageId = new PageId(containerId, lastSMP);
                             * }
                             */
                            continue;
                        }
                        break;
                    }

                    if (pass == 1 && (nextSMP == 0 || nextSMP > stopSMP)) {
                        break search;
                    } else {
                        smpPageId = new PageId(containerId, nextSMP);
                    }
                }

                /*
                 * We have hit EOF
                 */
                currentPageNumber = FreeSpaceManagerImpl.FIRST_USER_PAGE;
                smpPageId = new PageId(
                    containerId,
                    FreeSpaceManagerImpl.FIRST_SPACE_MAP_PAGE);
                stopSMP = currentSMP - 1;
                wrapCount++;

            }

            currentSMP = FreeSpaceManagerImpl.FIRST_SPACE_MAP_PAGE;
            currentPageNumber = FreeSpaceManagerImpl.FIRST_USER_PAGE;

            return -1;
        }

        /* (non-Javadoc)
         * @see org.simpledbm.rss.sm.SpaceCursor#fixSpaceMapPageExclusively(int, int)
         */
        public final void fixSpaceMapPageExclusively(int spaceMapPageNumber,
                int pageNumber) {
            if (bab != null) {
                log.error(SpaceCursorImpl.class.getName(), "fixSpaceMapPageExclusively", mcat
                    .getMessage("EF0005"));
                throw new IllegalStateException(mcat.getMessage("EF0005"));
            }
            PageId smpPageId = new PageId(containerId, spaceMapPageNumber);
            bab = spacemgr.bufmgr.fixExclusive(smpPageId, false, -1, 0);
            SpaceMapPageImpl smpPage = (SpaceMapPageImpl) bab.getPage();
            if (!smpPage.contains(pageNumber)) {
                bab.unfix();
                bab = null;
                log.error(SpaceCursorImpl.class.getName(), "fixSpaceMapPageExclusively", mcat
                    .getMessage("EF0004", pageNumber, smpPage));
                throw new FreeSpaceManagerException(mcat.getMessage(
                    "EF0004",
                    pageNumber,
                    smpPage));
            }
            currentSMP = spaceMapPageNumber;
            currentPageNumber = pageNumber;
        }

        /* (non-Javadoc)
         * @see org.simpledbm.rss.sm.SpaceCursor#unfixCurrentSpaceMapPage()
         */
        public final void unfixCurrentSpaceMapPage() {
            if (bab == null) {
                return;
            }
            bab.unfix();
            bab = null;
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.simpledbm.rss.sm.SpaceCursor#getCurrentSpaceMapPage()
         */
        public final FreeSpaceMapPage getCurrentSpaceMapPage() {
            if (bab == null) {
                log.error(SpaceCursorImpl.class.getName(), "getCurrentSpaceMapPage", mcat
                    .getMessage("EF0006"));
                throw new IllegalStateException(mcat.getMessage("EF0006"));
            }
            return (FreeSpaceMapPage) bab.getPage();
        }

        /* (non-Javadoc)
         * @see org.simpledbm.rss.sm.SpaceCursor#updateAndLogRedoOnly(org.simpledbm.rss.tm.Transaction, int, int)
         */
        public final void updateAndLogRedoOnly(Transaction trx, int pageNumber,
                int value) {
            doUpdateAndLogRedoOnly(trx, pageNumber, value);
        }

        final void doUpdateAndLogRedoOnly(Transaction trx, int pageNumber,
                int value) {
            if (bab == null) {
                log.error(SpaceCursorImpl.class.getName(), "doUpdateAndLogRedoOnly", mcat
                    .getMessage("EF0006"));
                throw new IllegalStateException(mcat.getMessage("EF0006"));
            }
            UpdateSpaceMapPage updateSpaceMapLog = new UpdateSpaceMapPage(
                MODULE_ID,
                FreeSpaceManagerImpl.TYPE_UPDATESPACEMAPPAGE);
            Page page = bab.getPage();
            updateSpaceMapLog.setPageNumber(pageNumber);
            updateSpaceMapLog.setSpaceValue(value);
            Lsn lsn = trx.logInsert(page, updateSpaceMapLog);
            spacemgr.redo(page, updateSpaceMapLog);
            bab.setDirty(lsn);
        }

        /* (non-Javadoc)
         * @see org.simpledbm.rss.sm.SpaceCursor#updateAndLogUndoably(org.simpledbm.rss.tm.Transaction, int, int)
         */
        public final void updateAndLogUndoably(Transaction trx, int pageNumber,
                int value) {
            doUpdateAndLogUndoably(trx, pageNumber, value);
        }

        final void doUpdateAndLogUndoably(Transaction trx, int pageNumber,
                int value) {
            if (bab == null) {
                log.error(SpaceCursorImpl.class.getName(), "doUpdateAndLogUndoably", mcat
                    .getMessage("EF0006"));
                throw new IllegalStateException(mcat.getMessage("EF0006"));
            }
            UndoableUpdateSpaceMapPage updateSpaceMapLog = new UndoableUpdateSpaceMapPage(
                MODULE_ID,
                FreeSpaceManagerImpl.TYPE_UNDOABLEUPDATESPACEMAPPAGE);
            SpaceMapPageImpl page = (SpaceMapPageImpl) bab.getPage();
            updateSpaceMapLog.pageNumber = pageNumber;
            updateSpaceMapLog.spaceValue = value;
            updateSpaceMapLog.oldSpaceValue = page.getSpaceBits(pageNumber);
            Lsn lsn = trx.logInsert(page, updateSpaceMapLog);
            spacemgr.redo(page, updateSpaceMapLog);
            bab.setDirty(lsn);
        }

		public int getContainerId() {
			return containerId;
		}
    }

    /**
     * Cache for FreeSpaceCursor objects, keyed by container Id.
     * @author Dibyendu Majumdar
     * @since 10 Aug 08
     */
    static class FreeSpaceCursorCache {
    	
    	FreeSpaceManagerImpl fsm;
    	
    	HashMap<Integer, List<FreeSpaceCursor>> cache = new HashMap<Integer, List<FreeSpaceCursor>>();
    	
    	FreeSpaceCursorCache(FreeSpaceManagerImpl fsm) {
    		this.fsm = fsm;
		}

    	/**
    	 * Returns a FreeSpaceCursor for specified container Id. If no cached instance exists,
    	 * a new cursor is allocated and returned.
    	 */
    	FreeSpaceCursor getFreeSpaceCursor(int containerId) {
    		synchronized (cache) {
    			List<FreeSpaceCursor> cursorList = cache.get(containerId);
    			if (cursorList == null) {
    				cursorList = new ArrayList<FreeSpaceCursor>();
    				cache.put(containerId, cursorList);
    			}
    			if (cursorList.size() > 0) {
    				return cursorList.remove(cursorList.size()-1);
    			}
    		}
    		return new SpaceCursorImpl(fsm, containerId);
    	}
    	
    	/**
    	 * Returns a FreeSpaceCursor to the cache.
    	 */
    	void returnFreeSpaceCursor(FreeSpaceCursor fsc) {
    		synchronized (cache) {
    			List<FreeSpaceCursor> cursorList = cache.get(fsc.getContainerId());
    			if (cursorList == null) {
    				cursorList = new ArrayList<FreeSpaceCursor>();
    				cache.put(fsc.getContainerId(), cursorList);
    			}
    			cursorList.add(fsc);
    		}
    	}
    	
    	/**
    	 * Removes all cached instances of FreeSpaceCursor for specified
    	 * containerId.
    	 */
    	void removeContainerId(int containerId) {
    		synchronized (cache) {
    			List<FreeSpaceCursor> cursorList = cache.remove(containerId);
    			if (cursorList != null) {
    				cursorList.clear();
    			}
    		}
    	}
    }
}
