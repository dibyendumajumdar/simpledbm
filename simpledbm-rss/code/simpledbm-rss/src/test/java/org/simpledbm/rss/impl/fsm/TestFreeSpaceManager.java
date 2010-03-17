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
 *    Linking this library statically or dynamically with other modules 
 *    is making a combined work based on this library. Thus, the terms and
 *    conditions of the GNU General Public License cover the whole
 *    combination.
 *    
 *    As a special exception, the copyright holders of this library give 
 *    you permission to link this library with independent modules to 
 *    produce an executable, regardless of the license terms of these 
 *    independent modules, and to copy and distribute the resulting 
 *    executable under terms of your choice, provided that you also meet, 
 *    for each linked independent module, the terms and conditions of the 
 *    license of that module.  An independent module is a module which 
 *    is not derived from or based on this library.  If you modify this 
 *    library, you may extend this exception to your version of the 
 *    library, but you are not obligated to do so.  If you do not wish 
 *    to do so, delete this exception statement from your version.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.rss.impl.fsm;

import java.util.Properties;

import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.registry.ObjectRegistry;
import org.simpledbm.common.api.tx.IsolationMode;
import org.simpledbm.common.impl.platform.PlatformImpl;
import org.simpledbm.common.impl.registry.ObjectRegistryImpl;
import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.rss.api.fsm.FreeSpaceChecker;
import org.simpledbm.rss.api.fsm.FreeSpaceManagerException;
import org.simpledbm.rss.api.fsm.FreeSpaceScan;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.locking.LockManager;
import org.simpledbm.rss.api.locking.LockMgrFactory;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.api.tx.LoggableFactory;
import org.simpledbm.rss.api.tx.Savepoint;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.api.tx.TransactionManager;
import org.simpledbm.rss.api.tx.TransactionalModuleRegistry;
import org.simpledbm.rss.api.wal.LogManager;
import org.simpledbm.rss.impl.bm.BufferManagerImpl;
import org.simpledbm.rss.impl.fsm.FreeSpaceManagerImpl.SpaceCursorImpl;
import org.simpledbm.rss.impl.latch.LatchFactoryImpl;
import org.simpledbm.rss.impl.locking.LockManagerFactoryImpl;
import org.simpledbm.rss.impl.pm.PageManagerImpl;
import org.simpledbm.rss.impl.st.FileStorageContainerFactory;
import org.simpledbm.rss.impl.st.StorageManagerImpl;
import org.simpledbm.rss.impl.tx.LoggableFactoryImpl;
import org.simpledbm.rss.impl.tx.TransactionManagerImpl;
import org.simpledbm.rss.impl.tx.TransactionalModuleRegistryImpl;
import org.simpledbm.rss.impl.wal.LogFactoryImpl;

public class TestFreeSpaceManager extends BaseTestCase {

    public TestFreeSpaceManager(String arg0) {
        super(arg0);
    }

    public void testOneBitSpaceMapPage() throws Exception {

        MyDB db = new MyDB(true);

        try {
            FreeSpaceManagerImpl.OneBitSpaceMapPage onebitsmp = (FreeSpaceManagerImpl.OneBitSpaceMapPage) db.pageFactory
                    .getInstance(FreeSpaceManagerImpl.TYPE_ONEBITSPACEMAPPAGE,
                            new PageId(1, 0));
            onebitsmp.setFirstPageNumber(0);
            assertEquals(onebitsmp.getCount(), 65328);
            for (int i = 0; i < onebitsmp.getCount(); i++) {
                assertTrue(onebitsmp.getSpaceBits(i) == 0);
            }
            onebitsmp.setSpaceBits(0, 1);
            onebitsmp.setSpaceBits(onebitsmp.getLastPageNumber(), 1);
            try {
                onebitsmp.setSpaceBits(onebitsmp.getCount(), 1);
                fail();
            } catch (FreeSpaceManagerException e) {
            }
            assertTrue(onebitsmp.getSpaceBits(0) == 1);
            assertTrue(onebitsmp.getSpaceBits(onebitsmp.getLastPageNumber()) == 1);
            for (int i = 1; i < onebitsmp.getLastPageNumber(); i++) {
                assertTrue(onebitsmp.getSpaceBits(i) == 0);
            }

            FreeSpaceManagerImpl.TwoBitSpaceMapPage twobitsmp = (FreeSpaceManagerImpl.TwoBitSpaceMapPage) db.pageFactory
                    .getInstance(FreeSpaceManagerImpl.TYPE_TWOBITSPACEMAPPAGE,
                            new PageId(1, 0));
            twobitsmp.setFirstPageNumber(0);
            for (int i = 0; i < twobitsmp.getCount(); i++) {
                assertTrue(twobitsmp.getSpaceBits(i) == 0);
            }
            twobitsmp.setSpaceBits(0, 2);
            twobitsmp.setSpaceBits(twobitsmp.getLastPageNumber(), 3);
            try {
                twobitsmp.setSpaceBits(twobitsmp.getCount(), 1);
                fail();
            } catch (FreeSpaceManagerException e) {
            }
            assertTrue(twobitsmp.getSpaceBits(0) == 2);
            assertTrue(twobitsmp.getSpaceBits(twobitsmp.getLastPageNumber()) == 3);
            for (int i = 1; i < twobitsmp.getLastPageNumber(); i++) {
                assertTrue(twobitsmp.getSpaceBits(i) == 0);
            }

            assertEquals(twobitsmp.getCount(), 32664);
        } finally {
            db.shutdown();
        }
    }

    public void doCreateContainer(boolean commit) throws Exception {

        FreeSpaceManagerImpl.SpaceMapPageImpl.TESTING = true;

        MyDB db = new MyDB(false);

        if (!commit) {
            db.spacemgr.setTesting(1);
        }
        try {
            Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
            boolean okay = false;
            try {
                db.spacemgr.createContainer(trx, "testctr.dat", 1, 1, 8,
                        db.pageFactory.getRawPageType());
                assertTrue(db.storageManager.getInstance(1) != null);
                okay = true;
            } finally {
                if (okay && commit) {
                    trx.commit();
                } else {
                    trx.abort();
                    assertTrue(db.storageManager.getInstance(1) == null);
                }
            }
        } finally {
            db.shutdown();
        }
    }

    public void testCreateContainerAbort() throws Exception {
        try {
            doCreateContainer(false);
        } catch (FreeSpaceManagerException.TestException e) {
            // Expected
        }
    }

    public void testCreateContainerCommit() throws Exception {
        doCreateContainer(true);
    }

    void doTestOpenAndDropContainer(boolean commit) throws Exception {

        FreeSpaceManagerImpl.SpaceMapPageImpl.TESTING = true;

        MyDB db = new MyDB(false);

        try {
            assertTrue(db.storageManager.getInstance(0) != null);
            assertTrue(db.storageManager.getInstance(1) != null);

            Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
            db.spacemgr.dropContainer(trx, 1);
            if (commit) {
                trx.commit();
                assertTrue(db.storageManager.getInstance(1) == null);
            } else {
                trx.abort();
                assertTrue(db.storageManager.getInstance(1) != null);
            }
        } finally {
            db.shutdown();
        }
    }

    void doTestOpenAndDropContainerRollback() throws Exception {

        FreeSpaceManagerImpl.SpaceMapPageImpl.TESTING = true;

        MyDB db = new MyDB(false);

        try {
            assertTrue(db.storageManager.getInstance(0) != null);
            assertTrue(db.storageManager.getInstance(1) != null);

            Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
            Savepoint sp = trx.createSavepoint(false);
            db.spacemgr.dropContainer(trx, 1);
            trx.rollback(sp);
            trx.commit();
            assertTrue(db.storageManager.getInstance(1) != null);
        } finally {
            db.shutdown();
        }
    }

    public void testDropContainerCommit() throws Exception {
        doTestOpenAndDropContainer(true);
    }

    public void testCreateContainerCommit2() throws Exception {
        doCreateContainer(true);
    }

    public void testDropContainerAbort() throws Exception {
        doTestOpenAndDropContainer(false);
    }

    public void testDropContainerRollback() throws Exception {
        doTestOpenAndDropContainerRollback();
    }

    public void testExtendContainer() throws Exception {

        FreeSpaceManagerImpl.SpaceMapPageImpl.TESTING = true;

        MyDB db = new MyDB(false);

        try {
            assertTrue(db.storageManager.getInstance(0) != null);
            assertTrue(db.storageManager.getInstance(1) != null);

            Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
            db.spacemgr.extendContainer(trx, 1);
            trx.abort();
        } finally {
            db.shutdown();
        }
    }

    public void testSpaceCursor() throws Exception {

        FreeSpaceManagerImpl.SpaceMapPageImpl.TESTING = true;

        MyDB db = new MyDB(false);

        try {
            assertTrue(db.storageManager.getInstance(0) != null);
            assertTrue(db.storageManager.getInstance(1) != null);

            int expectedPages[] = { 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15,
                    -1 };

            SpaceCursorImpl spaceCursor = new SpaceCursorImpl(db.spacemgr, 1);
            for (int i = 0; i < (expectedPages.length + 5); i++) {
                int pageNumber = spaceCursor
                        .findAndFixSpaceMapPageExclusively(new FreeSpaceChecker() {
                            public boolean hasSpace(int value) {
                                return value == 0;
                            }
                        });
                assertTrue(i < expectedPages.length);
                assertEquals(expectedPages[i], pageNumber);
                if (pageNumber == -1) {
                    break;
                }
                Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
                spaceCursor.updateAndLogRedoOnly(trx, pageNumber, 1);
                spaceCursor.unfixCurrentSpaceMapPage();
                trx.commit();
            }

            expectedPages = new int[] { 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
                    14, 15, -1 };

            FreeSpaceScan spaceScan = db.spacemgr.openScan(1);
            try {
                int i = 0;
                while (spaceScan.fetchNext()) {
                    int pageNumber = spaceScan.getCurrentPage();
                    assertTrue(i < expectedPages.length);
                    assertEquals(expectedPages[i], pageNumber);
                    i++;
                }
                assertTrue(spaceScan.isEof());
            } finally {
                spaceScan.close();
            }

            Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
            db.spacemgr.dropContainer(trx, 1);
            trx.commit();
            assertTrue(db.storageManager.getInstance(1) == null);
        } finally {
            db.shutdown();
        }
    }

    public void testReopenContainer() throws Exception {

        FreeSpaceManagerImpl.SpaceMapPageImpl.TESTING = true;

        MyDB db = new MyDB(false);

        try {
            assertTrue(db.storageManager.getInstance(0) != null);
            assertTrue(db.storageManager.getInstance(1) == null);
        } finally {
            db.shutdown();
        }
    }

    class MyDB {
        /* Create the write ahead log */
        final Platform platform;
        final LogFactoryImpl logFactory;
        final ObjectRegistry objectFactory;
        final StorageContainerFactory storageFactory;
        final StorageManager storageManager;
        final LatchFactory latchFactory;
        final PageManager pageFactory;
        final LockMgrFactory lockmgrFactory;
        final LockManager lockmgr;
        final LogManager logmgr;
        final BufferManagerImpl bufmgr;
        final LoggableFactory loggableFactory;
        final TransactionalModuleRegistry moduleRegistry;
        final TransactionManager trxmgr;
        final FreeSpaceManagerImpl spacemgr;

        MyDB(boolean create) throws Exception {

            Properties properties = new Properties();
            properties.setProperty("log.ctl.1", "ctl.a");
            properties.setProperty("log.ctl.2", "ctl.b");
            properties.setProperty("log.groups.1.path", ".");
            properties.setProperty("log.archive.path", ".");
            properties.setProperty("log.group.files", "3");
            properties.setProperty("log.file.size", "16384");
            properties.setProperty("log.buffer.size", "16384");
            properties.setProperty("log.buffer.limit", "4");
            properties.setProperty("log.flush.interval", "5");
            properties.setProperty("logging.properties.file",
                    "classpath:simpledbm.logging.properties");
            properties.setProperty("logging.properties.type", "log4j");
            properties.setProperty("storage.basePath",
                    "testdata/TestFreeSpaceManager");

            platform = new PlatformImpl(properties);

            /* Create the write ahead log */
            logFactory = new LogFactoryImpl(platform, properties);
            storageFactory = new FileStorageContainerFactory(platform,
                    properties);
            if (create) {
                logFactory.createLog(storageFactory, properties);
            }

            objectFactory = new ObjectRegistryImpl(platform, properties);
            storageManager = new StorageManagerImpl(platform, properties);
            latchFactory = new LatchFactoryImpl(platform, properties);
            pageFactory = new PageManagerImpl(platform, objectFactory,
                    storageManager, latchFactory, properties);
            lockmgrFactory = new LockManagerFactoryImpl(platform, properties);
            lockmgr = lockmgrFactory.create(latchFactory, properties);
            logmgr = logFactory.getLog(storageFactory, properties);
            bufmgr = new BufferManagerImpl(platform, logmgr, pageFactory, 3, 11);
            loggableFactory = new LoggableFactoryImpl(platform, objectFactory,
                    properties);
            moduleRegistry = new TransactionalModuleRegistryImpl(platform,
                    properties);
            trxmgr = new TransactionManagerImpl(platform, logmgr,
                    storageFactory, storageManager, bufmgr, lockmgr,
                    loggableFactory, latchFactory, objectFactory,
                    moduleRegistry, properties);
            spacemgr = new FreeSpaceManagerImpl(platform, objectFactory,
                    pageFactory, logmgr, bufmgr, storageManager,
                    storageFactory, loggableFactory, trxmgr, moduleRegistry,
                    properties);

            bufmgr.setStorageManager(storageManager);

            logmgr.start();
            bufmgr.start();

            if (create) {
                StorageContainer sc = storageFactory.create("dual");
                storageManager.register(0, sc);
                Page page = pageFactory.getInstance(pageFactory
                        .getRawPageType(), new PageId(0, 0));
                pageFactory.store(page);
            }

            trxmgr.start();
        }

        void shutdown() {
            trxmgr.shutdown();
            bufmgr.shutdown();
            logmgr.shutdown();
            storageManager.shutdown();
        }

    }
}
