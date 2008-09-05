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
package org.simpledbm.rss.impl.tx;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.rss.api.bm.BufferAccessBlock;
import org.simpledbm.rss.api.bm.BufferManager;
import org.simpledbm.rss.api.bm.BufferManagerException;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockManager;
import org.simpledbm.rss.api.locking.LockMgrFactory;
import org.simpledbm.rss.api.locking.LockMode;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageException;
import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.registry.ObjectFactory;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageException;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.api.tx.BaseLockable;
import org.simpledbm.rss.api.tx.BaseLoggable;
import org.simpledbm.rss.api.tx.BaseTransactionalModule;
import org.simpledbm.rss.api.tx.Compensation;
import org.simpledbm.rss.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.Loggable;
import org.simpledbm.rss.api.tx.LoggableFactory;
import org.simpledbm.rss.api.tx.NonTransactionRelatedOperation;
import org.simpledbm.rss.api.tx.PageFormatOperation;
import org.simpledbm.rss.api.tx.Redoable;
import org.simpledbm.rss.api.tx.Savepoint;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.api.tx.TransactionException;
import org.simpledbm.rss.api.tx.TransactionManager;
import org.simpledbm.rss.api.tx.TransactionalModuleRegistry;
import org.simpledbm.rss.api.tx.Undoable;
import org.simpledbm.rss.api.wal.LogManager;
import org.simpledbm.rss.api.wal.Lsn;
import org.simpledbm.rss.impl.bm.BufferManagerImpl;
import org.simpledbm.rss.impl.latch.LatchFactoryImpl;
import org.simpledbm.rss.impl.locking.LockManagerFactoryImpl;
import org.simpledbm.rss.impl.pm.PageManagerImpl;
import org.simpledbm.rss.impl.registry.ObjectRegistryImpl;
import org.simpledbm.rss.impl.st.FileStorageContainerFactory;
import org.simpledbm.rss.impl.st.StorageManagerImpl;
import org.simpledbm.rss.impl.wal.LogFactoryImpl;
import org.simpledbm.rss.util.ByteString;

public class TestTransactionManager2 extends BaseTestCase {

    static final short TYPE_BITMGRPAGE = 25000;
    static final short TYPE_BITMGRLOGCREATECONTAINER = 25001;
    static final short TYPE_BITMGRLOGFORMATPAGE = 25002;
    static final short TYPE_BITMGRLOGREDOUNDO = 25003;
    static final short TYPE_BITMGRLOGCLR = 25004;
    static final short TYPE_BITMGRLOGOPENCONTAINER = 25005;

    public TestTransactionManager2(String arg0) {
        super(arg0);
    }

    void setupObjectFactory(ObjectRegistry objectFactory, PageManager pageFactory) {
        objectFactory.registerSingleton(
        	TYPE_BITMGRPAGE, new BitMgrPage.BitMgrPageFactory(pageFactory));
        objectFactory.registerObjectFactory(
            TYPE_BITMGRLOGCREATECONTAINER,
            new BitMgrLogCreateContainer.BitMgrLogCreateContainerFactory());
        objectFactory.registerObjectFactory(
            TYPE_BITMGRLOGFORMATPAGE,
            new BitMgrLogFormatPage.BitMgrLogFormatPageFactory());
        objectFactory.registerObjectFactory(
            TYPE_BITMGRLOGREDOUNDO,
            new BitMgrLogRedoUndo.BitMgrLogRedoUndoFactory());
        objectFactory.registerObjectFactory(
        	TYPE_BITMGRLOGCLR, 
        	new BitMgrLogCLR.BitMgrLogCLRFactory());
        objectFactory.registerObjectFactory(
            TYPE_BITMGRLOGOPENCONTAINER,
            new BitMgrLogOpenContainer.BitMgrLogOpenContainerFactory());
    }

    public void testTrxMgrStart() throws Exception {

        /* Create the write ahead log */
        Properties properties = new Properties();
        properties.setProperty(
            "storage.basePath",
            "testdata/TestTransactionManager2");
        final LogFactoryImpl logFactory = new LogFactoryImpl();
        logFactory.createLog(properties);

        final ObjectRegistry objectFactory = new ObjectRegistryImpl(properties);
        final StorageContainerFactory storageFactory = new FileStorageContainerFactory(
            properties);
        final StorageManager storageManager = new StorageManagerImpl(properties);
        final LatchFactory latchFactory = new LatchFactoryImpl(properties);
        final PageManager pageFactory = new PageManagerImpl(
            objectFactory,
            storageManager,
            latchFactory,
            properties);
        setupObjectFactory(objectFactory, pageFactory);
        final LockMgrFactory lockmgrFactory = new LockManagerFactoryImpl();
        final LockManager lockmgr = lockmgrFactory.create(latchFactory, properties);
        final LogManager logmgr = logFactory.getLog(properties);
        logmgr.start();
        final BufferManager bufmgr = new BufferManagerImpl(
            logmgr,
            pageFactory,
            3,
            11);
        bufmgr.start();
        final LoggableFactory loggableFactory = new LoggableFactoryImpl(
            objectFactory, properties);
        final TransactionalModuleRegistry moduleRegistry = new TransactionalModuleRegistryImpl(properties);
        final TransactionManagerImpl trxmgr = new TransactionManagerImpl(
            logmgr,
            storageFactory,
            storageManager,
            bufmgr,
            lockmgr,
            loggableFactory,
            latchFactory,
            objectFactory,
            moduleRegistry,
            properties);
        OneBitMgr bitmgr = new OneBitMgr(
            storageFactory,
            storageManager,
            pageFactory,
            bufmgr,
            loggableFactory,
            trxmgr,
            objectFactory);
        moduleRegistry.registerModule(OneBitMgr.moduleId, bitmgr);

        /* Now we are ready to start */
        try {
            trxmgr.start();
        } finally {
            trxmgr.shutdown();
            bufmgr.shutdown();
            logmgr.shutdown();
            storageManager.shutdown();
        }
    }

    public void testBitMgrCreateContainer() throws Exception {

        Properties properties = new Properties();
        properties.setProperty(
            "storage.basePath",
            "testdata/TestTransactionManager2");
        final LogFactoryImpl logFactory = new LogFactoryImpl();
        final ObjectRegistry objectFactory = new ObjectRegistryImpl(properties);
        final StorageContainerFactory storageFactory = new FileStorageContainerFactory(
            properties);
        final StorageManager storageManager = new StorageManagerImpl(properties);
        final LatchFactory latchFactory = new LatchFactoryImpl(properties);
        final PageManager pageFactory = new PageManagerImpl(
            objectFactory,
            storageManager,
            latchFactory,
            properties);
        setupObjectFactory(objectFactory, pageFactory);
        final LockMgrFactory lockmgrFactory = new LockManagerFactoryImpl();
        final LockManager lockmgr = lockmgrFactory.create(latchFactory, properties);
        final LogManager logmgr = logFactory.getLog(properties);
        logmgr.start();
        final BufferManager bufmgr = new BufferManagerImpl(
            logmgr,
            pageFactory,
            3,
            11);
        bufmgr.start();
        final LoggableFactory loggableFactory = new LoggableFactoryImpl(
            objectFactory, properties);
        final TransactionalModuleRegistry moduleRegistry = new TransactionalModuleRegistryImpl(properties);
        final TransactionManagerImpl trxmgr = new TransactionManagerImpl(
            logmgr,
            storageFactory,
            storageManager,
            bufmgr,
            lockmgr,
            loggableFactory,
            latchFactory,
            objectFactory,
            moduleRegistry,
            properties);
        OneBitMgr bitmgr = new OneBitMgr(
            storageFactory,
            storageManager,
            pageFactory,
            bufmgr,
            loggableFactory,
            trxmgr,
            objectFactory);
        moduleRegistry.registerModule(OneBitMgr.moduleId, bitmgr);

        /* Now we are ready to start */
        try {
            StorageContainer sc = storageFactory.create("dual");
            storageManager.register(999, sc);
            Page page = pageFactory.getInstance(
                pageFactory.getRawPageType(),
                new PageId(999, 0));
            pageFactory.store(page);
            trxmgr.start();
            bitmgr.create("bit.dat", 1, 0);
        } finally {
            trxmgr.shutdown();
            bufmgr.shutdown();
            logmgr.shutdown();
            storageManager.shutdown();
        }

    }

    void printBits(Transaction trx, OneBitMgr bitmgr, int start, int n,
            int[] values) throws Exception {
        Savepoint sp = trx.createSavepoint(false);
        try {
            for (int i = start; i < start + n; i++) {
                int bit = bitmgr.getBit(trx, i);
                System.out.println("Bit[" + i + "] = " + bit);
                if (bit != values[i]) {
                    throw new Exception("Bit[" + i + "] = " + bit
                            + ", expected value=" + values[i]);
                }
            }
        } finally {
            trx.rollback(sp);
        }
    }

    public void testBitMgrSingleThread() throws Exception {

        Properties properties = new Properties();
        properties.setProperty(
            "storage.basePath",
            "testdata/TestTransactionManager2");
        final LogFactoryImpl logFactory = new LogFactoryImpl();
        final ObjectRegistry objectFactory = new ObjectRegistryImpl(properties);
        final StorageContainerFactory storageFactory = new FileStorageContainerFactory(
            properties);
        final StorageManager storageManager = new StorageManagerImpl(properties);
        final LatchFactory latchFactory = new LatchFactoryImpl(properties);
        final PageManager pageFactory = new PageManagerImpl(
            objectFactory,
            storageManager,
            latchFactory,
            properties);
        setupObjectFactory(objectFactory, pageFactory);
        final LockMgrFactory lockmgrFactory = new LockManagerFactoryImpl();
        final LockManager lockmgr = lockmgrFactory.create(latchFactory, properties);
        final LogManager logmgr = logFactory.getLog(properties);
        logmgr.start();
        final BufferManager bufmgr = new BufferManagerImpl(
            logmgr,
            pageFactory,
            3,
            11);
        bufmgr.start();
        final LoggableFactory loggableFactory = new LoggableFactoryImpl(
            objectFactory, properties);
        final TransactionalModuleRegistry moduleRegistry = new TransactionalModuleRegistryImpl(properties);
        final TransactionManagerImpl trxmgr = new TransactionManagerImpl(
            logmgr,
            storageFactory,
            storageManager,
            bufmgr,
            lockmgr,
            loggableFactory,
            latchFactory,
            objectFactory,
            moduleRegistry,
            properties);
        OneBitMgr bitmgr = new OneBitMgr(
            storageFactory,
            storageManager,
            pageFactory,
            bufmgr,
            loggableFactory,
            trxmgr,
            objectFactory);
        moduleRegistry.registerModule(OneBitMgr.moduleId, bitmgr);
        StorageContainer sc = storageFactory.open("dual");
        storageManager.register(999, sc);

        /* Now we are ready to start */
        try {
            boolean success = false;
            trxmgr.start();
            Transaction trx = trxmgr.begin(IsolationMode.SERIALIZABLE);
            try {
                System.out.println("After restart 0-7");
                printBits(trx, bitmgr, 0, 7, new int[] { 0, 0, 0, 0, 0, 0, 0 });
                bitmgr.changeBit(trx, 0, 10);
                bitmgr.changeBit(trx, 1, 15);
                bitmgr.changeBit(trx, 2, 20);
                bitmgr.changeBit(trx, 3, 25);
                bitmgr.changeBit(trx, 4, 30);
                bitmgr.changeBit(trx, 5, 35);
                bitmgr.changeBit(trx, 6, 40);
                success = true;
            } finally {
                if (success)
                    trx.commit();
                else
                    trx.abort();
            }
            trxmgr.checkpoint();

            trx = trxmgr.begin(IsolationMode.SERIALIZABLE);
            try {
                System.out.println("After commit 0-7");
                printBits(trx, bitmgr, 0, 7, new int[] { 10, 15, 20, 25, 30,
                        35, 40 });
                bitmgr.changeBit(trx, 0, 11);
                bitmgr.changeBit(trx, 1, 16);
                bitmgr.changeBit(trx, 2, 21);
                bitmgr.changeBit(trx, 3, 26);
                bitmgr.changeBit(trx, 4, 31);
                bitmgr.changeBit(trx, 5, 36);
                bitmgr.changeBit(trx, 6, 41);
                success = false;
            } finally {
                if (success)
                    trx.commit();
                else
                    trx.abort();
            }

            trx = trxmgr.begin(IsolationMode.SERIALIZABLE);
            try {
                System.out.println("After rollback 0-7");
                printBits(trx, bitmgr, 0, 7, new int[] { 10, 15, 20, 25, 30,
                        35, 40 });
                bitmgr.changeBit(trx, 3, 90);
                bitmgr.changeBit(trx, 4, 99);
                success = true;
            } finally {
                if (success)
                    trx.commit();
                else
                    trx.abort();
            }

            trx = trxmgr.begin(IsolationMode.SERIALIZABLE);
            try {
                System.out.println("After commit 3-4");
                printBits(trx, bitmgr, 0, 7, new int[] { 10, 15, 20, 90, 99,
                        35, 40 });
                bitmgr.changeBit(trx, 0, 11);
                bitmgr.changeBit(trx, 1, 16);
                bitmgr.changeBit(trx, 2, 21);
                Savepoint sp = trx.createSavepoint(false);
                bitmgr.changeBit(trx, 3, 26);
                bitmgr.changeBit(trx, 4, 31);
                trx.rollback(sp);
                bitmgr.changeBit(trx, 5, 36);
                bitmgr.changeBit(trx, 6, 41);
                System.out
                    .println("After commit 0-2, rollback sp(3-4), change 5-6");
                printBits(trx, bitmgr, 0, 7, new int[] { 11, 16, 21, 90, 99,
                        36, 41 });
                success = true;
            } catch (Exception e) {
                trx.abort();
            }
            // 
        } finally {
            trxmgr.shutdown();
            bufmgr.shutdown();
            logmgr.shutdown();
            storageManager.shutdown();
        }

    }

    public void testBitMgrSingleThreadRestart() throws Exception {

        Properties properties = new Properties();
        properties.setProperty(
            "storage.basePath",
            "testdata/TestTransactionManager2");
        final LogFactoryImpl logFactory = new LogFactoryImpl();
        final ObjectRegistry objectFactory = new ObjectRegistryImpl(properties);
        final StorageContainerFactory storageFactory = new FileStorageContainerFactory(
            properties);
        final StorageManager storageManager = new StorageManagerImpl(properties);
        final LatchFactory latchFactory = new LatchFactoryImpl(properties);
        final PageManager pageFactory = new PageManagerImpl(
            objectFactory,
            storageManager,
            latchFactory,
            properties);
        setupObjectFactory(objectFactory, pageFactory);
        final LockMgrFactory lockmgrFactory = new LockManagerFactoryImpl();
        final LockManager lockmgr = lockmgrFactory.create(latchFactory, properties);
        final LogManager logmgr = logFactory.getLog(properties);
        logmgr.start();
        final BufferManager bufmgr = new BufferManagerImpl(
            logmgr,
            pageFactory,
            3,
            11);
        bufmgr.start();
        final LoggableFactory loggableFactory = new LoggableFactoryImpl(
            objectFactory, properties);
        final TransactionalModuleRegistry moduleRegistry = new TransactionalModuleRegistryImpl(properties);
        final TransactionManagerImpl trxmgr = new TransactionManagerImpl(
            logmgr,
            storageFactory,
            storageManager,
            bufmgr,
            lockmgr,
            loggableFactory,
            latchFactory,
            objectFactory,
            moduleRegistry,
            properties);
        OneBitMgr bitmgr = new OneBitMgr(
            storageFactory,
            storageManager,
            pageFactory,
            bufmgr,
            loggableFactory,
            trxmgr,
            objectFactory);
        moduleRegistry.registerModule(OneBitMgr.moduleId, bitmgr);
        StorageContainer sc = storageFactory.open("dual");
        storageManager.register(999, sc);
        // FIXME 
        sc = storageFactory.open("bit.dat");
        storageManager.register(1, sc);

        /* Now we are ready to start */
        try {
            boolean success = false;
            trxmgr.start();
            Transaction trx = trxmgr.begin(IsolationMode.SERIALIZABLE);
            try {
                System.out.println("After restart 0-7");
                printBits(trx, bitmgr, 0, 7, new int[] { 10, 15, 20, 90, 99,
                        35, 40 });
                success = true;
            } finally {
                if (success)
                    trx.commit();
                else
                    trx.abort();
            }
            trxmgr.checkpoint();
        } finally {
            trxmgr.shutdown();
            bufmgr.shutdown();
            logmgr.shutdown();
            storageManager.shutdown();
        }
    }

    boolean testfailed = false;

    public void testTrxLocking() throws Exception {

        Properties properties = new Properties();
        properties.setProperty(
            "storage.basePath",
            "testdata/TestTransactionManager1");
        final LogFactoryImpl logFactory = new LogFactoryImpl();
        final ObjectRegistry objectFactory = new ObjectRegistryImpl(properties);
        final StorageContainerFactory storageFactory = new FileStorageContainerFactory(
            properties);
        final StorageManager storageManager = new StorageManagerImpl(properties);
        final LatchFactory latchFactory = new LatchFactoryImpl(properties);
        final PageManager pageFactory = new PageManagerImpl(
            objectFactory,
            storageManager,
            latchFactory,
            properties);
        setupObjectFactory(objectFactory, pageFactory);
        final LockMgrFactory lockmgrFactory = new LockManagerFactoryImpl();
        final LockManager lockmgr = lockmgrFactory.create(latchFactory, properties);
        final LogManager logmgr = logFactory.getLog(properties);
        logmgr.start();
        final BufferManager bufmgr = new BufferManagerImpl(
            logmgr,
            pageFactory,
            3,
            11);
        bufmgr.start();
        final LoggableFactory loggableFactory = new LoggableFactoryImpl(
            objectFactory, properties);
        final TransactionalModuleRegistry moduleRegistry = new TransactionalModuleRegistryImpl(properties);
        final TransactionManagerImpl trxmgr = new TransactionManagerImpl(
            logmgr,
            storageFactory,
            storageManager,
            bufmgr,
            lockmgr,
            loggableFactory,
            latchFactory,
            objectFactory,
            moduleRegistry,
            properties);
        OneBitMgr bitmgr = new OneBitMgr(
            storageFactory,
            storageManager,
            pageFactory,
            bufmgr,
            loggableFactory,
            trxmgr,
            objectFactory);
        moduleRegistry.registerModule(OneBitMgr.moduleId, bitmgr);
        StorageContainer sc = storageFactory.open("dual");
        storageManager.register(999, sc);
        // FIXME 
        sc = storageFactory.open("bit.dat");
        storageManager.register(1, sc);

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                Transaction trx = trxmgr.begin(IsolationMode.SERIALIZABLE);
                try {
                    ObjectLock ol = new ObjectLock(1, 15);
                    trx.acquireLock(
                        ol,
                        LockMode.SHARED,
                        LockDuration.MANUAL_DURATION);
                    System.err.println("T2: Acquired lock");
                    assertTrue(trx.hasLock(ol) == LockMode.SHARED);
                    assertTrue(trx.releaseLock(ol));
                    System.err.println("T2: Lock released");
                    trx.commit();
                } catch (Exception e) {
                    e.printStackTrace();
                    testfailed = true;
                }
            }
        });

        /* Now we are ready to start */
        try {
            trxmgr.start();
            Transaction trx = trxmgr.begin(IsolationMode.SERIALIZABLE);
            try {
                testfailed = false;
                ObjectLock ol = new ObjectLock(1, 15);
                trx.acquireLock(
                    ol,
                    LockMode.SHARED,
                    LockDuration.MANUAL_DURATION);
                assertTrue(trx.hasLock(ol) == LockMode.SHARED);
                System.err.println("T1: Upgrading lock");
                trx.acquireLock(
                    ol,
                    LockMode.UPDATE,
                    LockDuration.MANUAL_DURATION);
                assertTrue(trx.hasLock(ol) == LockMode.UPDATE);
                t1.start();
                Thread.sleep(1000);
                System.err.println("T1: Downgrading lock");
                trx.downgradeLock(ol, LockMode.SHARED);
                assertTrue(trx.hasLock(ol) == LockMode.SHARED);
                Thread.sleep(1000);
                assertFalse(trx.releaseLock(ol));
                assertTrue(trx.releaseLock(new ObjectLock(1, 15)));
                t1.join(1000);
                assertTrue(!t1.isAlive());
                assertFalse(testfailed);
            } finally {
                trx.commit();
            }

            trx = trxmgr.begin(IsolationMode.SERIALIZABLE);
            ObjectLock lock1 = new ObjectLock(1, 15);
            ObjectLock lock2 = new ObjectLock(2, 16);
            try {
                trx.acquireLock(
                    lock1,
                    LockMode.SHARED,
                    LockDuration.COMMIT_DURATION);
                Savepoint savepoint = trx.createSavepoint(false);
                trx.acquireLock(
                    lock2,
                    LockMode.EXCLUSIVE,
                    LockDuration.COMMIT_DURATION);
                assertEquals(trx.hasLock(lock1), LockMode.SHARED);
                assertEquals(trx.hasLock(lock2), LockMode.EXCLUSIVE);
                trx.acquireLock(
                    lock1,
                    LockMode.UPDATE,
                    LockDuration.COMMIT_DURATION);
                assertEquals(trx.hasLock(lock1), LockMode.UPDATE);
                trx.rollback(savepoint);
                // System.err.println("Rolled back to savepoint");
                assertEquals(LockMode.UPDATE, trx.hasLock(lock1));
                assertEquals(LockMode.NONE, trx.hasLock(lock2));

                savepoint = trx.createSavepoint(false);
                trx.acquireLock(
                    lock1,
                    LockMode.EXCLUSIVE,
                    LockDuration.COMMIT_DURATION);
                trx.acquireLock(
                    lock2,
                    LockMode.EXCLUSIVE,
                    LockDuration.COMMIT_DURATION);
                assertEquals(trx.hasLock(lock1), LockMode.EXCLUSIVE);
                assertEquals(trx.hasLock(lock2), LockMode.EXCLUSIVE);
                trx.rollback(savepoint);
                // System.err.println("Rolled back to savepoint");
                assertEquals(LockMode.EXCLUSIVE, trx.hasLock(lock1));
                assertEquals(LockMode.NONE, trx.hasLock(lock2));
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            } finally {
                trx.commit();
                // System.err.println("After commit");
                assertEquals(LockMode.NONE, trx.hasLock(lock1));
                assertEquals(LockMode.NONE, trx.hasLock(lock2));
            }
        } finally {
            trxmgr.shutdown();
            bufmgr.shutdown();
            logmgr.shutdown();
            storageManager.shutdown();
        }
    }

    public static class ObjectLock extends BaseLockable {

        static final int CONTAINER = 1;
        static final int BIT = 2;

        final int value;

        public ObjectLock(int type, int value) {
            super((byte) type);
            this.value = value;
        }

        @Override
        public int hashCode() {
            final int PRIME = 31;
            int result = super.hashCode();
            result = PRIME * result + value;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (!super.equals(obj))
                return false;
            if (getClass() != obj.getClass())
                return false;
            final ObjectLock other = (ObjectLock) obj;
            if (value != other.value)
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "ObjectLock(" + super.toString() + ", value = " + value
                    + ")";
        }

        public int getContainerId() {
            if (getNameSpace() == CONTAINER) {
                return value;
            }
            return 0;
        }
    }

    public static class BitMgrContainerOperation extends BaseLoggable {

        int containerId;
        ByteString name;

        public BitMgrContainerOperation(int moduleId, int typeCode) {
			super(moduleId, typeCode);
            containerId = -1;
            name = new ByteString("");
		}

		public BitMgrContainerOperation(ByteBuffer bb) {
			super(bb);
            containerId = bb.getInt();
            name = new ByteString(bb);
		}

//		@Override
//        public void init() {
//            containerId = -1;
//            name = new ByteString("");
//        }

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
            return super.getStoredLength() + (Integer.SIZE / Byte.SIZE)
                    + name.getStoredLength();
        }

//        @Override
//        public void retrieve(ByteBuffer bb) {
//            super.retrieve(bb);
//            containerId = bb.getInt();
//            name = new ByteString();
//            name.retrieve(bb);
//        }

        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(containerId);
            name.store(bb);
        }
    }

    public static class BitMgrLogCreateContainer extends
            BitMgrContainerOperation implements Redoable {

		public BitMgrLogCreateContainer(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public BitMgrLogCreateContainer(ByteBuffer bb) {
			super(bb);
		}
		
		static final class BitMgrLogCreateContainerFactory implements ObjectFactory {
			public Class<?> getType() {
				return BitMgrLogCreateContainer.class;
			}

//			public Object newInstance() {
//				return new BitMgrLogCreateContainer();
//			}

			public Object newInstance(ByteBuffer buf) {
				return new BitMgrLogCreateContainer(buf);
			}
			
		}
    	
    }

    public static class BitMgrLogOpenContainer extends BitMgrContainerOperation
			implements NonTransactionRelatedOperation {

		public BitMgrLogOpenContainer(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public BitMgrLogOpenContainer(ByteBuffer bb) {
			super(bb);
		}

		static final class BitMgrLogOpenContainerFactory implements ObjectFactory {
			public Class<?> getType() {
				return BitMgrLogOpenContainer.class;
			}

//			public Object newInstance() {
//				return new BitMgrLogOpenContainer();
//			}

			public Object newInstance(ByteBuffer buf) {
				return new BitMgrLogOpenContainer(buf);
			}
		}
	}

    public static class BitMgrLogFormatPage extends BaseLoggable implements
            Redoable, PageFormatOperation {

		public BitMgrLogFormatPage(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public BitMgrLogFormatPage(ByteBuffer bb) {
			super(bb);
		}

//        @Override
//        public void init() {
//        }

		static final class BitMgrLogFormatPageFactory implements ObjectFactory {
			public Class<?> getType() {
				return BitMgrLogFormatPage.class;
			}

//			public Object newInstance() {
//				return new BitMgrLogFormatPage();
//			}

			public Object newInstance(ByteBuffer buf) {
				return new BitMgrLogFormatPage(buf);
			}
			
		}		
		
    }

    public static class BitMgrLogRedoUndo extends BaseLoggable implements
            Undoable {

        int index;
        int newValue;
        int oldValue;

        static final int SIZE = BaseLoggable.SIZE + (Integer.SIZE / Byte.SIZE)
                * 3;

        public BitMgrLogRedoUndo(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public BitMgrLogRedoUndo(ByteBuffer bb) {
			super(bb);
            index = bb.getInt();
            newValue = bb.getInt();
            oldValue = bb.getInt();
		}

//		@Override
//        public void init() {
//        }

        public final int getIndex() {
            return index;
        }

        public final void setIndex(int index) {
            this.index = index;
        }

        public final int getNewValue() {
            return newValue;
        }

        public final void setNewValue(int newValue) {
            this.newValue = newValue;
        }

        public final int getOldValue() {
            return oldValue;
        }

        public final void setOldValue(int oldValue) {
            this.oldValue = oldValue;
        }

        @Override
        public int getStoredLength() {
            return SIZE;
        }

//        @Override
//        public void retrieve(ByteBuffer bb) {
//            super.retrieve(bb);
//            index = bb.getInt();
//            newValue = bb.getInt();
//            oldValue = bb.getInt();
//        }

        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(index);
            bb.putInt(newValue);
            bb.putInt(oldValue);
        }

        static final class BitMgrLogRedoUndoFactory implements ObjectFactory {
			public Class<?> getType() {
				return BitMgrLogRedoUndo.class;
			}

//			public Object newInstance() {
//				return new BitMgrLogRedoUndo();
//			}

			public Object newInstance(ByteBuffer buf) {
				return new BitMgrLogRedoUndo(buf);
			}
		}		
    }

    public static class BitMgrLogCLR extends BaseLoggable implements
            Compensation {

        int index;
        int newValue;

        static final int SIZE = BaseLoggable.SIZE + (Integer.SIZE / Byte.SIZE)
                * 2;

        
//        @Override
//        public void init() {
//        }

        public BitMgrLogCLR(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public BitMgrLogCLR(ByteBuffer bb) {
			super(bb);
            index = bb.getInt();
            newValue = bb.getInt();
		}

		public final int getIndex() {
            return index;
        }

        public final void setIndex(int index) {
            this.index = index;
        }

        public final int getNewValue() {
            return newValue;
        }

        public final void setNewValue(int newValue) {
            this.newValue = newValue;
        }

        @Override
        public int getStoredLength() {
            return SIZE;
        }

//        @Override
//        public void retrieve(ByteBuffer bb) {
//            super.retrieve(bb);
//            index = bb.getInt();
//            newValue = bb.getInt();
//        }

        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(index);
            bb.putInt(newValue);
        }
        static final class BitMgrLogCLRFactory implements ObjectFactory {
			public Class<?> getType() {
				return BitMgrLogCLR.class;
			}

//			public Object newInstance() {
//				return new BitMgrLogCLR();
//			}

			public Object newInstance(ByteBuffer buf) {
				return new BitMgrLogCLR(buf);
			}
		}		
    }

    public static class BitMgrPage extends Page {

        byte[] bits;

        
        BitMgrPage(PageManager pageFactory, int type, PageId pageId) {
			super(pageFactory, type, pageId);
            int n_bits = super.getStoredLength() - Page.SIZE;
            bits = new byte[n_bits];
		}

		BitMgrPage(PageManager pageFactory, PageId pageId, ByteBuffer bb) {
			super(pageFactory, pageId, bb);
            int n_bits = super.getStoredLength() - Page.SIZE;
            bits = new byte[n_bits];
            bb.get(bits);
		}

//		public BitMgrPage(PageFactory pageFactory, ByteBuffer bb) {
//			super(pageFactory, bb);
//            int n_bits = super.getStoredLength() - Page.SIZE;
//            bits = new byte[n_bits];
//            bb.get(bits);
//		}
//
//		public BitMgrPage(PageFactory pageFactory) {
//			super(pageFactory);
//            int n_bits = super.getStoredLength() - Page.SIZE;
//            bits = new byte[n_bits];
//		}

//		@Override
//        public void init() {
//            int n_bits = super.getStoredLength() - Page.SIZE;
//            bits = new byte[n_bits];
//        }
//
//        @Override
//        public void retrieve(ByteBuffer bb) {
//            super.retrieve(bb);
//            bb.get(bits);
//        }

        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            bb.put(bits);
        }

        public void setBit(int offset, byte value) {
            bits[offset] = value;
        }

        public int getBit(int offset) {
            return bits[offset];
        }

        static final class BitMgrPageFactory implements PageFactory {
        	private final PageManager pageFactory;
        	BitMgrPageFactory(PageManager pageFactory) {
        		this.pageFactory = pageFactory;
        	}
//			public Class<?> getType() {
//				return BitMgrPage.class;
//			}
//			public Object newInstance() {
//				return new BitMgrPage(pageFactory);
//			}
//			public Object newInstance(ByteBuffer buf) {
//				return new BitMgrPage(pageFactory, buf);
//			}
			public Page getInstance(int type, PageId pageId) {
				return new BitMgrPage(pageFactory, type, pageId);
			}
			public Page getInstance(PageId pageId, ByteBuffer bb) {
				return new BitMgrPage(pageFactory, pageId, bb);
			}
			public int getPageType() {
				return TYPE_BITMGRPAGE;
			}
        }
    }

    public static class OneBitMgr extends BaseTransactionalModule {

        static final short moduleId = 10;

        BufferManager bufmgr;

        LogManager logmgr;

        LoggableFactory loggableFactory;

        PageId pageId = new PageId(1, 0);

        StorageContainerFactory storageFactory;

        StorageManager storageManager;

        TransactionManager trxmgr;

        PageManager pageFactory;

        ObjectRegistry objectFactory;

        public OneBitMgr(StorageContainerFactory storageFactory,
                StorageManager storageManager, PageManager pageFactory,
                BufferManager bufmgr, LoggableFactory loggableFactory,
                TransactionManager trxmgr, ObjectRegistry objectFactory) {
            this.storageFactory = storageFactory;
            this.storageManager = storageManager;
            this.pageFactory = pageFactory;
            this.bufmgr = bufmgr;
            this.loggableFactory = loggableFactory;
            this.objectFactory = objectFactory;
            this.trxmgr = trxmgr;
        }

//        @Override
//        public void undo(Transaction trx, Undoable undoable)
//                throws BufferManagerException, TransactionException {
//
//            BitMgrLogRedoUndo logrec = (BitMgrLogRedoUndo) undoable;
//
//            BitMgrLogCLR clr = (BitMgrLogCLR) loggableFactory.getInstance(
//                OneBitMgr.moduleId,
//                TestTransactionManager1.TYPE_BITMGRLOGCLR);
//            BufferAccessBlock bab = bufmgr.fixExclusive(
//                pageId,
//                false,
//                TestTransactionManager1.TYPE_BITMGRPAGE,
//                0);
//            try {
//                BitMgrPage page = (BitMgrPage) bab.getPage();
//
//                clr.index = logrec.index;
//                clr.newValue = logrec.oldValue;
//                System.out.println("UNDO: Setting bit[" + clr.index + "] to "
//                        + clr.newValue);
//
//                /*
//                 * How to ensure that these are not missed out??
//                 */
//                clr.setPageId(TYPE_BITMGRPAGE, pageId);
//                clr.setUndoNextLsn(logrec.getPrevTrxLsn());
//                Lsn lsn = trx.logInsert(page, clr);
//                page.setBit(clr.index, (byte) clr.newValue);
//                bab.setDirty(lsn);
//            } finally {
//                bab.unfix();
//            }
//
//        }        
        
        @Override
        public Compensation generateCompensation(Undoable undoable) {
            if (undoable instanceof BitMgrLogRedoUndo) {
                BitMgrLogRedoUndo logrec = (BitMgrLogRedoUndo) undoable;

//                BitMgrLogCLR clr = (BitMgrLogCLR) loggableFactory.getInstance(
//                    OneBitMgr.moduleId,
//                    TestTransactionManager2.TYPE_BITMGRLOGCLR);
                BitMgrLogCLR clr = new BitMgrLogCLR(
                        OneBitMgr.moduleId,
                        TestTransactionManager2.TYPE_BITMGRLOGCLR);
                clr.index = logrec.index;
                clr.newValue = logrec.oldValue;
                System.out.println("UNDO: Setting bit[" + clr.index + "] to "
                        + clr.newValue);
                return clr;
            }
            return null;
        }

        @Override
        public void redo(Loggable loggable) throws StorageException {
            if (loggable instanceof BitMgrLogOpenContainer) {
                BitMgrLogOpenContainer logrec = (BitMgrLogOpenContainer) loggable;
                StorageContainer sc = storageManager.getInstance(logrec
                    .getContainerId());
                if (sc == null) {
                    sc = storageFactory.open(logrec.getName());
                    storageManager.register(logrec.getContainerId(), sc);
                }
            }
        }

        @Override
        public void redo(Page page, Redoable loggable) throws StorageException,
                PageException {
            if (loggable instanceof BitMgrLogCreateContainer) {
                BitMgrLogCreateContainer logrec = (BitMgrLogCreateContainer) loggable;
                StorageContainer sc = storageFactory.create(logrec.getName());
                storageManager.register(logrec.getContainerId(), sc);
            } else if (loggable instanceof BitMgrLogRedoUndo) {
                BitMgrLogRedoUndo logrec = (BitMgrLogRedoUndo) loggable;
                System.out.println("REDO: Setting bit[" + logrec.index
                        + "] to " + logrec.newValue);
                BitMgrPage bpage = (BitMgrPage) page;
                bpage.setBit(logrec.index, (byte) logrec.newValue);
            } else if (loggable instanceof BitMgrLogFormatPage) {
                page.setPageLsn(loggable.getLsn());
                pageFactory.store(page);
            } else if (loggable instanceof BitMgrLogCLR) {
                BitMgrLogCLR logrec = (BitMgrLogCLR) loggable;
                System.out.println("REDO (CLR): Setting bit[" + logrec.index
                        + "] to " + logrec.newValue);
                BitMgrPage bpage = (BitMgrPage) page;
                bpage.setBit(logrec.index, (byte) logrec.newValue);
            }
        }

        public void changeBit(Transaction trx, int index, int newValue)
                throws BufferManagerException, TransactionException {
            trx.acquireLock(
                new ObjectLock(ObjectLock.CONTAINER, pageId.getContainerId()),
                LockMode.SHARED,
                LockDuration.MANUAL_DURATION);
            trx.acquireLock(
                new ObjectLock(ObjectLock.BIT, index),
                LockMode.EXCLUSIVE,
                LockDuration.COMMIT_DURATION);
//            BitMgrLogRedoUndo logrec = (BitMgrLogRedoUndo) loggableFactory
//                .getInstance(
//                    OneBitMgr.moduleId,
//                    TestTransactionManager2.TYPE_BITMGRLOGREDOUNDO);
            BitMgrLogRedoUndo logrec = new BitMgrLogRedoUndo(
                OneBitMgr.moduleId,
                TestTransactionManager2.TYPE_BITMGRLOGREDOUNDO);
            BufferAccessBlock bab = bufmgr.fixExclusive(
                pageId,
                false,
                TestTransactionManager2.TYPE_BITMGRPAGE,
                0);
            try {
                BitMgrPage page = (BitMgrPage) bab.getPage();

                logrec.index = index;
                logrec.newValue = newValue;
                logrec.oldValue = page.getBit(index);
                System.out.println("DO: Setting bit[" + logrec.index + "] to "
                        + logrec.newValue);

                // logrec.setPageId(objectFactory.getTypeCode("bitmgrpage"), pageId);
                Lsn lsn = trx.logInsert(page, logrec);
                page.setBit(logrec.index, (byte) logrec.newValue);
                bab.setDirty(lsn);
            } finally {
                bab.unfix();
            }
        }

        public int getBit(Transaction trx, int index)
                throws BufferManagerException, TransactionException {
            trx.acquireLock(
                new ObjectLock(ObjectLock.CONTAINER, pageId.getContainerId()),
                LockMode.SHARED,
                LockDuration.MANUAL_DURATION);
            trx.acquireLock(
                new ObjectLock(ObjectLock.BIT, index),
                LockMode.SHARED,
                LockDuration.MANUAL_DURATION);
            BufferAccessBlock bab = bufmgr.fixShared(pageId, 0);
            int value = -1;
            try {
                BitMgrPage page = (BitMgrPage) bab.getPage();
                value = page.getBit(index);
            } finally {
                bab.unfix();
            }
            return value;
        }

        public int getBitForUpdate(Transaction trx, int index)
                throws BufferManagerException, TransactionException {
            trx.acquireLock(
                new ObjectLock(ObjectLock.CONTAINER, pageId.getContainerId()),
                LockMode.SHARED,
                LockDuration.MANUAL_DURATION);
            trx.acquireLock(
                new ObjectLock(ObjectLock.BIT, index),
                LockMode.UPDATE,
                LockDuration.MANUAL_DURATION);
            BufferAccessBlock bab = bufmgr.fixShared(pageId, 0);
            int value = -1;
            try {
                BitMgrPage page = (BitMgrPage) bab.getPage();
                value = page.getBit(index);
            } finally {
                bab.unfix();
            }
            return value;
        }

        public void create(String name, int containerId, int pageNumber)
                throws StorageException, BufferManagerException,
                TransactionException, PageException {

            Transaction trx = trxmgr.begin(IsolationMode.SERIALIZABLE);
            boolean success = false;
            try {
//                BitMgrLogCreateContainer logcreate = (BitMgrLogCreateContainer) loggableFactory
//                    .getInstance(
//                        OneBitMgr.moduleId,
//                        TestTransactionManager2.TYPE_BITMGRLOGCREATECONTAINER);
//                BitMgrLogOpenContainer logopen = (BitMgrLogOpenContainer) loggableFactory
//                    .getInstance(
//                        OneBitMgr.moduleId,
//                        TestTransactionManager2.TYPE_BITMGRLOGOPENCONTAINER);
//                BitMgrLogFormatPage formatpage = (BitMgrLogFormatPage) loggableFactory
//                    .getInstance(
//                        OneBitMgr.moduleId,
//                        TestTransactionManager2.TYPE_BITMGRLOGFORMATPAGE);

                BitMgrLogCreateContainer logcreate = new BitMgrLogCreateContainer(
                    OneBitMgr.moduleId,
                    TestTransactionManager2.TYPE_BITMGRLOGCREATECONTAINER);
                BitMgrLogOpenContainer logopen = new BitMgrLogOpenContainer(
                    OneBitMgr.moduleId,
                    TestTransactionManager2.TYPE_BITMGRLOGOPENCONTAINER);
                BitMgrLogFormatPage formatpage = new BitMgrLogFormatPage(
                    OneBitMgr.moduleId,
                    TestTransactionManager2.TYPE_BITMGRLOGFORMATPAGE);            	
            	
                trx.acquireLock(
                    new ObjectLock(ObjectLock.CONTAINER, containerId),
                    LockMode.EXCLUSIVE,
                    LockDuration.COMMIT_DURATION);
                /* check if the container already exists */
                StorageContainer sc = storageManager.getInstance(containerId);
                if (sc != null) {
                    throw new StorageException(); // TODO change exception type
                }

                BufferAccessBlock bab1 = bufmgr.fixExclusive(
                    new PageId(999, 0),
                    true,
                    pageFactory.getRawPageType(),
                    0);
                try {
                    Page page1 = bab1.getPage();
                    logcreate.setContainerId(containerId);
                    logcreate.setName(name);
                    Lsn mylsn = trx.logInsert(page1, logcreate);
                    logcreate.setLsn(mylsn);
                    redo(page1, logcreate);

                    logopen.setContainerId(containerId);
                    logopen.setName(name);
//					mylsn = trx.logInsert(page1, logopen);
//					logopen.setLsn(mylsn);
//					redo(logopen);
//					bab1.setDirty(mylsn);
                    trxmgr.logNonTransactionRelatedOperation(logopen);

                    PageId pageId = new PageId(containerId, pageNumber);
                    formatpage.setPageId(TYPE_BITMGRPAGE, pageId);
                    BufferAccessBlock bab = bufmgr.fixExclusive(
                        pageId,
                        true,
                        TestTransactionManager2.TYPE_BITMGRPAGE,
                        0);
                    try {
                        Page page = bab.getPage();
                        mylsn = trx.logInsert(page, formatpage);
                        formatpage.setLsn(mylsn);
                        redo(page, formatpage);
                        bab.setDirty(mylsn);
                    } finally {
                        bab.unfix();
                    }
                } finally {
                    bab1.unfix();
                }
            } finally {
                /*
                 * To test we deliberatly do not set success to true, causing the
                 * transaction to rollback. 
                 */
                if (success) {
                    trx.commit();
                } else {
                    trx.abort();
                }
            }
        }

    }

}
