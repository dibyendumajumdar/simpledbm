/**
 * DO NOT REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Contributor(s):
 *
 * The Original Software is SimpleDBM (www.simpledbm.org).
 * The Initial Developer of the Original Software is Dibyendu Majumdar.
 *
 * Portions Copyright 2005-2014 Dibyendu Majumdar. All Rights Reserved.
 *
 * The contents of this file are subject to the terms of the
 * Apache License Version 2 (the "APL"). You may not use this
 * file except in compliance with the License. A copy of the
 * APL may be obtained from:
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the APL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the APL, the GPL or the LGPL.
 *
 * Copies of GPL and LGPL may be obtained from:
 * http://www.gnu.org/licenses/license-list.html
 */
/*
 * Created on: 12-Dec-2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.rss.impl.tuple;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.registry.ObjectRegistry;
import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.api.tx.IsolationMode;
import org.simpledbm.common.impl.registry.ObjectRegistryImpl;
import org.simpledbm.common.util.ByteString;
import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.rss.api.bm.BufferManager;
import org.simpledbm.rss.api.fsm.FreeSpaceManager;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.locking.LockManager;
import org.simpledbm.rss.api.locking.LockMgrFactory;
import org.simpledbm.rss.api.locking.util.LockAdaptor;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.sp.SlottedPageManager;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.api.tuple.TupleContainer;
import org.simpledbm.rss.api.tuple.TupleInserter;
import org.simpledbm.rss.api.tuple.TupleManager;
import org.simpledbm.rss.api.tuple.TupleScan;
import org.simpledbm.rss.api.tx.LoggableFactory;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.api.tx.TransactionException;
import org.simpledbm.rss.api.tx.TransactionalModuleRegistry;
import org.simpledbm.rss.api.wal.LogManager;
import org.simpledbm.rss.impl.bm.BufferManagerImpl;
import org.simpledbm.rss.impl.fsm.FreeSpaceManagerImpl;
import org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl;
import org.simpledbm.rss.impl.latch.LatchFactoryImpl;
import org.simpledbm.rss.impl.locking.LockManagerFactoryImpl;
import org.simpledbm.rss.impl.locking.util.DefaultLockAdaptor;
import org.simpledbm.rss.impl.pm.PageManagerImpl;
import org.simpledbm.rss.impl.sp.SlottedPageManagerImpl;
import org.simpledbm.rss.impl.st.FileStorageContainerFactory;
import org.simpledbm.rss.impl.st.StorageManagerImpl;
import org.simpledbm.rss.impl.tx.LoggableFactoryImpl;
import org.simpledbm.rss.impl.tx.TransactionManagerImpl;
import org.simpledbm.rss.impl.tx.TransactionalModuleRegistryImpl;
import org.simpledbm.rss.impl.wal.LogFactoryImpl;

public class TestTupleManager extends BaseTestCase {

    public TestTupleManager() {
        super();
    }

    public TestTupleManager(String arg0) {
        super(arg0);
    }

    private Properties getLogProperties() {
        Properties properties = new Properties();
        properties.setProperty("log.ctl.1", "log/control1/ctl.a");
        properties.setProperty("log.ctl.2", "log/control2/ctl.b");
        properties.setProperty("log.groups.1.path", "log/current");
        properties.setProperty("log.archive.path", "log/archive");
        properties.setProperty("log.group.files", "3");
        properties.setProperty("log.file.size", "65536");
        properties.setProperty("log.buffer.size", "65536");
        properties.setProperty("log.buffer.limit", "4");
        properties.setProperty("log.flush.interval", "30");
        properties.setProperty("storage.basePath", "testdata/TestTupleManager");
        properties.setProperty("logging.properties.file",
                "classpath:simpledbm.logging.properties");
        properties.setProperty("logging.properties.type", "log4j");
        return properties;
    }

    /**
     * Initialize the test harness. New log is created, and the test container
     * initialized. The container is allocated an extent of 64 pages which ought
     * to be large enough for all the test cases.
     */
    public void testCase1() throws Exception {

        final TupleDB db = new TupleDB(platform, getLogProperties(), true);

        try {
            StorageContainer sc = db.storageFactory.create("dual");
            db.storageManager.register(0, sc);
            Page page = db.pageFactory.getInstance(db.pageFactory
                    .getRawPageType(), new PageId(0, 0));
            db.pageFactory.store(page);

            db.trxmgr.start();
            Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
            boolean success = false;
            try {
                db.tuplemgr.createTupleContainer(trx, "testctr.dat", 1, 20);
                success = true;
            } finally {
                if (success)
                    trx.commit();
                else
                    trx.abort();
            }
            db.trxmgr.checkpoint();
        } finally {
            db.shutdown();
        }
    }

    public void testCase2() throws Exception {

        final TupleDB db = new TupleDB(platform, getLogProperties(), false);

        try {
            db.trxmgr.start();
            Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
            TupleContainer tcont = db.tuplemgr.getTupleContainer(trx, 1);
            StringTuple t = new StringTuple();
            t.parseString("hello", 16524);
            TupleInserter inserter = tcont.insert(trx, t);
            Location location = inserter.getLocation();
            inserter.completeInsert();
            trx.commit();
            trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
            tcont = db.tuplemgr.getTupleContainer(trx, 1);
            byte[] data = tcont.read(location);
            assertEquals(data.length, 16526);
            ByteBuffer bb = ByteBuffer.wrap(data);
            t = new StringTuple(bb);
            assertTrue(t.toString().equals("hello"));
            tcont.delete(trx, location);
            trx.abort();
            t = new StringTuple();
            t.parseString("updated hello", 18000);
            trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
            tcont = db.tuplemgr.getTupleContainer(trx, 1);
            data = tcont.read(location);
            assertEquals(data.length, 16526);
            tcont.update(trx, location, t);
            data = tcont.read(location);
            bb = ByteBuffer.wrap(data);
            t = new StringTuple(bb);
            trx.commit();
            assertEquals(t.getStoredLength(), 18002);
            assertTrue(t.toString().equals("updated hello"));
        } finally {
            db.shutdown();
        }

    }

    public void testCase3() throws Exception {

        final TupleDB db = new TupleDB(platform, getLogProperties(), false);

        try {
            db.trxmgr.start();

            TupleContainer tcont = null;
            /*
             * First insert a few rows.
             */
            int[] tlens = new int[] { 18000, 15, 95, 138, 516, 1700, 4500,
                    13000 };
            for (int i = 1; i < tlens.length; i++) {
                Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
                tcont = db.tuplemgr.getTupleContainer(trx, 1);
                StringTuple t = new StringTuple();
                t.parseString("rec" + i, tlens[i]);
                TupleInserter inserter = tcont.insert(trx, t);
                inserter.getLocation();
                inserter.completeInsert();
                trx.commit();
            }

            Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
            tcont = db.tuplemgr.getTupleContainer(trx, 1);
            TupleScan scan = tcont.openScan(trx, false);
            int i = 0;
            while (scan.fetchNext()) {
                byte[] data = scan.getCurrentTuple();
                System.err.println("len=" + data.length);
                assertEquals(tlens[i] + 2, data.length);
                ByteBuffer bb = ByteBuffer.wrap(data);
                StringTuple t = new StringTuple(bb);
                System.err.println("Location=" + scan.getCurrentLocation()
                        + ", tupleData=[" + t.toString() + "]");
                i++;
            }
            assertEquals(i + 1, ((TransactionManagerImpl.TransactionImpl) trx)
                    .countLocks());
            trx.commit();
        } finally {
            db.shutdown();
        }
    }

    Location location = null;

    /**
     * This test case uses two threads. The first thread creates a new tuple.
     * The second thread starts a scan and should wait for the first thread to
     * commit or abort. The first thread aborts and the second thread completes
     * the scan.
     */
    void doTestCase4(final boolean commit) throws Exception {

        final TupleDB db = new TupleDB(platform, getLogProperties(), false);

        try {
            db.trxmgr.start();

            Thread thr = new Thread(new Runnable() {
                public void run() {
                    Transaction trx = db.trxmgr
                            .begin(IsolationMode.SERIALIZABLE);
                    TupleContainer tcont = db.tuplemgr
                            .getTupleContainer(trx, 1);
                    try {
                        StringTuple t = new StringTuple();
                        t.parseString("sample", 10000);
                        TupleInserter inserter = tcont.insert(trx, t);
                        inserter.getLocation();
                        inserter.completeInsert();
                        System.err
                                .println("Inserted new tuple - going to sleep");
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        if (trx != null) {
                            try {
                                if (!commit) {
                                    System.err.println("Aborting tuple insert");
                                    trx.abort();
                                } else {
                                    System.err
                                            .println("Committing tuple insert");
                                    trx.commit();
                                }
                            } catch (TransactionException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            });

            /*
             * First insert a few rows.
             */
            int[] tlens;
            if (commit) {
                tlens = new int[] { 18000, 15, 95, 138, 516, 1700, 4500, 13000,
                        10000 };
            } else {
                tlens = new int[] { 18000, 15, 95, 138, 516, 1700, 4500, 13000 };
            }
            Transaction trx = db.trxmgr.begin(IsolationMode.READ_COMMITTED);
            TupleContainer tcont = db.tuplemgr.getTupleContainer(trx, 1);
            TupleScan scan = tcont.openScan(trx, false);

            thr.start();
            Thread.sleep(100);

            int i = 0;
            while (scan.fetchNext()) {
                byte[] data = scan.getCurrentTuple();
                System.err.println("len=" + data.length);
                assertEquals(tlens[i] + 2, data.length);
                ByteBuffer bb = ByteBuffer.wrap(data);
                StringTuple t = new StringTuple(bb);
                System.err.println("Location=" + scan.getCurrentLocation()
                        + ", tupleData=[" + t.toString() + "]");
                if (location == null && i > (tlens.length / 2)) {
                    location = scan.getCurrentLocation();
                }
                i++;
                System.err.println("Fetching next tuple");
            }
            System.err.println("Scan completed");
            assertEquals(1, ((TransactionManagerImpl.TransactionImpl) trx)
                    .countLocks());
            trx.commit();

            thr.join(2000);
            assertTrue(!thr.isAlive());
        } finally {
            db.shutdown();
        }
    }

    public void testCase4() throws Exception {
        doTestCase4(false);
    }

    public void testCase5() throws Exception {
        doTestCase4(true);
    }

    public void doTestUndoUpdate() throws Exception {
        final TupleDB db = new TupleDB(platform, getLogProperties(), false);

        try {
            db.trxmgr.start();
            Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
            TupleContainer tcont = db.tuplemgr.getTupleContainer(trx, 1);
            TupleScan scan = tcont.openScan(trx, true);
            int i = 0;
            ArrayList<Integer> lens = new ArrayList<Integer>();
            ArrayList<Integer> newLens = new ArrayList<Integer>();
            while (scan.fetchNext()) {
                byte[] data = scan.getCurrentTuple();
                System.err.println("len=" + data.length);
                lens.add(data.length);
                if (data.length < 100) {
                    Location location = scan.getCurrentLocation();
                    StringTuple t = new StringTuple();
                    t.parseString("updating tuple " + location.toString(),
                            16524);
                    tcont.update(trx, location, t);
                    newLens.add(16524 + 2);
                } else {
                    newLens.add(data.length);
                }
            }
            scan.close();

            scan = tcont.openScan(trx, true);
            i = 0;
            while (scan.fetchNext()) {
                byte[] data = scan.getCurrentTuple();
                System.err.println("len=" + data.length);
                assertEquals(new Integer(data.length), newLens.get(i));
                i++;
            }
            scan.close();
            trx.abort();

            trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
            tcont = db.tuplemgr.getTupleContainer(trx, 1);
            scan = tcont.openScan(trx, false);
            i = 0;
            while (scan.fetchNext()) {
                byte[] data = scan.getCurrentTuple();
                System.err.println("len=" + data.length);
                assertEquals(new Integer(data.length), lens.get(i));
                i++;
            }
            scan.close();
            trx.commit();
        } finally {
            db.shutdown();
        }
    }

    public void testCase6() throws Exception {
        doTestUndoUpdate();
    }

    /**
     * This test opens an UPDATE scan and deletes tuples that exceed 100 bytes.
     * It then does another scan to verify that the transaction does not see the
     * deleted tuples. The transaction is committed and another scan is
     * performed to verify. 20 new tuples are added, increasing in size. This is
     * meant to exercise the reclaiming of deleted tuples, as well as trigger a
     * container extension. Transaction is committed and a scan performed to
     * verify the data.
     */
    public void doTestDeleteInsertScan() throws Exception {
        final TupleDB db = new TupleDB(platform, getLogProperties(), false);

        try {
            db.trxmgr.start();
            Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
            TupleContainer tcont = db.tuplemgr.getTupleContainer(trx, 1);
            TupleScan scan = tcont.openScan(trx, true);
            int n_total = 0;
            int n_deleted = 0;
            ArrayList<Integer> lens = new ArrayList<Integer>();
            HashMap<Location, Integer> map = new HashMap<Location, Integer>();
            while (scan.fetchNext()) {
                byte[] data = scan.getCurrentTuple();
                Location location = scan.getCurrentLocation();
                System.err.println("len=" + data.length);
                if (data.length > 100) {
                    tcont.delete(trx, location);
                    System.err.println("Deleted tuple at location " + location);
                    n_deleted++;
                } else {
                    lens.add(data.length);
                    map.put(location, data.length);
                }
                n_total++;
            }
            scan.close();

            System.err.println("Map = " + map);

            scan = tcont.openScan(trx, false);
            int j = 0;
            while (scan.fetchNext()) {
                Location location = scan.getCurrentLocation();
                System.err.println("After delete: Location " + location);
                assertTrue(map.get(location) != null);
                j++;
            }
            scan.close();
            trx.commit();
            assertEquals(j, n_total - n_deleted);

            trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
            tcont = db.tuplemgr.getTupleContainer(trx, 1);
            scan = tcont.openScan(trx, false);
            int i = 0;
            while (scan.fetchNext()) {
                byte[] data = scan.getCurrentTuple();
                location = scan.getCurrentLocation();
                System.err.println("len=" + data.length);
                assertTrue(map.get(location) != null);
                assertEquals(new Integer(data.length), lens.get(i));
                i++;
            }
            scan.close();
            trx.commit();
            assertEquals(i, n_total - n_deleted);

            n_total -= n_deleted;

            trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
            tcont = db.tuplemgr.getTupleContainer(trx, 1);
            int len = 100;
            for (i = 0; i < 20; i++) {
                StringTuple t = new StringTuple();
                t.parseString("hello " + i, len);
                TupleInserter inserter = tcont.insert(trx, t);
                Location location = inserter.getLocation();
                map.put(location, len + 2);
                System.err.println("Tuple [" + t.toString() + "] of length "
                        + len + " inserted at location " + location);
                inserter.completeInsert();
                n_total++;
                len += 1000;
            }
            trx.commit();

            trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
            tcont = db.tuplemgr.getTupleContainer(trx, 1);
            scan = tcont.openScan(trx, false);
            i = 0;
            while (scan.fetchNext()) {
                byte[] data = scan.getCurrentTuple();
                location = scan.getCurrentLocation();
                System.err.println("Tuple " + location + ", length="
                        + data.length);
                assertTrue(map.get(location) != null);
                i++;
            }
            scan.close();
            trx.commit();
            assertEquals(i, n_total);
        } finally {
            db.shutdown();
        }
    }

    public void testCase7() throws Exception {
        doTestDeleteInsertScan();
    }

    public static final class StringTuple implements Storable {

        private ByteString string;

        public StringTuple(String string) {
            byte data[] = new byte[16524];
            Arrays.fill(data, (byte) ' ');
            byte[] srcdata = string.getBytes();
            System.arraycopy(srcdata, 0, data, 0, srcdata.length);
            this.string = new ByteString(data);
        }

        public StringTuple(String string, int padLength) {
            byte data[] = new byte[padLength];
            Arrays.fill(data, (byte) ' ');
            byte[] srcdata = string.getBytes();
            System.arraycopy(srcdata, 0, data, 0, srcdata.length);
            this.string = new ByteString(data);
        }

        public StringTuple(byte[] bytes) {
            string = new ByteString(bytes);
        }

        public StringTuple(ByteBuffer bb) {
            string = new ByteString(bb);
        }

        public StringTuple() {
            string = new ByteString("");
        }

        public void setString(String s) {
            parseString(s);
        }

        public void setBytes(byte[] bytes) {
            string = new ByteString(bytes);
        }

        @Override
        public String toString() {
            return string.toString().trim();
        }

        public void parseString(String string) {
            byte data[] = new byte[16524];
            Arrays.fill(data, (byte) ' ');
            byte[] srcdata = string.getBytes();
            System.arraycopy(srcdata, 0, data, 0, srcdata.length);
            this.string = new ByteString(data);
        }

        public void parseString(String string, int padLength) {
            byte data[] = new byte[padLength];
            Arrays.fill(data, (byte) ' ');
            byte[] srcdata = string.getBytes();
            System.arraycopy(srcdata, 0, data, 0, srcdata.length);
            this.string = new ByteString(data);
        }

        public int getStoredLength() {
            return string.getStoredLength();
        }

        public void store(ByteBuffer bb) {
            string.store(bb);
        }
    }

    public static class TupleDB {
        final Platform platform;
        final LogFactoryImpl logFactory;
        final ObjectRegistry objectFactory;
        final StorageContainerFactory storageFactory;
        final StorageManager storageManager;
        final LatchFactory latchFactory;
        final PageManager pageFactory;
        final SlottedPageManager spmgr;
        final LockMgrFactory lockmgrFactory;
        final LockManager lockmgr;
        final LogManager logmgr;
        final BufferManager bufmgr;
        final LoggableFactory loggableFactory;
        final TransactionalModuleRegistry moduleRegistry;
        final TransactionManagerImpl trxmgr;
        final FreeSpaceManager spacemgr;
        final BTreeIndexManagerImpl btreeMgr;
        final TupleManager tuplemgr;

        public TupleDB(Platform platform, Properties props, boolean create) throws Exception {

//            platform = new PlatformImpl(props);
        	this.platform = platform;
            storageFactory = new FileStorageContainerFactory(platform, props);
            logFactory = new LogFactoryImpl(platform, storageFactory, props);
            if (create) {
                logFactory.createLog();
            }
            LockAdaptor lockAdaptor = new DefaultLockAdaptor(platform, props);
            objectFactory = new ObjectRegistryImpl(platform, props);
            storageManager = new StorageManagerImpl(platform, props);
            latchFactory = new LatchFactoryImpl(platform, props);
            pageFactory = new PageManagerImpl(platform, objectFactory,
                    storageManager, latchFactory, props);
            spmgr = new SlottedPageManagerImpl(platform, objectFactory,
                    pageFactory, props);
            lockmgrFactory = new LockManagerFactoryImpl(platform, props);
            lockmgr = lockmgrFactory.create(latchFactory, props);
            logmgr = logFactory.getLog();
            logmgr.start();
            bufmgr = new BufferManagerImpl(platform, logmgr, pageFactory, 5, 11);
            bufmgr.start();
            loggableFactory = new LoggableFactoryImpl(platform, objectFactory,
                    props);
            moduleRegistry = new TransactionalModuleRegistryImpl(platform,
                    props);
            trxmgr = new TransactionManagerImpl(platform, logmgr,
                    storageFactory, storageManager, bufmgr, lockmgr,
                    loggableFactory, latchFactory, objectFactory,
                    moduleRegistry, props);
            spacemgr = new FreeSpaceManagerImpl(platform, objectFactory,
                    pageFactory, logmgr, bufmgr, storageManager,
                    storageFactory, loggableFactory, trxmgr, moduleRegistry,
                    props);
            btreeMgr = new BTreeIndexManagerImpl(platform, objectFactory,
                    loggableFactory, spacemgr, bufmgr, spmgr, moduleRegistry,
                    lockAdaptor, props);
            tuplemgr = new TupleManagerImpl(platform, objectFactory,
                    loggableFactory, spacemgr, bufmgr, spmgr, moduleRegistry,
                    pageFactory, lockAdaptor, props);
        }

        public void shutdown() {
            trxmgr.shutdown();
            bufmgr.shutdown();
            logmgr.shutdown();
            storageManager.shutdown();
        }
    }

}
