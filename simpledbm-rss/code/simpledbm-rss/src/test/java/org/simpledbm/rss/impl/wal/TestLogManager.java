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
package org.simpledbm.rss.impl.wal;

import java.nio.ByteBuffer;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.wal.LogFactory;
import org.simpledbm.rss.api.wal.LogManager;
import org.simpledbm.rss.api.wal.LogReader;
import org.simpledbm.rss.api.wal.LogRecord;
import org.simpledbm.rss.api.wal.Lsn;
import org.simpledbm.rss.impl.st.FileStorageContainerFactory;

/**
 * Test cases for the Log Manager (Write Ahead Log) module.
 * 
 * @author Dibyendu Majumdar
 * @since 21-Aug-2005
 */
public class TestLogManager extends BaseTestCase {

    public TestLogManager(String name) {
        super(name);
    }

    public void testCreate() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("storage.basePath", "testdata/TestLogManager");
        properties.setProperty("logging.properties.file",
                "classpath:simpledbm.logging.properties");
        properties.setProperty("logging.properties.type", "log4j");
//        Platform platform = new PlatformImpl(properties);
        StorageContainerFactory storageFactory = new FileStorageContainerFactory(
                platform, properties);
        LogFactory factory = new LogFactoryImpl(platform, storageFactory, properties);
        factory.createLog();
    }

    public void testCreate2() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("log.ctl.1", "ctl.a");
        properties.setProperty("log.ctl.2", "ctl.b");
        properties.setProperty("log.groups.1.path", ".");
        properties.setProperty("log.archive.path", ".");
        properties.setProperty("log.group.files", "3");
        properties.setProperty("log.file.size", "16384");
        properties.setProperty("log.buffer.size", "16384");
        properties.setProperty("log.buffer.limit", "4");
        properties.setProperty("log.flush.interval", "30");
        properties.setProperty("storage.basePath", "testdata/TestLogManager");
        properties.setProperty("logging.properties.file",
                "classpath:simpledbm.logging.properties");
        properties.setProperty("logging.properties.type", "log4j");
//        Platform platform = new PlatformImpl(properties);
        StorageContainerFactory storageFactory = new FileStorageContainerFactory(
                platform, properties);
        LogFactory factory = new LogFactoryImpl(platform, storageFactory, properties);
        factory.createLog();
    }

    public void testOpen() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("storage.basePath", "testdata/TestLogManager");
        properties.setProperty("logging.properties.file",
                "classpath:simpledbm.logging.properties");
        properties.setProperty("logging.properties.type", "log4j");
//        Platform platform = new PlatformImpl(properties);
        StorageContainerFactory storageFactory = new FileStorageContainerFactory(
                platform, properties);
        LogFactory factory = new LogFactoryImpl(platform, storageFactory, properties);
        LogManager log = factory.getLog();
        log.start();
        log.shutdown();
    }

    public void testInsertOne() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("storage.basePath", "testdata/TestLogManager");
        properties.setProperty("logging.properties.file",
                "classpath:simpledbm.logging.properties");
        properties.setProperty("logging.properties.type", "log4j");
//        Platform platform = new PlatformImpl(properties);
        StorageContainerFactory storageFactory = new FileStorageContainerFactory(
                platform, properties);
        LogFactory factory = new LogFactoryImpl(platform, storageFactory, properties);
        LogManager log = factory.getLog();
        log.start();
        try {
            String s = "hello world!";
            byte[] b = s.getBytes();
            Lsn lsn = log.insert(b, b.length);
            System.out.println("Lsn of new record = " + lsn);
        } finally {
            if (log != null)
                log.shutdown();
        }
    }

    public void testReadOne() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("storage.basePath", "testdata/TestLogManager");
        properties.setProperty("logging.properties.file",
                "classpath:simpledbm.logging.properties");
        properties.setProperty("logging.properties.type", "log4j");
//        Platform platform = new PlatformImpl(properties);
        StorageContainerFactory storageFactory = new FileStorageContainerFactory(
                platform, properties);
        LogFactory factory = new LogFactoryImpl(platform, storageFactory, properties);
        LogManager log = factory.getLog();
        log.start();
        try {
            LogReader reader = log.getForwardScanningReader(null);
            LogRecord rec = reader.getNext();
            byte[] b = rec.getData();
            String s = "hello world!";
            String s2 = new String(b);
            assertTrue(s.equals(s2));
            System.out.println("Record = {" + s2 + "}");
        } finally {
            if (log != null)
                log.shutdown();
        }
    }

    public void testLogSwitch() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("storage.basePath", "testdata/TestLogManager");
        properties.setProperty("logging.properties.file",
                "classpath:simpledbm.logging.properties");
        properties.setProperty("logging.properties.type", "log4j");
//        Platform platform = new PlatformImpl(properties);
        StorageContainerFactory storageFactory = new FileStorageContainerFactory(
                platform, properties);
        LogFactory factory = new LogFactoryImpl(platform, storageFactory, properties);
        LogManager log = factory.getLog();
        log.start();
        int n = ((2048 / 50) * 2) + 2; // just enough to go beyond two log
        // files
        try {
            for (int i = 0; i < n; i++) {
                String s = ("Record #" + i + "                                                 ")
                        .substring(0, 22);
                byte[] b = s.getBytes();
                Lsn lsn = log.insert(b, b.length);
                System.out.println("Lsn of new record = " + lsn);
            }
        } finally {
            if (log != null)
                log.shutdown();
        }

    }

    public void testReadAfterLogSwitch() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("storage.basePath", "testdata/TestLogManager");
        properties.setProperty("logging.properties.file",
                "classpath:simpledbm.logging.properties");
        properties.setProperty("logging.properties.type", "log4j");
//        Platform platform = new PlatformImpl(properties);
        StorageContainerFactory storageFactory = new FileStorageContainerFactory(
                platform, properties);
        LogFactory factory = new LogFactoryImpl(platform, storageFactory, properties);
        LogManager log = factory.getLog();
        log.start();
        try {
            LogReader reader = log.getForwardScanningReader(null);
            int n = ((2048 / 50) * 2) + 2 + 1; // just enough to go beyond two
            // log
            // files
            for (int i = 0; i < n; i++) {
                LogRecord rec = reader.getNext();
                byte[] b = rec.getData();
                String s;
                if (i == 0) {
                    s = "hello world!";
                } else {
                    s = ("Record #" + (i - 1) + "                     ")
                            .substring(0, 22);
                }
                String s2 = new String(b);
                assertTrue(s.equals(s2));
                System.out.println("Record = {" + s2 + "}");
            }
        } finally {
            if (log != null)
                log.shutdown();
        }
    }

    void printRecord(LogRecord rec) {
        if (rec != null) {
            byte[] data = rec.getData();
            ByteBuffer bb = ByteBuffer.wrap(data);
            MyRecord trec = new MyRecord(0);
            trec.retrieve(bb);
            //System.out.println(Thread.currentThread().getName() + ":" + trec);
        }
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    void readLastRecord(LogManager log) throws Exception {
        Lsn lsn = log.getMaxLsn();
        LogReader reader = log.getForwardScanningReader(lsn);
        LogRecord rec = reader.getNext();
        reader.close();
        printRecord(rec);
    }

    void readLastFile(LogManager log) throws Exception {
        Lsn lsn = log.getMaxLsn();
        if (lsn.isNull()) {
            return;
        }
        Lsn startLsn = new Lsn(lsn.getIndex(), LogManagerImpl.FIRST_LSN
                .getOffset());
        //System.out.println("Starting last file scan from " + startLsn);
        LogReader reader = log.getForwardScanningReader(startLsn);
        for (;;) {
            LogRecord rec = reader.getNext();
            if (rec == null) {
                break;
            }
            printRecord(rec);
            if (rec.getLsn().getIndex() > startLsn.getIndex()) {
                break;
            }
        }
        reader.close();
    }

    void readAllRecords(LogManager log) throws Exception {
        LogReader reader = log.getForwardScanningReader(null);
        for (;;) {
            LogRecord rec = reader.getNext();
            if (rec == null) {
                break;
            }
            printRecord(rec);
        }
        reader.close();
    }

    void insertRecords(LogManager log, int startNo, int endNo) throws Exception {
        byte[] data = new byte[22];
        ByteBuffer bb = ByteBuffer.wrap(data);
        MyRecord trec = new MyRecord(0);
        for (int i = startNo; i < endNo; i++) {
            bb.clear();
            trec.id = i;
            trec.store(bb);
            Lsn lsn = log.insert(data, data.length);
            System.out.println(Thread.currentThread().getName() + ":inserted "
                    + trec + ", lsn = " + lsn);
            // Thread.sleep(10);
        }
    }

    public void testMultipleThreads() throws Exception {
        testCreate(); // create a fresh log.

        Properties properties = new Properties();
        properties.setProperty("storage.basePath", "testdata/TestLogManager");
        properties.setProperty("logging.properties.file",
                "classpath:simpledbm.logging.properties");
        properties.setProperty("logging.properties.type", "log4j");
//        Platform platform = new PlatformImpl(properties);
        StorageContainerFactory storageFactory = new FileStorageContainerFactory(
                platform, properties);
        LogFactory factory = new LogFactoryImpl(platform, storageFactory, properties);
        LogManager log = factory.getLog();
        log.start();
        try {
            LastRecordReader reader1 = new LastRecordReader(log, this);
            LastFileReader reader2 = new LastFileReader(log, this);
            AllRecordsReader reader3 = new AllRecordsReader(log, this);
            Thread threadInserter1 = new Thread(new RecordInserter(log, this,
                    1, 10000), "Inserter10000");
            Thread threadInserter2 = new Thread(new RecordInserter(log, this,
                    10001, 10000), "Inserter20000");
            Thread threadReader1 = new Thread(reader1, "LastRecordReader");
            Thread threadReader2 = new Thread(reader2, "LastFileReader");
            Thread threadReader3 = new Thread(reader3, "AllRecordsReader");

            threadInserter1.start();
            threadReader1.start();
            threadReader2.start();
            threadReader3.start();
            threadInserter2.start();

            threadInserter1.join();
            threadInserter2.join();

            reader1.stop();
            reader2.stop();
            reader3.stop();

            threadReader1.join();
            threadReader2.join();
            threadReader3.join();

        } finally {
            if (log != null)
                log.shutdown();
        }
    }

    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTest(new TestLogManager("testCreate2"));
        suite.addTest(new TestLogManager("testCreate"));
        suite.addTest(new TestLogManager("testOpen"));
        suite.addTest(new TestLogManager("testInsertOne"));
        suite.addTest(new TestLogManager("testReadOne"));
        suite.addTest(new TestLogManager("testLogSwitch"));
        suite.addTest(new TestLogManager("testReadAfterLogSwitch"));
        //		suite.addTest(new LogTests("testMultipleThreads"));
        return suite;
    }

}

/**
 * Reads the latest record in a loop
 * 
 * @author Dibyendu Majumdar
 * 
 */
abstract class ActionReader implements Runnable {

    TestLogManager tester;

    LogManager log;

    volatile boolean stopped = false;

    public ActionReader(LogManager log, TestLogManager tester) {
        this.log = log;
        this.tester = tester;
    }

    public abstract void execute() throws Exception;

    public void run() {
        while (!stopped) {
            try {
                execute();
            } catch (Exception e) {
                e.printStackTrace();
                stopped = true;
            }
        }
    }

    public void stop() {
        stopped = true;
    }
}

/**
 * Reads the latest record in a loop
 * 
 * @author Dibyendu Majumdar
 * 
 */
class LastRecordReader extends ActionReader {

    public LastRecordReader(LogManager log, TestLogManager tester) {
        super(log, tester);
    }

    @Override
    public void execute() throws Exception {
        tester.readLastRecord(log);
    }

}

class LastFileReader extends ActionReader {

    public LastFileReader(LogManager log, TestLogManager tester) {
        super(log, tester);
    }

    @Override
    public void execute() throws Exception {
        tester.readLastFile(log);
    }

}

class AllRecordsReader extends ActionReader {

    public AllRecordsReader(LogManager log, TestLogManager tester) {
        super(log, tester);
    }

    @Override
    public void execute() throws Exception {
        tester.readAllRecords(log);
    }

}

class RecordInserter extends ActionReader {

    int n;

    int startNo;

    public RecordInserter(LogManager log, TestLogManager tester, int startNo,
            int n) {
        super(log, tester);
        this.n = n;
        this.startNo = startNo;
    }

    @Override
    public void execute() throws Exception {
        tester.insertRecords(log, startNo, startNo + n);
        this.stop(); // Execute only once
    }

}

class MyRecord implements Storable {
    int id;

    public MyRecord(int id) {
        this.id = id;
    }

    public int getStoredLength() {
        return 22;
    }

    public void retrieve(ByteBuffer bb) {
        id = bb.getInt();
        for (int i = 0; i < 18; i++) {
            bb.get();
        }
    }

    public void store(ByteBuffer bb) {
        bb.putInt(id);
        for (int i = 0; i < 18; i++) {
            bb.put((byte) 0);
        }
    }

    @Override
    public boolean equals(Object arg0) {
        if (!(arg0 instanceof MyRecord))
            return false;
        MyRecord other = (MyRecord) arg0;
        return other.id == id;
    }

    @Override
    public String toString() {
        return "TestRecord(" + id + ")";
    }

}