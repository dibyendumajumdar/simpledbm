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
package org.simpledbm.rss.impl.latch;

import java.util.Properties;

import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.rss.api.latch.Latch;

/**
 * 
 * @author Dibyendu Majumdar
 * @since Aug 22, 2005
 */
public class TestLatch extends BaseTestCase {

    //    static Properties properties;
    //    static Platform platform;
    LatchFactoryImpl latchFactory;

    //    static {
    //        properties = new Properties();
    //        properties.setProperty("logging.properties.file",
    //                "classpath:simpledbm.logging.properties");
    //        properties.setProperty("logging.properties.type", "log4j");
    //        platform = new PlatformImpl(properties);
    //    }

    public TestLatch(String arg0) {
        super(arg0);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        latchFactory = new LatchFactoryImpl(platform, new Properties());
    }

    public void testCase1() throws Exception {
        final Latch latch = latchFactory.newReadWriteUpdateLatch();
        latch.exclusiveLock();
        assertTrue(latch.isLatchedExclusively());
        latch.unlockExclusive();
        assertFalse(latch.isLatchedExclusively());

        latch.updateLock();
        assertFalse(latch.isLatchedExclusively());
        assertTrue(latch.isLatchedForUpdate());
        latch.sharedLock();
        assertTrue(latch.isLatchedForUpdate());
        latch.sharedLock();
        assertTrue(latch.isLatchedForUpdate());
        latch.unlockShared();
        latch.unlockShared();
        latch.unlockShared();

        assertFalse(latch.isLatchedExclusively());
        assertFalse(latch.isLatchedForUpdate());
        latch.updateLock();
        latch.upgradeUpdateLock();
        latch.unlockExclusive();
        assertFalse(latch.isLatchedExclusively());
    }

    public void testWriteBlocksWrite() throws Exception {
        final Latch latch = latchFactory.newReadWriteUpdateLatch();
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                assertFalse(latch.tryExclusiveLock());
                latch.exclusiveLock();
                assertTrue(latch.isLatchedExclusively());
                latch.unlockExclusive();
                assertFalse(latch.isLatchedExclusively());
            }
        });
        latch.exclusiveLock();
        assertTrue(latch.isLatchedExclusively());
        t1.start();
        Thread.sleep(100);
        assertTrue(latch.isLatchedExclusively());
        latch.unlockExclusive();
        assertFalse(latch.isLatchedExclusively());
        t1.join(200);
        assertTrue(!t1.isAlive());
    }

    public void testReadBlocksWrite() throws Exception {
        final Latch latch = latchFactory.newReadWriteUpdateLatch();
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                assertFalse(latch.tryExclusiveLock());
                latch.exclusiveLock();
                assertTrue(latch.isLatchedExclusively());
                latch.unlockExclusive();
                assertFalse(latch.isLatchedExclusively());
            }
        });
        latch.sharedLock();
        t1.start();
        Thread.sleep(100);
        assertFalse(latch.isLatchedExclusively());
        latch.unlockShared();
        t1.join(200);
        assertTrue(!t1.isAlive());
    }

    public void testReadBlocksUpgrade() throws Exception {
        final Latch latch = latchFactory.newReadWriteUpdateLatch();
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                assertTrue(latch.trySharedLock());
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                latch.unlockShared();
            }
        });
        t1.start();
        Thread.sleep(100);
        latch.updateLock();
        assertTrue(latch.isLatchedForUpdate());
        assertFalse(latch.tryUpgradeUpdateLock());
        latch.upgradeUpdateLock();
        assertTrue(latch.isLatchedExclusively());
        latch.unlockExclusive();
        assertFalse(latch.isLatchedExclusively());
        assertFalse(latch.isLatchedForUpdate());
        t1.join(300);
        assertTrue(!t1.isAlive());
    }

    boolean testfailed = false;

    public void testReadLockFailsIfUpgradePending() throws Exception {
        final Latch latch = latchFactory.newReadWriteUpdateLatch();
        latch.sharedLock();
        testfailed = false;
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                if (!latch.tryUpdateLock()) {
                    testfailed = true;
                    return;
                }
                latch.upgradeUpdateLock();
                latch.unlockExclusive();
                assertFalse(latch.isLatchedExclusively());
            }
        });
        Thread t2 = new Thread(new Runnable() {
            public void run() {
                if (latch.trySharedLock()) {
                    testfailed = true;
                }
            }
        });
        t1.start();
        Thread.sleep(100);
        t2.start();
        Thread.sleep(100);
        latch.unlockShared();
        t1.join();
        t2.join();
        assertFalse(testfailed);
    }

    public void testReadLockFailsIfWriteLockPending() throws Exception {
        final Latch latch = latchFactory.newReadWriteUpdateLatch();
        latch.sharedLock();
        testfailed = false;
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                latch.exclusiveLock();
                latch.unlockExclusive();
            }
        });
        Thread t2 = new Thread(new Runnable() {
            public void run() {
                if (latch.trySharedLock()) {
                    testfailed = true;
                }
            }
        });
        t1.start();
        Thread.sleep(100);
        t2.start();
        Thread.sleep(100);
        latch.unlockShared();
        t1.join();
        t2.join();
        assertFalse(testfailed);
    }

    /**
     * read tryLock succeeds when readlocked
     */
    public void testTryReadLockWhenReadLocked() throws Exception {
        final Latch latch = latchFactory.newReadWriteUpdateLatch();
        latch.sharedLock();
        testfailed = false;
        Thread t = new Thread(new Runnable() {
            public void run() {
                if (latch.trySharedLock()) {
                    latch.unlockShared();
                } else {
                    testfailed = true;
                }
            }
        });
        t.start();
        t.join();
        latch.unlockShared();

        assertFalse(testfailed);
    }

    /**
     * update tryLock succeeds when readlocked
     */
    public void testTryUpdateLockWhenReadLocked() throws Exception {
        final Latch latch = latchFactory.newReadWriteUpdateLatch();
        latch.sharedLock();
        testfailed = false;
        Thread t = new Thread(new Runnable() {
            public void run() {
                if (latch.tryUpdateLock()) {
                    latch.unlockUpdate();
                } else {
                    testfailed = true;
                }
            }
        });
        t.start();
        t.join();
        latch.unlockShared();

        assertFalse(testfailed);
    }

    /**
     * Try read lock fails when update locked. NOTE: Works when SHARED is NOT
     * compatible with UPDATE
     */
    public void disabledTestTryReadLockFailsWhenUpdateLocked() throws Exception {
        // Test disabled because SHARED locks are now compatible with
        // UPDATE locks
        final Latch latch = latchFactory.newReadWriteUpdateLatch();
        latch.updateLock();
        testfailed = false;
        Thread t = new Thread(new Runnable() {
            public void run() {
                if (latch.trySharedLock()) {
                    testfailed = true;
                    latch.unlockShared();
                }
            }
        });
        t.start();
        t.join();
        latch.unlockUpdate();

        assertFalse(testfailed);
    }

    /**
     * Try read lock succeeds when update locked. NOTE Works when SHARED IS
     * compaibel with UPDATE
     */
    public void testTryReadLockFailsWhenUpdateLocked() throws Exception {
        final Latch latch = latchFactory.newReadWriteUpdateLatch();
        latch.updateLock();
        testfailed = false;
        Thread t = new Thread(new Runnable() {
            public void run() {
                if (!latch.trySharedLock()) {
                    testfailed = true;
                } else {
                    latch.unlockShared();
                }
            }
        });
        t.start();
        t.join();
        latch.unlockUpdate();

        assertFalse(testfailed);
    }

    /**
     * Read lock delayed when update locked.
     */
    public void testReadLockDelayedWhenUpdateLocked() throws Exception {
        final Latch latch = latchFactory.newReadWriteUpdateLatch();
        latch.updateLock();
        Thread t = new Thread(new Runnable() {
            public void run() {
                latch.sharedLock();
                latch.unlockShared();
            }
        });
        t.start();
        Thread.sleep(100);
        assertTrue(latch.isLatchedForUpdate());
        latch.unlockUpdate();
        t.join(300);
        assertTrue(!t.isAlive());
    }

    /**
     * write tryLock fails when readlocked
     */
    public void testUpdateTryLockWhenWriteLocked() throws Exception {
        final Latch latch = latchFactory.newReadWriteUpdateLatch();
        latch.exclusiveLock();
        testfailed = false;
        Thread t = new Thread(new Runnable() {
            public void run() {
                if (latch.tryUpdateLock()) {
                    testfailed = true;
                }
            }
        });
        t.start();
        t.join();
        latch.unlockExclusive();

        assertFalse(testfailed);
    }

    /**
     * write tryLock fails when readlocked
     */
    public void testWriteTryLockWhenReadLocked() throws Exception {
        final Latch latch = latchFactory.newReadWriteUpdateLatch();
        latch.sharedLock();
        testfailed = false;
        Thread t = new Thread(new Runnable() {
            public void run() {
                if (latch.tryExclusiveLock()) {
                    testfailed = true;
                }
            }
        });
        t.start();
        t.join();
        latch.unlockShared();

        assertFalse(testfailed);
    }

    public void testReentrantWriteLock() throws Exception {
        final Latch lock = latchFactory.newReadWriteUpdateLatch();
        lock.exclusiveLock();
        assertTrue(lock.tryExclusiveLock());
        lock.unlockExclusive();
        assertTrue(lock.isLatchedExclusively());
        lock.unlockExclusive();
        assertFalse(lock.isLatchedExclusively());
    }

    /**
     * Read lock succeeds if write locked by current thread even if other
     * threads are waiting for readlock
     */
    public void testReadHoldingWriteLock2() throws Exception {
        final Latch lock = latchFactory.newReadWriteUpdateLatch();
        lock.exclusiveLock();
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                lock.sharedLock();
                lock.unlockShared();
            }
        });
        Thread t2 = new Thread(new Runnable() {
            public void run() {
                lock.sharedLock();
                lock.unlockShared();
            }
        });

        t1.start();
        t2.start();
        lock.sharedLock();
        lock.unlockShared();
        Thread.sleep(100);
        lock.sharedLock();
        lock.unlockShared();
        lock.unlockExclusive();
        t1.join(200);
        t2.join(200);
        assertTrue(!t1.isAlive());
        assertTrue(!t2.isAlive());
    }

    /**
     * Read lock succeeds if write locked by current thread even if other
     * threads are waiting for writelock
     */
    public void testReadHoldingWriteLock3() throws Exception {
        final Latch lock = latchFactory.newReadWriteUpdateLatch();
        lock.exclusiveLock();
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                lock.updateLock();
                lock.unlockUpdate();
            }
        });
        Thread t2 = new Thread(new Runnable() {
            public void run() {
                lock.exclusiveLock();
                lock.unlockUpdate();
            }
        });

        t1.start();
        t2.start();
        lock.sharedLock();
        lock.unlockShared();
        Thread.sleep(100);
        lock.sharedLock();
        lock.unlockShared();
        lock.unlockExclusive();
        t1.join(200);
        t2.join(200);
        assertTrue(!t1.isAlive());
        assertTrue(!t2.isAlive());
    }

    /**
     * Write lock succeeds if write locked by current thread even if other
     * threads are waiting for writelock
     */
    public void testWriteHoldingWriteLock4() throws Exception {
        final Latch lock = latchFactory.newReadWriteUpdateLatch();
        lock.exclusiveLock();
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                lock.exclusiveLock();
                lock.unlockExclusive();
            }
        });
        Thread t2 = new Thread(new Runnable() {
            public void run() {
                lock.exclusiveLock();
                lock.unlockExclusive();
            }
        });

        t1.start();
        t2.start();
        lock.exclusiveLock();
        lock.unlockExclusive();
        Thread.sleep(100);
        lock.exclusiveLock();
        lock.unlockExclusive();
        lock.unlockExclusive();
        t1.join(200);
        t2.join(200);
        assertTrue(!t1.isAlive());
        assertTrue(!t2.isAlive());
    }

//    /**
//     * write lockInterruptibly succeeds if lock free else is interruptible
//     */
//    public void testWriteLockInterruptibly() throws Exception {
//        final ReadWriteUpdateLatch lock = (ReadWriteUpdateLatch) latchFactory
//                .newReadWriteUpdateLatch();
//        lock.exclusiveLock();
//        testfailed = false;
//        Thread t = new Thread(new Runnable() {
//            public void run() {
//                try {
//                    lock.exclusiveLockInterruptibly();
//                    testfailed = true;
//                } catch (InterruptedException success) {
//                }
//            }
//        });
//        t.start();
//        t.interrupt();
//        t.join();
//        lock.unlock();
//
//        assertFalse(testfailed);
//    }
//
//    /**
//     * read lockInterruptibly succeeds if lock free else is interruptible
//     */
//    public void testReadLockInterruptibly() throws Exception {
//        final ReadWriteUpdateLatch lock = (ReadWriteUpdateLatch) latchFactory
//                .newReadWriteUpdateLatch();
//        lock.exclusiveLock();
//        testfailed = false;
//        Thread t = new Thread(new Runnable() {
//            public void run() {
//                try {
//                    lock.sharedLockInterruptibly();
//                    testfailed = true;
//                } catch (InterruptedException success) {
//                }
//            }
//        });
//        t.start();
//        t.interrupt();
//        t.join();
//        lock.unlock();
//
//        assertFalse(testfailed);
//    }

    static final class Incrementor implements Runnable {

        final Latch latch;
        static int number = 0;

        Incrementor(Latch latch) {
            this.latch = latch;
        }

        public void run() {
            for (int i = 0; i < 100000; i++) {
                latch.exclusiveLock();
                number++;
                latch.unlockExclusive();
            }
        }
    }

    public void testPerformance() throws Exception {
        final Latch latch = latchFactory.newReadWriteUpdateLatch();
        Thread threads[] = new Thread[8];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Incrementor(latch));
        }
        latch.sharedLock();
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        long t1 = System.currentTimeMillis();
        latch.unlockShared();
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        long t2 = System.currentTimeMillis();
        System.err
                .println("Time taken by 8 threads to update number 100000 times each = "
                        + (t2 - t1) + " millisecs");
        assertEquals(Incrementor.number, 800000);
    }

}
