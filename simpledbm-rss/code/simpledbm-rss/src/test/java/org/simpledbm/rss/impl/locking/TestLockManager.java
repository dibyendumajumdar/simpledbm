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
package org.simpledbm.rss.impl.locking;

import org.simpledbm.common.api.locking.LockMode;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.impl.platform.PlatformImpl;
import org.simpledbm.junit.SimpleDBMTestCase;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.locking.LockDeadlockException;
import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockException;
import org.simpledbm.rss.api.locking.LockManager;
import org.simpledbm.rss.impl.latch.LatchFactoryImpl;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Test cases for Lock Manager module.
 * 
 * @author Dibyendu Majumdar
 * @since 21-Aug-2005
 */
public class TestLockManager extends SimpleDBMTestCase {

    public TestLockManager(String arg0) {
        super(arg0);
    }

    volatile int countDeadlocks = 0;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        countDeadlocks = 0;
    }

    /**
     * Tests that SHARED lock requests block if EXCLUSIVE lock is held, and are
     * granted once the EXCLUSIVE lock is released.
     */
    public void testReadAfterWriteLock() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object tran3 = new Integer(3);
        final Object lockname = new Integer(10);
        final AtomicInteger counter = new AtomicInteger(0);

        // Obtain an exclusive lock
        lockmgr.acquire(tran1, lockname, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);

        Thread t1 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1,
                        lockname));
                // We expect this lock to be granted after the exclusive lock is released
                lockmgr.acquire(tran2, lockname,
                        LockMode.SHARED, LockDuration.MANUAL_DURATION, -1
                );
                assertEquals(LockMode.SHARED, lockmgr.findLock(tran2,
                        lockname));
                assertEquals(LockMode.NONE, lockmgr.findLock(tran1,
                        lockname));
                lockmgr.release(tran2, lockname, false);
                counter.incrementAndGet();
            }
        }));
        Thread t2 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1,
                        lockname));
                // We expect this lock to be granted after the exclusive lock is released
                lockmgr.acquire(tran3, lockname,
                        LockMode.SHARED, LockDuration.MANUAL_DURATION, -1);
                assertEquals(LockMode.SHARED, lockmgr.findLock(tran3,
                        lockname));
                assertEquals(LockMode.NONE, lockmgr.findLock(tran1,
                        lockname));
                lockmgr.release(tran3, lockname, false);
                counter.incrementAndGet();
            }
        }));

        try {
            t1.start();
            t2.start();
            // Let the threads attempt to acquire shared locks
            Thread.sleep(500);
            // We still hold the exclusive lock
            assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname));
            // Release the exclusive lock which should grant the shared lock requests
            lockmgr.release(tran1, lockname, false);
            t1.join(1000);
            t2.join(1000);
            assertTrue(!t1.isAlive());
            assertTrue(!t2.isAlive());
            assertTrue(counter.get() == 2);
        } catch (InterruptedException e) {
            fail();
        }
    }

    /**
     * A type of lockable object that has a constant hash code value.
     * 
     * @author Dibyendu Majumdar
     */
    static final class ConstantHashingObject {
        final int i;

        public ConstantHashingObject(int i) {
            this.i = i;
        }

        @Override
        public int hashCode() {
            // Always return the same hash value
            return 10;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return i == ((ConstantHashingObject) o).i;
        }
    }

    /**
     * Stresses concurrent access to same hash bucket. Two threads are started,
     * both of which attempt a large number of locks, all using the same hash
     * bucket.
     */
    public void testConcurrentHashBucket() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object lockname1 = new ConstantHashingObject(10);
        final Object lockname2 = new ConstantHashingObject(20);
        final AtomicInteger counter = new AtomicInteger(0);

        Thread t1 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                for (int i = 0; i < 5000; i++) {
                    lockmgr.acquire(tran1, lockname1,
                            LockMode.EXCLUSIVE,
                            LockDuration.MANUAL_DURATION, -1);
                    assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(
                            tran1, lockname1));
                    lockmgr.release(tran1, lockname1, false);
                    assertEquals(LockMode.NONE, lockmgr.findLock(tran1,
                            lockname1));
                    counter.incrementAndGet();
                }
            }
        }));
        Thread t2 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                for (int i = 0; i < 5000; i++) {
                    lockmgr.acquire(tran2, lockname2,
                            LockMode.EXCLUSIVE,
                            LockDuration.MANUAL_DURATION, -1);
                    assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(
                            tran2, lockname2));
                    lockmgr.release(tran2, lockname2, false);
                    assertEquals(LockMode.NONE, lockmgr.findLock(tran2,
                            lockname2));
                    counter.incrementAndGet();
                }
            }
        }));

        t1.start();
        t2.start();
        t1.join(10000);
        t2.join(10000);
        assertTrue(!t1.isAlive());
        assertTrue(!t2.isAlive());
        assertTrue(counter.get() == 10000);
    }

    /**
     * Stresses concurrent access to same hash bucket. Two threads are started,
     * both of which attempt a large number of locks, all using the same hash
     * bucket.
     */
    public void testStress3T() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object tran3 = new Integer(3);
        final Object lockname1 = new ConstantHashingObject(10);
        //final AtomicInteger counter = new AtomicInteger(0);
        final int N = 100000;
        final LinkedBlockingQueue<Integer> q = new LinkedBlockingQueue<Integer>();
        final CountDownLatch go = new CountDownLatch(1);
        final CountDownLatch ready = new CountDownLatch(3);

        // first warmup
        for (int i = 0; i < N; i++) {
            lockmgr.acquire(tran1, lockname1,
                    LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
            // assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname1));
            lockmgr.release(tran1, lockname1, false);
        }

        Thread t1 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e1) {
                }
                for (int i = 0; i < N; i++) {
                    lockmgr.acquire(tran1, lockname1,
                            LockMode.EXCLUSIVE,
                            LockDuration.MANUAL_DURATION, -1);
//                        assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(
//                                tran1, lockname1));
                    lockmgr.release(tran1, lockname1, false);
//                        assertEquals(LockMode.NONE, lockmgr.findLock(tran1,
//                                lockname1));
//                        counter.incrementAndGet();
                }
                q.offer(1);
            }
        }));
        Thread t2 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e1) {
                }
                for (int i = 0; i < N; i++) {
                    lockmgr.acquire(tran2, lockname1,
                            LockMode.EXCLUSIVE,
                            LockDuration.MANUAL_DURATION, -1);
//                        assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(
//                                tran2, lockname2));
                    lockmgr.release(tran2, lockname1, false);
//                        assertEquals(LockMode.NONE, lockmgr.findLock(tran2,
//                                lockname2));
//                        counter.incrementAndGet();
                }
                q.offer(1);
            }
        }));
        Thread t3 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                for (int i = 0; i < N; i++) {
                    lockmgr.acquire(tran3, lockname1,
                            LockMode.EXCLUSIVE,
                            LockDuration.MANUAL_DURATION, -1);
//                        assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(
//                                tran2, lockname2));
                    lockmgr.release(tran3, lockname1, false);
//                        assertEquals(LockMode.NONE, lockmgr.findLock(tran2,
//                                lockname2));
//                        counter.incrementAndGet();
                }
                q.offer(1);
            }
        }));


        t1.start();
        t2.start();
        t3.start();
        ready.await();
        long s1 = System.nanoTime();
        go.countDown();
        q.take();
        q.take();
        q.take();
        long s2 = System.nanoTime();
        System.err.println(N + "   testStress3T = " + ((s2 - s1) / N) + " ns/op");
        t1.join(10000);
        t2.join(10000);
        t3.join(10000);
        assertTrue(!t1.isAlive());
        assertTrue(!t2.isAlive());
        assertTrue(!t3.isAlive());
//        assertTrue(counter.get() == 10000);
    }

    /**
     * Stresses concurrent access to same hash bucket. Two threads are started,
     * both of which attempt a large number of locks, all using the same hash
     * bucket.
     */
    public void testStress2T() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object lockname1 = new ConstantHashingObject(10);
        //final AtomicInteger counter = new AtomicInteger(0);
        final int N = 100000;
        final LinkedBlockingQueue<Integer> q = new LinkedBlockingQueue<Integer>();
        final CountDownLatch go = new CountDownLatch(1);
        final CountDownLatch ready = new CountDownLatch(2);

        // first warmup
        for (int i = 0; i < N; i++) {
            lockmgr.acquire(tran1, lockname1,
                    LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
            // assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname1));
            lockmgr.release(tran1, lockname1, false);
        }

        Thread t1 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e1) {
                }
                for (int i = 0; i < N; i++) {
                    lockmgr.acquire(tran1, lockname1,
                            LockMode.EXCLUSIVE,
                            LockDuration.MANUAL_DURATION, -1);
//                        assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(
//                                tran1, lockname1));
                    lockmgr.release(tran1, lockname1, false);
//                        assertEquals(LockMode.NONE, lockmgr.findLock(tran1,
//                                lockname1));
//                        counter.incrementAndGet();
                }
                q.offer(1);
            }
        }));
        Thread t2 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                ready.countDown();
                try {
                    go.await();
                } catch (InterruptedException e1) {
                }
                for (int i = 0; i < N; i++) {
                    lockmgr.acquire(tran2, lockname1,
                            LockMode.EXCLUSIVE,
                            LockDuration.MANUAL_DURATION, -1);
//                        assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(
//                                tran2, lockname2));
                    lockmgr.release(tran2, lockname1, false);
//                        assertEquals(LockMode.NONE, lockmgr.findLock(tran2,
//                                lockname2));
//                        counter.incrementAndGet();
                }
                q.offer(1);
            }
        }));


        t1.start();
        t2.start();
        ready.await();
        long s1 = System.nanoTime();
        go.countDown();
        q.take();
        q.take();
        long s2 = System.nanoTime();
        System.err.println(N + "   testStress2T = " + ((s2 - s1)/N) + " ns/op");
        t1.join(10000);
        t2.join(10000);
        assertTrue(!t1.isAlive());
        assertTrue(!t2.isAlive());
    }

    public void testStressNoContention() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object lockname1 = new ConstantHashingObject(10);
        //final AtomicInteger counter = new AtomicInteger(0);
        final int N = 1000000;

        // first warmup
        for (int i = 0; i < N; i++) {
            lockmgr.acquire(tran1, lockname1,
                    LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
            // assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname1));
            lockmgr.release(tran1, lockname1, false);
        }

        long s1 = System.nanoTime();
        for (int i = 0; i < N; i++) {
            lockmgr.acquire(tran1, lockname1,
                    LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
            // assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname1));
            lockmgr.release(tran1, lockname1, false);
        }
        long s2 = System.nanoTime();
        System.err.println(N + "   testStress1T = " + ((s2 - s1)/N) + " ns/op");
    }


    /**
     * Simple lock acquire and release tests
     */
    public void testLocks() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object lockname = new Integer(10);

        lockmgr.acquire(tran1, lockname,
                LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
        assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname));
        assertEquals(1, lockmgr.getLocks(tran1, null).length);
        lockmgr.release(tran1, lockname, false);
        assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
        lockmgr.acquire(tran1, lockname, LockMode.SHARED,
                LockDuration.MANUAL_DURATION, -1);
        assertEquals(LockMode.SHARED, lockmgr.findLock(tran1, lockname));
        lockmgr.release(tran1, lockname, false);
        assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
        lockmgr.acquire(tran1, lockname, LockMode.INTENTION_EXCLUSIVE,
                LockDuration.MANUAL_DURATION, -1);
        assertEquals(LockMode.INTENTION_EXCLUSIVE, lockmgr.findLock(tran1,
                lockname));
        lockmgr.release(tran1, lockname, false);
        assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
        lockmgr.acquire(tran1, lockname, LockMode.INTENTION_SHARED,
                LockDuration.MANUAL_DURATION, -1);
        assertEquals(LockMode.INTENTION_SHARED, lockmgr.findLock(tran1,
                lockname));
        lockmgr.release(tran1, lockname, false);
        assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
        lockmgr.acquire(tran1, lockname,
                LockMode.SHARED_INTENTION_EXCLUSIVE,
                LockDuration.MANUAL_DURATION, -1);
        assertEquals(LockMode.SHARED_INTENTION_EXCLUSIVE, lockmgr.findLock(
                tran1, lockname));
        lockmgr.release(tran1, lockname, false);
        assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
        lockmgr.acquire(tran1, lockname, LockMode.UPDATE,
                LockDuration.MANUAL_DURATION, -1);
        assertEquals(LockMode.UPDATE, lockmgr.findLock(tran1, lockname));
        lockmgr.release(tran1, lockname, false);
        assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
        lockmgr.acquire(tran1, lockname, LockMode.UPDATE,
                LockDuration.MANUAL_DURATION, -1);
        assertEquals(LockMode.UPDATE, lockmgr.findLock(tran1, lockname));
        lockmgr.acquire(tran1, lockname, LockMode.UPDATE,
                LockDuration.COMMIT_DURATION, -1);
        assertEquals(LockMode.UPDATE, lockmgr.findLock(tran1, lockname));
        assertFalse(lockmgr.release(tran1, lockname, false));
        assertEquals(LockMode.UPDATE, lockmgr.findLock(tran1, lockname));
        lockmgr.acquire(tran1, lockname, LockMode.EXCLUSIVE,
                LockDuration.MANUAL_DURATION, -1);
        assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname));
        assertFalse(lockmgr.release(tran1, lockname, false));
        assertFalse(lockmgr.release(tran1, lockname, false));
        assertFalse(lockmgr.release(tran1, lockname, false));
        assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname));
        assertTrue(lockmgr.release(tran1, lockname, true));
    }

    /**
     * @see TestLockManager#testLocking()
     */
    final class Lock1 implements Runnable {

        private final AtomicInteger sync;

        private final Object tran;

        private final Object lockname;

        private final LockManager lockMgr;

        public Lock1(AtomicInteger sync, Object tran, Object lockname,
                LockManager lockMgr) {
            this.tran = tran;
            this.lockname = lockname;
            this.lockMgr = lockMgr;
            this.sync = sync;
        }

        public void run() {

            System.err.println("T1(1) locking " + lockname
                    + " in shared mode");
            lockMgr
                    .acquire(tran, lockname, LockMode.SHARED,
                            LockDuration.MANUAL_DURATION, -1);
            System.err.println("T1(2) acquired lock " + lockname);
            assertTrue(sync.compareAndSet(0, 2));
            try {
                // System.err.println("T1 sleeping for 5 seconds");
                // Wait until threads 2 and 3 start
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.err
                    .println("T1(6) locking "
                            + lockname
                            + " in exclusive mode (should trigger conversion request and block) ...");
            lockMgr.acquire(tran, lockname,
                    LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
            System.err.println("T1(8) lock acquired " + lockname);
            assertTrue(sync.compareAndSet(4, 8));
            try {
                //System.err.println("T1 sleeping for 5 seconds");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.err.println("T1(10) downgrading to shared mode");
            lockMgr.downgrade(tran, lockname, LockMode.SHARED);
            System.err.println("T1(11) releasing lock");
            lockMgr.release(tran, lockname, false);
            System.err
                    .println("T1(12) releasing lock (should grant exclusive to T3)");
            lockMgr.release(tran, lockname, false);
        }
    }

    /**
     * @see TestLockManager#testLocking()
     */
    final class Lock2 implements Runnable {

        private final AtomicInteger sync;

        private final Object tran;

        private final Object lockname;

        private final LockManager lockMgr;

        public Lock2(AtomicInteger sync, Object tran1, Object lockname,
                LockManager lockMgr) {
            this.tran = tran1;
            this.lockname = lockname;
            this.lockMgr = lockMgr;
            this.sync = sync;
        }

        public void run() {

            System.err.println("T2(3) locking " + lockname
                    + " in shared mode");
            lockMgr
                    .acquire(tran, lockname, LockMode.SHARED,
                            LockDuration.MANUAL_DURATION, -1);
            System.err.println("T2(4) acquired lock " + lockname);
            assertTrue(sync.compareAndSet(2, 4));
            try {
                // System.err.println("T2 sleeping for 25 seconds");
                // Wait for thread 3 to start and thread 1 to block
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.err
                    .println("T2(7) releasing lock (should grant conversion request T1)");
            lockMgr.release(tran, lockname, false);
            try {
                // System.err.println("T2 sleeping for 1 second");
                // wait for thread 1 to get conversion request
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.err.println("T2(9) locking " + lockname
                    + " in shared mode (should block) ...");
            lockMgr.acquire(tran, lockname, LockMode.SHARED,
                    LockDuration.MANUAL_DURATION, -1);
            System.err.println("T2(15) acquired lock" + lockname);
            assertTrue(sync.compareAndSet(13, 16));
            System.err.println("T2(16) releasing lock");
            lockMgr.release(tran, lockname, false);
        }
    }

    /**
     * @see TestLockManager#testLocking()
     */
    final class Lock3 implements Runnable {

        private final AtomicInteger sync;

        private final Object tran;

        private final Object lockname;

        private final LockManager lockMgr;

        public Lock3(AtomicInteger sync, Object tran, Object lockname,
                     LockManager lockMgr) {
            this.tran = tran;
            this.lockname = lockname;
            this.lockMgr = lockMgr;
            this.sync = sync;
        }

        public void run() {

            System.err.println("T3(5) locking " + lockname
                    + " in exclusive mode (should wait) ...");
            lockMgr.acquire(tran, lockname,
                    LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
            System.err.println("T3(13) acquired lock " + lockname);
            assertTrue(sync.compareAndSet(8, 13));
            System.err
                    .println("T3(14) releasing lock (should grant shared lock to T2)");
            lockMgr.release(tran, lockname, false);
        }
    }

    /**
     * A complex test case where various interactions between three threads are
     * tested. The thread interactions are defined in terms of a state machine
     * which is validated.
     *
     * @see TestLockManager.Lock1
     * @see TestLockManager.Lock2
     * @see TestLockManager.Lock3
     */
    public void testLocking() throws Exception {

        ThreadFactory factory = Executors.defaultThreadFactory();

        LockManager lockmgr = createLockManager();

        Object tran1 = new Integer(1);
        Object tran2 = new Integer(2);
        Object tran3 = new Integer(3);
        Object lockname = new Integer(10);
        AtomicInteger sync = new AtomicInteger(0);

        Thread locker1 = factory.newThread(getTestRunnable(new Lock1(sync, tran1, lockname,
                lockmgr)));
        Thread locker2 = factory.newThread(getTestRunnable(new Lock2(sync, tran2, lockname,
                lockmgr)));
        Thread locker3 = factory.newThread(getTestRunnable(new Lock3(sync, tran3, lockname,
                lockmgr)));
        locker1.start();
        Thread.sleep(500);
        locker2.start();
        Thread.sleep(500);
        locker3.start();
        locker1.join();
        locker2.join();
        locker3.join();
        assertEquals(16, sync.get());
    }

    /**
     * Tests lock downgrade. A thread acquires an UPDATE lock. Two other threads
     * get blocked on SHARED lock request. The first thread downgrades the
     * UPDATE lock to SHARED lock, at which point the two blocked threads obtain
     * the lock and get unblocked.
     */
    public void testLockDowngrade() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object tran3 = new Integer(3);
        final Object lockname = new Integer(10);
        final AtomicInteger counter = new AtomicInteger(0);

        lockmgr.acquire(tran1, lockname, LockMode.UPDATE,
                LockDuration.MANUAL_DURATION, -1);

        Thread t1 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                try {
                    lockmgr.acquire(tran2, lockname,
                            LockMode.SHARED, LockDuration.MANUAL_DURATION, -1);
                    lockmgr.release(tran2, lockname, false);
                    counter.incrementAndGet();
                } catch (LockException e) {
                    e.printStackTrace();
                }
            }
        }));
        Thread t2 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                try {
                    lockmgr.acquire(tran3, lockname,
                            LockMode.SHARED, LockDuration.MANUAL_DURATION, -1);
                    lockmgr.release(tran3, lockname, false);
                    counter.incrementAndGet();
                } catch (LockException e) {
                    e.printStackTrace();
                }
            }
        }));

        try {
            t1.start();
            t2.start();
            Thread.sleep(500);
            lockmgr.downgrade(tran1, lockname, LockMode.SHARED);
            t1.join(1000);
            t2.join(1000);
            assertTrue(!t1.isAlive());
            assertTrue(!t2.isAlive());
            assertTrue(counter.get() == 2);
            boolean result = lockmgr.release(tran1, lockname, false);
            assertTrue(result);
        } catch (Exception e) {
            fail();
        }

    }

    /**
     * This test verifies that invalid downgrade attempts are rejected.
     */
    public void testInvalidDowngrade() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object lockname = new Integer(10);

        lockmgr.acquire(tran1, lockname, LockMode.UPDATE,
                LockDuration.MANUAL_DURATION, -1);
        try {
            lockmgr.downgrade(tran1, lockname, LockMode.EXCLUSIVE);
            fail();
        } catch (LockException e) {
            assertTrue(true);
        }
        assertTrue(lockmgr.release(tran1, lockname, false));
    }

    /**
     * This test verifies that an INSTANT_DURATION lock works correctly when it
     * is necessary to wait for the lock to become available. Also, it tests
     * that a lock downgrade leads to eligible waiting requests being granted
     * locks. NOTE: This test case works when SHARED mode is not compatible with
     * UPDATE mode.
     */
    public void disabledTestInstantLockWait() throws Exception {
        // This test has been disabled because of the change
        // to lock compaibility - SHARED LOCKs are now compatible with
        // UPDATE locks.
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object lockname = new Integer(10);
        final AtomicInteger counter = new AtomicInteger(0);

        lockmgr.acquire(tran1, lockname, LockMode.UPDATE,
                LockDuration.MANUAL_DURATION, -1);

        Thread t1 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                try {
                    lockmgr.acquire(tran2, lockname, LockMode.SHARED,
                            LockDuration.INSTANT_DURATION, -1);
                    assertEquals(LockMode.NONE, lockmgr.findLock(tran2,
                            lockname));
                    assertTrue(counter.get() == 1);
                } catch (LockException e) {
                    counter.incrementAndGet();
                    e.printStackTrace();
                }
            }
        }));

        t1.start();
        Thread.sleep(500);
        counter.incrementAndGet();
        lockmgr.downgrade(tran1, lockname, LockMode.SHARED);
        t1.join(1000);
        assertTrue(!t1.isAlive());
        assertTrue(counter.get() == 1);
        assertTrue(lockmgr.release(tran1, lockname, false));
    }

    /**
     * This test verifies that an INSTANT_DURATION lock works correctly when it
     * is necessary to wait for the lock to become available. Also, it tests
     * that a lock downgrade leads to eligible waiting requests being granted
     * locks. NOTE: This test case works when SHARED mode IS compatible with
     * UPDATE mode.
     */
    public void testInstantLockWait2() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object lockname = new Integer(10);
        final AtomicInteger counter = new AtomicInteger(0);

        lockmgr.acquire(tran1, lockname, LockMode.UPDATE,
                LockDuration.MANUAL_DURATION, -1);

        Thread t1 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                try {
                    lockmgr.acquire(tran2, lockname, LockMode.SHARED,
                            LockDuration.INSTANT_DURATION, -1);
                    assertEquals(LockMode.NONE, lockmgr.findLock(tran2,
                            lockname));
                    assertTrue(counter.get() == 0);
                } catch (LockException e) {
                    counter.incrementAndGet();
                    e.printStackTrace();
                }
            }
        }));

        t1.start();
        Thread.sleep(500);
        counter.incrementAndGet();
        lockmgr.downgrade(tran1, lockname, LockMode.SHARED);
        t1.join(1000);
        assertTrue(!t1.isAlive());
        assertTrue(counter.get() == 1);
        assertTrue(lockmgr.release(tran1, lockname, false));
    }

    /**
     * This test verifies that an incompatible lock request of INSTANT_DURATION
     * fails if nowait is true.
     */
    public void testInstantLockNowait() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object lockname = new Integer(10);

        lockmgr.acquire(tran1, lockname,
                LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
        try {
            lockmgr.acquire(tran2, lockname, LockMode.SHARED,
                    LockDuration.INSTANT_DURATION, 0);
            fail();
        } catch (LockException e) {
            assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname));
            assertEquals(LockMode.NONE, lockmgr.findLock(tran2, lockname));
        }
        assertTrue(lockmgr.release(tran1, lockname, false));
    }

    /**
     * This test verifies that if a client already holds a lock, a request for
     * compatible INSTANT_DURATION lock succeeds even when the lock is requested
     * conditionally.
     */
    public void testInstantLockSelf() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object lockname = new Integer(10);

        lockmgr.acquire(tran1, lockname,
                LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
        try {
            lockmgr.acquire(tran1, lockname, LockMode.SHARED,
                    LockDuration.INSTANT_DURATION, 0);
        } catch (LockException e) {
            fail(e.getMessage());
        }
        assertTrue(lockmgr.release(tran1, lockname, false));
    }

    /**
     * This test verifies that if an conversion lock request is asked for as
     * INSTANT_DURATION, then it is not actually granted. The caller is delayed
     * until the lock becomes available, but the lock remains in the previous
     * mode.
     */
    public void testInstantLockConversionWait() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object lockname = new Integer(10);
        final AtomicInteger counter = new AtomicInteger(0);

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    lockmgr.acquire(tran2, lockname,
                            LockMode.SHARED, LockDuration.MANUAL_DURATION, -1);
                    Thread.sleep(500);
                    lockmgr.acquire(tran2, lockname,
                            LockMode.EXCLUSIVE, LockDuration.INSTANT_DURATION,
                            -1);
                    assertTrue(lockmgr.findLock(tran2, lockname) == LockMode.SHARED);
                    assertTrue(lockmgr.release(tran2, lockname, false));
                } catch (Exception e) {
                    counter.incrementAndGet();
                    e.printStackTrace();
                }
            }
        });

        t1.start();
        Thread.sleep(200);
        lockmgr.acquire(tran1, lockname, LockMode.UPDATE,
                LockDuration.MANUAL_DURATION, -1);
        Thread.sleep(1000);
        assertEquals(LockMode.UPDATE, lockmgr.findLock(tran1, lockname));
        assertEquals(LockMode.SHARED, lockmgr.findLock(tran2, lockname));
        assertTrue(lockmgr.release(tran1, lockname, false));
        t1.join(5000);
        assertTrue(!t1.isAlive());
        assertTrue(counter.get() == 0);
    }

    /**
     * This test verifies that if an conversion lock request is asked for
     * COMMIT_DURATION, then the lock request gets updated to commit duration.
     */
    public void testCommitLockConversionWait() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object lockname = new Integer(10);
        final AtomicInteger counter = new AtomicInteger(0);

        Thread t1 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                try {
                    lockmgr.acquire(tran2, lockname,
                            LockMode.SHARED, LockDuration.MANUAL_DURATION, -1);
                    Thread.sleep(500);
                    lockmgr.acquire(tran2, lockname,
                            LockMode.EXCLUSIVE, LockDuration.COMMIT_DURATION,
                            -1);
                    assertTrue(lockmgr.findLock(tran2, lockname) == LockMode.EXCLUSIVE);
                    assertFalse(lockmgr.release(tran2, lockname, false));
                    assertTrue(lockmgr.release(tran2, lockname, true));
                } catch (Exception e) {
                    counter.incrementAndGet();
                    e.printStackTrace();
                }
            }
        }));

        t1.start();
        Thread.sleep(200);
        lockmgr.acquire(tran1, lockname, LockMode.UPDATE,
                LockDuration.MANUAL_DURATION, -1);
        Thread.sleep(1000);
        assertEquals(LockMode.UPDATE, lockmgr.findLock(tran1, lockname));
        assertEquals(LockMode.SHARED, lockmgr.findLock(tran2, lockname));
        assertTrue(lockmgr.release(tran1, lockname, false));
        t1.join(5000);
        assertTrue(!t1.isAlive());
        assertTrue(counter.get() == 0);
    }

    /**
     * In this test, a deadlock is produced by two threads acquiring exclusive
     * locks A and B repectively, and then each thread attempting to acquire an
     * exclusive lock on B and A respectively.
     */
    public void testDeadlockDetection() throws Exception {
        final LockManagerImpl lockmgr = (LockManagerImpl) createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object lockname1 = new Integer(10);
        final Object lockname2 = new Integer(11);

        Thread t1 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                try {
                    lockmgr.acquire(tran1, lockname1,
                            LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION,
                            5);
                    try {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                        }
                        lockmgr.acquire(tran1, lockname2,
                                LockMode.EXCLUSIVE,
                                LockDuration.MANUAL_DURATION, -1);
                        lockmgr.release(tran1, lockname2, false);
                    } finally {
                        lockmgr.release(tran1, lockname1, false);
                    }
                } catch (LockDeadlockException e) {
                    // System.err.println("T1 failed with " + e);
                    // e.printStackTrace(System.out);
                    countDeadlocks++;
                }

            }
        }));
        Thread t2 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                try {
                    lockmgr.acquire(tran2, lockname2,
                            LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION,
                            5);
                    try {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }
                        lockmgr.acquire(tran2, lockname1,
                                LockMode.EXCLUSIVE,
                                LockDuration.MANUAL_DURATION, -1);
                        lockmgr.release(tran2, lockname1, false);
                    } finally {
                        lockmgr.release(tran2, lockname2, false);
                    }
                } catch (LockDeadlockException e) {
                    // System.err.println("T1 failed with " + e);
                    // e.printStackTrace(System.out);
                    countDeadlocks++;
                }
            }
        }));

        lockmgr.setDeadlockDetectorInterval(1); // set deadlock detection interval to 1 second
        lockmgr.start();
        try {
            t1.start();
            t2.start();
            Thread.sleep(2000);
            //lockmgr.detectDeadlocks();
            t1.join(10000);
            t2.join(10000);
            assertTrue(!t1.isAlive());
            assertTrue(!t2.isAlive());
            assertEquals(1, countDeadlocks);
        } finally {
            lockmgr.shutdown();
        }
    }

    /**
     * This test produces a deadlock situation where one of the requests is a
     * conversion request.
     */
    public void testConversionDeadlockDetection() throws Exception {
        final LockManagerImpl lockmgr = (LockManagerImpl) createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object lockname1 = new Integer(10);

        Thread t1 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                try {
                    lockmgr.acquire(tran1, lockname1,
                            LockMode.SHARED, LockDuration.MANUAL_DURATION, -1);
                    try {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }
                        lockmgr.acquire(tran1, lockname1,
                                LockMode.EXCLUSIVE,
                                LockDuration.MANUAL_DURATION, -1);
                        lockmgr.release(tran1, lockname1, false);
                    } finally {
                        lockmgr.release(tran1, lockname1, false);
                    }
                } catch (LockDeadlockException e) {
                    // System.err.println("T1 failed with " + e);
                    // e.printStackTrace(System.out);
                    countDeadlocks++;
                }
            }
        }));
        Thread t2 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                try {
                    lockmgr.acquire(tran2, lockname1,
                            LockMode.SHARED, LockDuration.MANUAL_DURATION, -1);
                    try {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }
                        lockmgr.acquire(tran2, lockname1,
                                LockMode.EXCLUSIVE,
                                LockDuration.MANUAL_DURATION, -1);
                        lockmgr.release(tran2, lockname1, false);
                    } finally {
                        lockmgr.release(tran2, lockname1, false);
                    }
                } catch (LockDeadlockException e) {
                    // System.err.println("T1 failed with " + e);
                    // e.printStackTrace(System.out);
                    countDeadlocks++;
                }
            }
        }));

        lockmgr.setDeadlockDetectorInterval(1);
        lockmgr.start();
        try {
            t1.start();
            t2.start();
            Thread.sleep(2000);
            // lockmgr.detectDeadlocks();
            t1.join(10000);
            t2.join(10000);
            assertTrue(!t1.isAlive());
            assertTrue(!t2.isAlive());
            assertEquals(1, countDeadlocks);
        } finally {
            lockmgr.shutdown();
        }
    }

    /**
     * This test case produces a multiple deadlock situation.
     */
    public void testMultipleDeadlockDetection() throws Exception {
        final LockManagerImpl lockmgr = (LockManagerImpl) createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object tran3 = new Integer(3);
        final Object lockname1 = new Integer(10);
        final Object lockname2 = new Integer(11);
        final Object lockname3 = new Integer(12);

        Thread t1 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                try {
                    lockmgr.acquire(tran1, lockname1,
                            LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION,
                            10);
                    try {
                        lockmgr.acquire(tran1, lockname2,
                                LockMode.SHARED, LockDuration.MANUAL_DURATION,
                                10);
                        try {
                            try {
                                Thread.sleep(1500);
                            } catch (InterruptedException e) {
                                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                            }
                            lockmgr.acquire(tran1,
                                    lockname3, LockMode.SHARED,
                                    LockDuration.MANUAL_DURATION, -1);
                            lockmgr.release(tran1, lockname3, false);
                        } finally {
                            lockmgr.release(tran1, lockname2, false);
                        }
                    } finally {
                        lockmgr.release(tran1, lockname1, false);
                    }
                } catch (LockDeadlockException e) {
                    // System.err.println("T1 failed with " + e);
                    // e.printStackTrace(System.out);
                    countDeadlocks++;
                }

            }
        }));
        Thread t2 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                try {
                    lockmgr.acquire(tran2, lockname3,
                            LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION,
                            10);
                    try {
                        lockmgr.acquire(tran2, lockname2,
                                LockMode.SHARED, LockDuration.MANUAL_DURATION,
                                10);
                        try {
                            try {
                                Thread.sleep(500);
                            } catch (InterruptedException e) {
                                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                            }
                            lockmgr.acquire(tran2,
                                    lockname2, LockMode.EXCLUSIVE,
                                    LockDuration.MANUAL_DURATION, -1);
                            lockmgr.release(tran1, lockname2, false);
                        } finally {
                            lockmgr.release(tran2, lockname2, false);
                        }
                    } finally {
                        lockmgr.release(tran2, lockname3, false);
                    }
                } catch (LockDeadlockException e) {
                    // System.err.println("T2 failed with " + e);
                    // e.printStackTrace(System.out);
                    countDeadlocks++;
                }
            }
        }));
        @SuppressWarnings("unused")
        Thread t3 = new Thread(getTestRunnable(new Runnable() {
            public void run() {
                try {
                    try {
                        Thread.sleep(1500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                    lockmgr.acquire(tran3, lockname2,
                            LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION,
                            -1);
                    lockmgr.release(tran3, lockname2, false);
                } catch (LockDeadlockException e) {
                    //					System.err.println("T3 failed with " + e);
                    //					e.printStackTrace(System.out);
                    countDeadlocks++;
                }
            }
        }));

        lockmgr.setDeadlockDetectorInterval(1); // set it to detect deadlocks every 1 second
        lockmgr.start();
        try {
            t1.start();
            t2.start();
            // t3.start();
            Thread.sleep(500);
            // lockmgr.detectDeadlocks();
            t1.join(10000);
            t2.join(10000);
            //t3.join(10000);
            assertTrue(!t1.isAlive());
            assertTrue(!t2.isAlive());
            //assertTrue(!t3.isAlive());
            //assertEquals(2, countDeadlocks);
            assertEquals(1, countDeadlocks);
        } finally {
            lockmgr.shutdown();
        }
    }

    /*

    void checkMemoryUsage() throws Exception {

        LockManager lockmgr = ioc.get(LockManager.class);
        Object tran1 = new Integer(1);

        long startingMemoryUse = getUsedMemory();

        System.out.println("Initial memory used = " + startingMemoryUse);
        for (int i = 0; i < 10000; i++) {
            lockmgr.acquire(tran1, new Integer(i), LockMode.EXCLUSIVE,
                    LockDuration.MANUAL_DURATION, 10);
        }
        long endingMemoryUse = getUsedMemory();
        System.out.println("Total memory used for 10000 locks = "
                + (endingMemoryUse - startingMemoryUse));
    }

    static long sizeOf(Class<?> clazz) {
        long size = 0;
        Object[] objects = new Object[10000];
        try {
            @SuppressWarnings("unused")
            Object primer = clazz.newInstance();
            long startingMemoryUse = getUsedMemory();
            for (int i = 0; i < objects.length; i++) {
                objects[i] = clazz.newInstance();
            }
            long endingMemoryUse = getUsedMemory();
            float approxSize = (endingMemoryUse - startingMemoryUse)
                    / (float) objects.length;
            size = Math.round(approxSize);
        } catch (Exception e) {
            System.out.println("WARNING:couldn't instantiate" + clazz);
            e.printStackTrace();
        }
        return size;
    }

    private static long getUsedMemory() {
        gc();
        long totalMemory = Runtime.getRuntime().totalMemory();
        gc();
        long freeMemory = Runtime.getRuntime().freeMemory();
        long usedMemory = totalMemory - freeMemory;
        return usedMemory;
    }

    private static void gc() {
        try {
            System.gc();
            Thread.sleep(500);
            System.runFinalization();
            Thread.sleep(500);
            System.gc();
            Thread.sleep(500);
            System.runFinalization();
            Thread.sleep(500);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    */

    private LockManager createLockManager() {
        Properties properties = new Properties();
        properties.setProperty("logging.properties.file",
                "classpath:simpledbm.logging.properties");
        properties.setProperty("logging.properties.type", "log4j");
        Platform platform = new PlatformImpl(properties);
        LatchFactory latchFactory = new LatchFactoryImpl(platform, properties);
        LockManager lockmgr = new LockManagerImpl(platform
                .getPlatformObjects(LockManager.LOGGER_NAME), latchFactory,
                properties);
        return lockmgr;
    }

}
