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
package org.simpledbm.rss.impl.locking;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.rss.api.locking.LockDeadlockException;
import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockException;
import org.simpledbm.rss.api.locking.LockHandle;
import org.simpledbm.rss.api.locking.LockManager;
import org.simpledbm.rss.api.locking.LockMode;

/**
 * Test cases for Lock Manager module.
 * 
 * @author Dibyendu Majumdar
 * @since 21-Aug-2005
 */
public class TestLockManager extends BaseTestCase {

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
     * Tests that SHARED lock requests block if EXCLUSIVE lock is held, and are granted
     * once the EXCLUSIVE lock is released.
     */
    public void testReadAfterWriteLock() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object tran3 = new Integer(3);
        final Object lockname = new Integer(10);
        final AtomicInteger counter = new AtomicInteger(0);

        // Obtain an exclusive lock
        LockHandle handle = lockmgr.acquire(
            tran1,
            lockname,
            LockMode.EXCLUSIVE,
            LockDuration.MANUAL_DURATION,
            -1,
            null);

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(
                        tran1,
                        lockname));
                    // We expect this lock to be granted after the exclusive lock is released
                    LockHandle handle1 = lockmgr.acquire(
                        tran2,
                        lockname,
                        LockMode.SHARED,
                        LockDuration.MANUAL_DURATION,
                        -1,
                        null);
                    assertEquals(LockMode.SHARED, lockmgr.findLock(
                        tran2,
                        lockname));
                    assertEquals(LockMode.NONE, lockmgr.findLock(
                        tran1,
                        lockname));
                    handle1.release(false);
                    counter.incrementAndGet();
                } catch (LockException e) {
                    setThreadFailed(Thread.currentThread(), e);
                }
            }
        });
        Thread t2 = new Thread(new Runnable() {
            public void run() {
                try {
                    assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(
                        tran1,
                        lockname));
                    // We expect this lock to be granted after the exclusive lock is released
                    LockHandle handle2 = lockmgr.acquire(
                        tran3,
                        lockname,
                        LockMode.SHARED,
                        LockDuration.MANUAL_DURATION,
                        -1,
                        null);
                    assertEquals(LockMode.SHARED, lockmgr.findLock(
                        tran3,
                        lockname));
                    assertEquals(LockMode.NONE, lockmgr.findLock(
                        tran1,
                        lockname));
                    handle2.release(false);
                    counter.incrementAndGet();
                } catch (LockException e) {
                    setThreadFailed(Thread.currentThread(), e);
                }
            }
        });

        try {
            t1.start();
            t2.start();
            // Let the threads attempt to acquire shared locks
            Thread.sleep(500);
            // We still hold the exclusive lock
            assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname));
            // Release the exclusive lock which should grant the shared lock requests
            handle.release(false);
            t1.join(1000);
            t2.join(1000);
            assertTrue(!t1.isAlive());
            assertTrue(!t2.isAlive());
            assertTrue(counter.get() == 2);
            checkThreadFailures();
        } catch (InterruptedException e) {
            fail();
        }
    }

    /**
     * A type of lockable object that has a constant hash code value.
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
            return i == ((ConstantHashingObject) o).i;
        }
    }

    /**
     * Stresses concurrent access to same hash bucket. Two threads are started,
     * both of which attempt a large number of locks, all using the same hash bucket.
     */
    public void testConcurrentHashBucket() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object lockname1 = new ConstantHashingObject(10);
        final Object lockname2 = new ConstantHashingObject(20);
        final AtomicInteger counter = new AtomicInteger(0);

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    for (int i = 0; i < 5000; i++) {
                        LockHandle handle1 = lockmgr.acquire(
                            tran1,
                            lockname1,
                            LockMode.EXCLUSIVE,
                            LockDuration.MANUAL_DURATION,
                            -1,
                            null);
                        assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(
                            tran1,
                            lockname1));
                        handle1.release(false);
                        assertEquals(LockMode.NONE, lockmgr.findLock(
                            tran1,
                            lockname1));
                        counter.incrementAndGet();
                    }
                } catch (LockException e) {
                    setThreadFailed(Thread.currentThread(), e);
                }
            }
        });
        Thread t2 = new Thread(new Runnable() {
            public void run() {
                try {
                    for (int i = 0; i < 5000; i++) {
                        LockHandle handle2 = lockmgr.acquire(
                            tran2,
                            lockname2,
                            LockMode.EXCLUSIVE,
                            LockDuration.MANUAL_DURATION,
                            -1,
                            null);
                        assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(
                            tran2,
                            lockname2));
                        handle2.release(false);
                        assertEquals(LockMode.NONE, lockmgr.findLock(
                            tran2,
                            lockname2));
                        counter.incrementAndGet();
                    }
                } catch (LockException e) {
                    setThreadFailed(Thread.currentThread(), e);
                }
            }
        });

        t1.start();
        t2.start();
        t1.join(10000);
        t2.join(10000);
        assertTrue(!t1.isAlive());
        assertTrue(!t2.isAlive());
        assertTrue(counter.get() == 10000);
        checkThreadFailures();
    }

    /**
     * Simple lock acquire and release tests
     */
    public void testLocks() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object lockname = new Integer(10);

        LockHandle handle = lockmgr.acquire(
            tran1,
            lockname,
            LockMode.EXCLUSIVE,
            LockDuration.MANUAL_DURATION,
            -1,
            null);
        assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname));
        assertEquals(1, lockmgr.getLocks(tran1, null).length);
        handle.release(false);
        assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
        handle = lockmgr.acquire(
            tran1,
            lockname,
            LockMode.SHARED,
            LockDuration.MANUAL_DURATION,
            -1,
            null);
        assertEquals(LockMode.SHARED, lockmgr.findLock(tran1, lockname));
        handle.release(false);
        assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
        handle = lockmgr.acquire(
            tran1,
            lockname,
            LockMode.INTENTION_EXCLUSIVE,
            LockDuration.MANUAL_DURATION,
            -1,
            null);
        assertEquals(LockMode.INTENTION_EXCLUSIVE, lockmgr.findLock(
            tran1,
            lockname));
        handle.release(false);
        assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
        handle = lockmgr.acquire(
            tran1,
            lockname,
            LockMode.INTENTION_SHARED,
            LockDuration.MANUAL_DURATION,
            -1,
            null);
        assertEquals(LockMode.INTENTION_SHARED, lockmgr.findLock(
            tran1,
            lockname));
        handle.release(false);
        assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
        handle = lockmgr.acquire(
            tran1,
            lockname,
            LockMode.SHARED_INTENTION_EXCLUSIVE,
            LockDuration.MANUAL_DURATION,
            -1,
            null);
        assertEquals(LockMode.SHARED_INTENTION_EXCLUSIVE, lockmgr.findLock(
            tran1,
            lockname));
        handle.release(false);
        assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
        handle = lockmgr.acquire(
            tran1,
            lockname,
            LockMode.UPDATE,
            LockDuration.MANUAL_DURATION,
            -1,
            null);
        assertEquals(LockMode.UPDATE, lockmgr.findLock(tran1, lockname));
        handle.release(false);
        assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
        handle = lockmgr.acquire(
            tran1,
            lockname,
            LockMode.UPDATE,
            LockDuration.MANUAL_DURATION,
            -1,
            null);
        assertEquals(LockMode.UPDATE, lockmgr.findLock(tran1, lockname));
        handle = lockmgr.acquire(
            tran1,
            lockname,
            LockMode.UPDATE,
            LockDuration.COMMIT_DURATION,
            -1,
            null);
        assertEquals(LockMode.UPDATE, lockmgr.findLock(tran1, lockname));
        assertFalse(handle.release(false));
        assertEquals(LockMode.UPDATE, lockmgr.findLock(tran1, lockname));
        handle = lockmgr.acquire(
            tran1,
            lockname,
            LockMode.EXCLUSIVE,
            LockDuration.MANUAL_DURATION,
            -1,
            null);
        assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname));
        assertFalse(handle.release(false));
        assertFalse(handle.release(false));
        assertFalse(handle.release(false));
        assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname));
        assertTrue(handle.release(true));
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

            try {
                System.err.println("T1(1) locking " + lockname
                        + " in shared mode");
                LockHandle handle1 = lockMgr.acquire(
                    tran,
                    lockname,
                    LockMode.SHARED,
                    LockDuration.MANUAL_DURATION,
                    60,
                    null);
                System.err.println("T1(2) acquired lock " + handle1);
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
                LockHandle handle2 = lockMgr.acquire(
                    tran,
                    lockname,
                    LockMode.EXCLUSIVE,
                    LockDuration.MANUAL_DURATION,
                    60,
                    null);
                System.err.println("T1(8) lock acquired " + handle2);
                assertTrue(sync.compareAndSet(4, 8));
                try {
                    //System.err.println("T1 sleeping for 5 seconds");
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.err.println("T1(10) downgrading to shared mode");
                handle2.downgrade(LockMode.SHARED);
                System.err.println("T1(11) releasing lock");
                handle2.release(false);
                System.err
                    .println("T1(12) releasing lock (should grant exclusive to T3)");
                handle1.release(false);
            } catch (LockException e) {
                TestLockManager.this.setThreadFailed(Thread.currentThread(), e);
            }
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

            try {
                System.err.println("T2(3) locking " + lockname
                        + " in shared mode");
                LockHandle handle1 = lockMgr.acquire(
                    tran,
                    lockname,
                    LockMode.SHARED,
                    LockDuration.MANUAL_DURATION,
                    60,
                    null);
                System.err.println("T2(4) acquired lock " + handle1);
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
                handle1.release(false);
                try {
                    // System.err.println("T2 sleeping for 1 second");
                    // wait for thread 1 to get conversion request
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.err.println("T2(9) locking " + lockname
                        + " in shared mode (should block) ...");
                handle1 = lockMgr.acquire(
                    tran,
                    lockname,
                    LockMode.SHARED,
                    LockDuration.MANUAL_DURATION,
                    60,
                    null);
                System.err.println("T2(15) acquired lock" + handle1);
                assertTrue(sync.compareAndSet(13, 16));
                System.err.println("T2(16) releasing lock");
                handle1.release(false);
            } catch (LockException e) {
                TestLockManager.this.setThreadFailed(Thread.currentThread(), e);
            }
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

            try {
                System.err.println("T3(5) locking " + lockname
                        + " in exclusive mode (should wait) ...");
                LockHandle handle = lockMgr.acquire(
                    tran,
                    lockname,
                    LockMode.EXCLUSIVE,
                    LockDuration.MANUAL_DURATION,
                    60,
                    null);
                System.err.println("T3(13) acquired lock " + handle);
                assertTrue(sync.compareAndSet(8, 13));
                System.err
                    .println("T3(14) releasing lock (should grant shared lock to T2)");
                handle.release(false);
            } catch (LockException e) {
                TestLockManager.this.setThreadFailed(Thread.currentThread(), e);
            }
        }
    }

    /**
     * A complex test case where various interactions between three threads are tested.
     * The thread interactions are defined in terms of a state machine which is
     * validated.
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

        Thread locker1 = factory.newThread(new Lock1(
            sync,
            tran1,
            lockname,
            lockmgr));
        Thread locker2 = factory.newThread(new Lock2(
            sync,
            tran2,
            lockname,
            lockmgr));
        Thread locker3 = factory.newThread(new Lock3(
            sync,
            tran3,
            lockname,
            lockmgr));
        locker1.start();
        Thread.sleep(500);
        locker2.start();
        Thread.sleep(500);
        locker3.start();
        locker1.join(10000);
        locker2.join(10000);
        locker3.join(10000);
        assertEquals(16, sync.get());
        checkThreadFailures();
    }

    /**
     * Tests lock downgrade. A thread acquires an UPDATE lock.
     * Two other threads get blocked on SHARED lock request.
     * The first thread downgrades the UPDATE lock to SHARED lock, at which
     * point the two blocked threads obtain the lock and get unblocked.
     */
    public void testLockDowngrade() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object tran3 = new Integer(3);
        final Object lockname = new Integer(10);
        final AtomicInteger counter = new AtomicInteger(0);

        LockHandle handle = lockmgr.acquire(
            tran1,
            lockname,
            LockMode.UPDATE,
            LockDuration.MANUAL_DURATION,
            -1,
            null);

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    LockHandle handle1 = lockmgr.acquire(
                        tran2,
                        lockname,
                        LockMode.SHARED,
                        LockDuration.MANUAL_DURATION,
                        -1,
                        null);
                    handle1.release(false);
                    counter.incrementAndGet();
                } catch (LockException e) {
                    e.printStackTrace();
                }
            }
        });
        Thread t2 = new Thread(new Runnable() {
            public void run() {
                try {
                    LockHandle handle2 = lockmgr.acquire(
                        tran3,
                        lockname,
                        LockMode.SHARED,
                        LockDuration.MANUAL_DURATION,
                        -1,
                        null);
                    handle2.release(false);
                    counter.incrementAndGet();
                } catch (LockException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            t1.start();
            t2.start();
            Thread.sleep(500);
            handle.downgrade(LockMode.SHARED);
            t1.join(1000);
            t2.join(1000);
            assertTrue(!t1.isAlive());
            assertTrue(!t2.isAlive());
            assertTrue(counter.get() == 2);
            boolean result = handle.release(false);
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

        LockHandle handle = lockmgr.acquire(
            tran1,
            lockname,
            LockMode.UPDATE,
            LockDuration.MANUAL_DURATION,
            -1,
            null);
        try {
            handle.downgrade(LockMode.EXCLUSIVE);
            fail();
        } catch (LockException e) {
            assertTrue(true);
        }
        assertTrue(handle.release(false));
    }

    /**
     * This test verifies that an INSTANT_DURATION lock works correctly when it is necessary 
     * to wait for the lock to become available. Also, it tests that a lock downgrade leads 
     * to eligible waiting requests being granted locks.
     * NOTE: This test case works when SHARED mode is not compatible with UPDATE mode.
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

        LockHandle handle = lockmgr.acquire(
            tran1,
            lockname,
            LockMode.UPDATE,
            LockDuration.MANUAL_DURATION,
            -1,
            null);

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    lockmgr.acquire(
                        tran2,
                        lockname,
                        LockMode.SHARED,
                        LockDuration.INSTANT_DURATION,
                        -1,
                        null);
                    assertEquals(LockMode.NONE, lockmgr.findLock(
                        tran2,
                        lockname));
                    assertTrue(counter.get() == 1);
                } catch (LockException e) {
                    counter.incrementAndGet();
                    e.printStackTrace();
                } catch (Throwable e) {
                    setThreadFailed(Thread.currentThread(), e);
                }
            }
        });

        t1.start();
        Thread.sleep(500);
        counter.incrementAndGet();
        handle.downgrade(LockMode.SHARED);
        t1.join(1000);
        checkThreadFailures();
        assertTrue(!t1.isAlive());
        assertTrue(counter.get() == 1);
        assertTrue(handle.release(false));
    }

    /**
     * This test verifies that an INSTANT_DURATION lock works correctly when it is necessary 
     * to wait for the lock to become available. Also, it tests that a lock downgrade leads 
     * to eligible waiting requests being granted locks.
     * NOTE: This test case works when SHARED mode IS compatible with UPDATE mode.
     */
    public void testInstantLockWait2() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object lockname = new Integer(10);
        final AtomicInteger counter = new AtomicInteger(0);

        LockHandle handle = lockmgr.acquire(
            tran1,
            lockname,
            LockMode.UPDATE,
            LockDuration.MANUAL_DURATION,
            -1,
            null);

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    lockmgr.acquire(
                        tran2,
                        lockname,
                        LockMode.SHARED,
                        LockDuration.INSTANT_DURATION,
                        -1,
                        null);
                    assertEquals(LockMode.NONE, lockmgr.findLock(
                        tran2,
                        lockname));
                    assertTrue(counter.get() == 0);
                } catch (LockException e) {
                    counter.incrementAndGet();
                    e.printStackTrace();
                } catch (Throwable e) {
                    setThreadFailed(Thread.currentThread(), e);
                }
            }
        });

        t1.start();
        Thread.sleep(500);
        counter.incrementAndGet();
        handle.downgrade(LockMode.SHARED);
        t1.join(1000);
        checkThreadFailures();
        assertTrue(!t1.isAlive());
        assertTrue(counter.get() == 1);
        assertTrue(handle.release(false));
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

        LockHandle handle = lockmgr.acquire(
            tran1,
            lockname,
            LockMode.EXCLUSIVE,
            LockDuration.MANUAL_DURATION,
            -1,
            null);
        try {
            lockmgr.acquire(
                tran2,
                lockname,
                LockMode.SHARED,
                LockDuration.INSTANT_DURATION,
                0,
                null);
            fail();
        } catch (LockException e) {
            assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname));
            assertEquals(LockMode.NONE, lockmgr.findLock(tran2, lockname));
        }
        assertTrue(handle.release(false));
    }

    /**
     * This test verifies that if a client already holds a lock, a request for compatible INSTANT_DURATION lock
     * succeeds even when the lock is requested conditionally.
     */
    public void testInstantLockSelf() throws Exception {
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object lockname = new Integer(10);

        LockHandle handle = lockmgr.acquire(
            tran1,
            lockname,
            LockMode.EXCLUSIVE,
            LockDuration.MANUAL_DURATION,
            -1,
            null);
        try {
            lockmgr.acquire(
                tran1,
                lockname,
                LockMode.SHARED,
                LockDuration.INSTANT_DURATION,
                0,
                null);
        } catch (LockException e) {
            fail(e.getMessage());
        }
        assertTrue(handle.release(false));
    }

    /**
     * This test verifies that if an conversion lock request is asked for as
     * INSTANT_DURATION, then it is not actually granted. The caller is delayed until
     * the lock becomes available, but the lock remains in the previous mode.
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
                    LockHandle handle1 = lockmgr.acquire(
                        tran2,
                        lockname,
                        LockMode.SHARED,
                        LockDuration.MANUAL_DURATION,
                        -1,
                        null);
                    Thread.sleep(500);
                    LockHandle handle2 = lockmgr.acquire(
                        tran2,
                        lockname,
                        LockMode.EXCLUSIVE,
                        LockDuration.INSTANT_DURATION,
                        -1,
                        null);
                    assertTrue(handle2.getCurrentMode() == LockMode.SHARED);
                    assertTrue(handle1.release(false));
                } catch (Exception e) {
                    counter.incrementAndGet();
                    e.printStackTrace();
                }
            }
        });

        t1.start();
        Thread.sleep(200);
        LockHandle handle = lockmgr.acquire(
            tran1,
            lockname,
            LockMode.UPDATE,
            LockDuration.MANUAL_DURATION,
            -1,
            null);
        Thread.sleep(1000);
        assertEquals(LockMode.UPDATE, lockmgr.findLock(tran1, lockname));
        assertEquals(LockMode.SHARED, lockmgr.findLock(tran2, lockname));
        assertTrue(handle.release(false));
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

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    LockHandle handle1 = lockmgr.acquire(
                        tran2,
                        lockname,
                        LockMode.SHARED,
                        LockDuration.MANUAL_DURATION,
                        -1,
                        null);
                    Thread.sleep(500);
                    LockHandle handle2 = lockmgr.acquire(
                        tran2,
                        lockname,
                        LockMode.EXCLUSIVE,
                        LockDuration.COMMIT_DURATION,
                        -1,
                        null);
                    assertTrue(handle2.getCurrentMode() == LockMode.EXCLUSIVE);
                    assertFalse(handle1.release(false));
                    assertTrue(handle1.release(true));
                } catch (Exception e) {
                    counter.incrementAndGet();
                    e.printStackTrace();
                }
            }
        });

        t1.start();
        Thread.sleep(200);
        LockHandle handle = lockmgr.acquire(
            tran1,
            lockname,
            LockMode.UPDATE,
            LockDuration.MANUAL_DURATION,
            -1,
            null);
        Thread.sleep(1000);
        assertEquals(LockMode.UPDATE, lockmgr.findLock(tran1, lockname));
        assertEquals(LockMode.SHARED, lockmgr.findLock(tran2, lockname));
        assertTrue(handle.release(false));
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

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    LockHandle handle1 = lockmgr.acquire(
                        tran1,
                        lockname1,
                        LockMode.EXCLUSIVE,
                        LockDuration.MANUAL_DURATION,
                        5,
                        null);
                    try {
                        Thread.sleep(1000);
                        LockHandle handle2 = lockmgr.acquire(
                            tran1,
                            lockname2,
                            LockMode.EXCLUSIVE,
                            LockDuration.MANUAL_DURATION,
                            10,
                            null);
                        handle2.release(false);
                    } finally {
                        handle1.release(false);
                    }
                } catch (LockDeadlockException e) {
                    // System.err.println("T1 failed with " + e);
                    // e.printStackTrace(System.out);
                    countDeadlocks++;
                } catch (Exception e) {
                    setThreadFailed(Thread.currentThread(), e);
                }

            }
        });
        Thread t2 = new Thread(new Runnable() {
            public void run() {
                try {
                    LockHandle handle2 = lockmgr.acquire(
                        tran2,
                        lockname2,
                        LockMode.EXCLUSIVE,
                        LockDuration.MANUAL_DURATION,
                        5,
                        null);
                    try {
                        Thread.sleep(1000);
                        LockHandle handle1 = lockmgr.acquire(
                            tran2,
                            lockname1,
                            LockMode.EXCLUSIVE,
                            LockDuration.MANUAL_DURATION,
                            10,
                            null);
                        handle1.release(false);
                    } finally {
                        handle2.release(false);
                    }
                } catch (LockDeadlockException e) {
                    // System.err.println("T1 failed with " + e);
                    // e.printStackTrace(System.out);
                    countDeadlocks++;
                } catch (Exception e) {
                    setThreadFailed(Thread.currentThread(), e);
                }
            }
        });

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
            checkThreadFailures();
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

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    LockHandle handle1 = lockmgr.acquire(
                        tran1,
                        lockname1,
                        LockMode.SHARED,
                        LockDuration.MANUAL_DURATION,
                        5,
                        null);
                    try {
                        Thread.sleep(1000);
                        LockHandle handle2 = lockmgr.acquire(
                            tran1,
                            lockname1,
                            LockMode.EXCLUSIVE,
                            LockDuration.MANUAL_DURATION,
                            10,
                            null);
                        handle2.release(false);
                    } finally {
                        handle1.release(false);
                    }
                } catch (LockDeadlockException e) {
                    // System.err.println("T1 failed with " + e);
                    // e.printStackTrace(System.out);
                    countDeadlocks++;
                } catch (Exception e) {
                    setThreadFailed(Thread.currentThread(), e);
                }
            }
        });
        Thread t2 = new Thread(new Runnable() {
            public void run() {
                try {
                    LockHandle handle2 = lockmgr.acquire(
                        tran2,
                        lockname1,
                        LockMode.SHARED,
                        LockDuration.MANUAL_DURATION,
                        5,
                        null);
                    try {
                        Thread.sleep(1000);
                        LockHandle handle1 = lockmgr.acquire(
                            tran2,
                            lockname1,
                            LockMode.EXCLUSIVE,
                            LockDuration.MANUAL_DURATION,
                            10,
                            null);
                        handle1.release(false);
                    } finally {
                        handle2.release(false);
                    }
                } catch (LockDeadlockException e) {
                    // System.err.println("T1 failed with " + e);
                    // e.printStackTrace(System.out);
                    countDeadlocks++;
                } catch (Exception e) {
                    setThreadFailed(Thread.currentThread(), e);
                }
            }
        });

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
            checkThreadFailures();
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

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    LockHandle handle1 = lockmgr.acquire(
                        tran1,
                        lockname1,
                        LockMode.EXCLUSIVE,
                        LockDuration.MANUAL_DURATION,
                        10,
                        null);
                    try {
                        LockHandle handle2 = lockmgr.acquire(
                            tran1,
                            lockname2,
                            LockMode.SHARED,
                            LockDuration.MANUAL_DURATION,
                            10,
                            null);
                        try {
                            Thread.sleep(1500);
                            LockHandle handle3 = lockmgr.acquire(
                                tran1,
                                lockname3,
                                LockMode.SHARED,
                                LockDuration.MANUAL_DURATION,
                                10,
                                null);
                            handle3.release(false);
                        } finally {
                            handle2.release(false);
                        }
                    } finally {
                        handle1.release(false);
                    }
                } catch (LockDeadlockException e) {
                    // System.err.println("T1 failed with " + e);
                    // e.printStackTrace(System.out);
                    countDeadlocks++;
                } catch (Exception e) {
                    setThreadFailed(Thread.currentThread(), e);
                }

            }
        });
        Thread t2 = new Thread(new Runnable() {
            public void run() {
                try {
                    LockHandle handle1 = lockmgr.acquire(
                        tran2,
                        lockname3,
                        LockMode.EXCLUSIVE,
                        LockDuration.MANUAL_DURATION,
                        10,
                        null);
                    try {
                        LockHandle handle2 = lockmgr.acquire(
                            tran2,
                            lockname2,
                            LockMode.SHARED,
                            LockDuration.MANUAL_DURATION,
                            10,
                            null);
                        try {
                            Thread.sleep(500);
                            LockHandle handle3 = lockmgr.acquire(
                                tran2,
                                lockname2,
                                LockMode.EXCLUSIVE,
                                LockDuration.MANUAL_DURATION,
                                10,
                                null);
                            handle3.release(false);
                        } finally {
                            handle2.release(false);
                        }
                    } finally {
                        handle1.release(false);
                    }
                } catch (LockDeadlockException e) {
                    // System.err.println("T2 failed with " + e);
                    // e.printStackTrace(System.out);
                    countDeadlocks++;
                } catch (Exception e) {
                    setThreadFailed(Thread.currentThread(), e);
                }
            }
        });
        Thread t3 = new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(1500);
                    LockHandle handle1 = lockmgr.acquire(
                        tran3,
                        lockname2,
                        LockMode.EXCLUSIVE,
                        LockDuration.MANUAL_DURATION,
                        10,
                        null);
                    handle1.release(false);
                } catch (LockDeadlockException e) {
//					System.err.println("T3 failed with " + e);
//					e.printStackTrace(System.out);
                    countDeadlocks++;
                } catch (Exception e) {
                    setThreadFailed(Thread.currentThread(), e);
                }
            }
        });

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
            checkThreadFailures();
        } finally {
            lockmgr.shutdown();
        }
    }

    void checkMemoryUsage() throws Exception {

        LockManager lockmgr = new LockManagerImpl();
        Object tran1 = new Integer(1);

        long startingMemoryUse = getUsedMemory();

        System.out.println("Initial memory used = " + startingMemoryUse);
        for (int i = 0; i < 10000; i++) {
            lockmgr.acquire(
                tran1,
                new Integer(i),
                LockMode.EXCLUSIVE,
                LockDuration.MANUAL_DURATION,
                10,
                null);
        }
        long endingMemoryUse = getUsedMemory();
        System.out.println("Total memory used for 10000 locks = "
                + (endingMemoryUse - startingMemoryUse));
    }

    static long sizeOf(Class clazz) {
        long size = 0;
        Object[] objects = new Object[10000];
        try {
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

    private static LockManager createLockManager() {
        return new LockManagerImpl();
    }

}
