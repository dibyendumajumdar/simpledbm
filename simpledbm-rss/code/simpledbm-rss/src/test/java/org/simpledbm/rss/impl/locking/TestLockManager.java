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

import junit.framework.TestCase;

import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockException;
import org.simpledbm.rss.api.locking.LockHandle;
import org.simpledbm.rss.api.locking.LockManager;
import org.simpledbm.rss.api.locking.LockMode;
import org.simpledbm.rss.impl.locking.LockManagerImpl;

/**
 * Test cases for Lock Manager module.
 * 
 * @author Dibyendu Majumdar
 * @since 21-Aug-2005
 */
public class TestLockManager extends TestCase {

	public TestLockManager(String arg0) {
		super(arg0);
	}

	public void testLocking() throws Exception {

		ThreadFactory factory = Executors.defaultThreadFactory();

		LockManager lockmgr = new LockManagerImpl(71);

		Object tran1 = new Integer(1);
		Object tran2 = new Integer(2);
		Object tran3 = new Integer(3);
		Object lockname = new Integer(10);
		AtomicInteger sync = new AtomicInteger(0);

		Thread locker1 = factory.newThread(new Lock1(sync, tran1, lockname, lockmgr));
		Thread locker2 = factory.newThread(new Lock2(sync, tran2, lockname, lockmgr));
		Thread locker3 = factory.newThread(new Lock3(sync, tran3, lockname, lockmgr));
		locker1.start();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		locker2.start();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		locker3.start();
		locker1.join();
		locker2.join();
		locker3.join();
	}

	/**
	 * Readlocks succeed after a writing thread unlocks
	 */
	public void testReadAfterWriteLock() throws Exception {
		final LockManager lockmgr = new LockManagerImpl(71);
		final Object tran1 = new Integer(1);
		final Object tran2 = new Integer(2);
		final Object tran3 = new Integer(3);
		final Object lockname = new Integer(10);
		final AtomicInteger counter = new AtomicInteger(0);

		LockHandle handle = lockmgr.acquire(tran1, lockname, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);

		Thread t1 = new Thread(new Runnable() {
			public void run() {
				try {
					LockHandle handle1 = lockmgr.acquire(tran2, lockname, LockMode.SHARED, LockDuration.MANUAL_DURATION, -1);
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
					LockHandle handle2 = lockmgr.acquire(tran3, lockname, LockMode.SHARED, LockDuration.MANUAL_DURATION, -1);
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
			Thread.sleep(1000);
			handle.release(false);
			t1.join(1000);
			t2.join(1000);
			assertTrue(!t1.isAlive());
			assertTrue(!t2.isAlive());
			assertTrue(counter.get() == 2);
		} catch (Exception e) {
			fail("");
		}
	}

	/**
	 * Readlocks succeed after a writing thread unlocks
	 */
	public void testConcurrentReadWrites() throws Exception {
		final LockManager lockmgr = new LockManagerImpl(71);
		final Object tran1 = new Integer(1);
		final Object tran2 = new Integer(2);
		final Object lockname1 = new ConstantHashingObject(10);
		final Object lockname2 = new ConstantHashingObject(20);
		final AtomicInteger counter = new AtomicInteger(0);

		Thread t1 = new Thread(new Runnable() {
			public void run() {
				try {
					for (int i = 0; i < 1000; i++) {
						LockHandle handle1 = lockmgr.acquire(tran1, lockname1, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
						handle1.release(false);
						counter.incrementAndGet();
					}
				} catch (LockException e) {
					e.printStackTrace();
				}
			}
		});
		Thread t2 = new Thread(new Runnable() {
			public void run() {
				try {
					for (int i = 0; i < 1000; i++) {
						LockHandle handle2 = lockmgr.acquire(tran2, lockname2, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
						handle2.release(false);
						counter.incrementAndGet();
					}
				} catch (LockException e) {
					e.printStackTrace();
				}
			}
		});

		try {
			t1.start();
			t2.start();
			t1.join(10000);
			t2.join(10000);
			assertTrue(!t1.isAlive());
			assertTrue(!t2.isAlive());
			assertTrue(counter.get() == 2000);
		} catch (Exception e) {
			fail("");
		}
	}

	public void testLocks() throws Exception {
		final LockManager lockmgr = new LockManagerImpl(71);
		final Object tran1 = new Integer(1);
		final Object lockname = new Integer(10);

		LockHandle handle = lockmgr.acquire(tran1, lockname, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
		handle.release(false);
		handle = lockmgr.acquire(tran1, lockname, LockMode.SHARED, LockDuration.MANUAL_DURATION, -1);
		handle.release(false);
		handle = lockmgr.acquire(tran1, lockname, LockMode.INTENTION_EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
		handle.release(false);
		handle = lockmgr.acquire(tran1, lockname, LockMode.INTENTION_SHARED, LockDuration.MANUAL_DURATION, -1);
		handle.release(false);
		handle = lockmgr.acquire(tran1, lockname, LockMode.SHARED_INTENTION_EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
		handle.release(false);
		handle = lockmgr.acquire(tran1, lockname, LockMode.UPDATE, LockDuration.MANUAL_DURATION, -1);
		handle.release(false);
	}

	static final class Lock1 implements Runnable {

		AtomicInteger sync;

		Object tran;

		Object lockname;

		LockManager lockMgr;

		public Lock1(AtomicInteger sync, Object tran, Object lockname, LockManager lockMgr) {
			this.tran = tran;
			this.lockname = lockname;
			this.lockMgr = lockMgr;
			this.sync = sync;
		}

		public void run() {

			try {
				System.err.println("T1(1) locking " + lockname + " in shared mode");
				LockHandle handle1 = lockMgr.acquire(tran, lockname, LockMode.SHARED, LockDuration.MANUAL_DURATION, 60);
				System.err.println("T1(2) acquired lock " + handle1);
				assertTrue(sync.compareAndSet(0, 2));
				try {
					System.err.println("T1 sleeping for 5 seconds");
					Thread.sleep(5 * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.err.println("T1(6) locking " + lockname + " in exclusive mode (should trigger conversion request) ...");
				LockHandle handle2 = lockMgr.acquire(tran, lockname, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, 60);
				System.err.println("T1(8) lock acquired " + handle2);
				assertTrue(sync.compareAndSet(4, 8));
				try {
					System.err.println("T1 sleeping for 5 seconds");
					Thread.sleep(5 * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.err.println("T1(10) downgrading to shared mode");
				handle2.downgrade(LockMode.SHARED);
				try {
					System.err.println("T1 sleeping for 5 seconds");
					Thread.sleep(5 * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.err.println("T1(11) releasing lock");
				handle2.release(false);
				System.err.println("T1(12) releasing lock (should grant exclusive to T3)");
				handle1.release(false);
			} catch (LockException e) {
				e.printStackTrace();
			}
		}
	}

	static final class Lock2 implements Runnable {

		AtomicInteger sync;

		Object tran;

		Object lockname;

		LockManager lockMgr;

		public Lock2(AtomicInteger sync, Object tran1, Object lockname, LockManager lockMgr) {
			this.tran = tran1;
			this.lockname = lockname;
			this.lockMgr = lockMgr;
			this.sync = sync;
		}

		public void run() {

			try {
				System.err.println("T2(3) locking " + lockname + " in shared mode");
				LockHandle handle1 = lockMgr.acquire(tran, lockname, LockMode.SHARED, LockDuration.MANUAL_DURATION, 60);
				System.err.println("T2(4) acquired lock " + handle1);
				assertTrue(sync.compareAndSet(2, 4));
				try {
					System.err.println("T2 sleeping for 25 seconds");
					Thread.sleep(25 * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.err.println("T2(7) releasing lock (should grant conversion request T1)");
				handle1.release(false);
				try {
					System.err.println("T2 sleeping for 1 second");
					Thread.sleep(1 * 1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.err.println("T2(9) locking " + lockname + " in shared mode (should wait) ...");
				handle1 = lockMgr.acquire(tran, lockname, LockMode.SHARED, LockDuration.MANUAL_DURATION, 60);
				System.err.println("T2(15) acquired lock" + handle1);
				assertTrue(sync.compareAndSet(13, 16));
				System.err.println("T2(16) releasing lock");
				handle1.release(false);
			} catch (LockException e) {
				e.printStackTrace();
			}
		}
	}

	static final class Lock3 implements Runnable {

		AtomicInteger sync;

		Object tran;

		Object lockname;

		LockManager lockMgr;

		public Lock3(AtomicInteger sync, Object tran, Object lockname, LockManager lockMgr) {
			this.tran = tran;
			this.lockname = lockname;
			this.lockMgr = lockMgr;
			this.sync = sync;
		}

		public void run() {

			try {
				System.err.println("T3(5) locking " + lockname + " in exclusive mode (should wait) ...");
				LockHandle handle = lockMgr.acquire(tran, lockname, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, 60);
				System.err.println("T3(13) acquired lock " + handle);
				assertTrue(sync.compareAndSet(8, 13));
				System.err.println("T3(14) releasing lock (should grant shared lock to T2)");
				handle.release(false);
			} catch (LockException e) {
				e.printStackTrace();
			}
		}
	}

	static final class ConstantHashingObject {
		final int i;

		public ConstantHashingObject(int i) {
			this.i = i;
		}

		@Override
		public int hashCode() {
			return 10;
		}

		@Override
		public boolean equals(Object o) {
			return i == ((ConstantHashingObject) o).i;
		}
	}

    /**
     * Readlocks succeed after a writing thread unlocks
     */
    public void testLockDowngrade() throws Exception {
        final LockManager lockmgr = new LockManagerImpl(71);
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object tran3 = new Integer(3);
        final Object lockname = new Integer(10);
        final AtomicInteger counter = new AtomicInteger(0);

        LockHandle handle = lockmgr.acquire(tran1, lockname, LockMode.UPDATE, LockDuration.MANUAL_DURATION, -1);

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    LockHandle handle1 = lockmgr.acquire(tran2, lockname, LockMode.SHARED, LockDuration.MANUAL_DURATION, -1);
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
                    LockHandle handle2 = lockmgr.acquire(tran3, lockname, LockMode.SHARED, LockDuration.MANUAL_DURATION, -1);
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
            Thread.sleep(1000);
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
        final LockManager lockmgr = new LockManagerImpl(71);
        final Object tran1 = new Integer(1);
        final Object lockname = new Integer(10);

        LockHandle handle = lockmgr.acquire(tran1, lockname, LockMode.UPDATE, LockDuration.MANUAL_DURATION, -1);
        try {
            handle.downgrade(LockMode.EXCLUSIVE);
            fail();
        }
        catch (Exception e) {
            e.printStackTrace();
            assertTrue(true);
        }
        assertTrue(handle.release(false));
    }

    /**
     * This test verifies that an INSTANT_DURATION lock works correctly when it is necessary 
     * to wait for the lock to become available. Also, it tests that a lock downgrade leads 
     * to eleigible waiting requests being granted locks.
     */
    public void testInstantLockWait() throws Exception {
        final LockManager lockmgr = new LockManagerImpl(71);
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object lockname = new Integer(10);
        final AtomicInteger counter = new AtomicInteger(0);

        LockHandle handle = lockmgr.acquire(tran1, lockname, LockMode.UPDATE, LockDuration.MANUAL_DURATION, -1);

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    /* LockHandle handle1 = */ lockmgr.acquire(tran2, lockname, LockMode.SHARED, LockDuration.INSTANT_DURATION, -1);
                    // FIXME incorrect 
       //             assert handle1.getCurrentMode() == LockMode.NONE;
                    assertTrue(counter.get() == 1);
                } catch (LockException e) {
                    counter.incrementAndGet();
                    e.printStackTrace();
                }
            }
        });

        try {
            t1.start();
            Thread.sleep(1000);
            counter.incrementAndGet();
            handle.downgrade(LockMode.SHARED);
            t1.join(1000);
            assertTrue(!t1.isAlive());
            assertTrue(counter.get() == 1);
            assertTrue(handle.release(false));
        } catch (Exception e) {
            fail();
        }
    }

    /**
     * This test verifies that an incompatible lock request of INSTANT_DURATION fails if nowait is true.
     */
    public void testInstantLockNowait() throws Exception {
        final LockManager lockmgr = new LockManagerImpl(71);
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object lockname = new Integer(10);

        LockHandle handle = lockmgr.acquire(tran1, lockname, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
        try {
            lockmgr.acquire(tran2, lockname, LockMode.SHARED, LockDuration.INSTANT_DURATION, 0);
            fail();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        assertTrue(handle.release(false));
    }
    
    /**
     * This test verifies that if a client already holds a lock, a request for compatible INSTANT_DURATION lock
     * succeeds even when the lock is requested conditionally.
     */
    public void testInstantLockSelf() throws Exception {
        final LockManager lockmgr = new LockManagerImpl(71);
        final Object tran1 = new Integer(1);
        final Object lockname = new Integer(10);

        LockHandle handle = lockmgr.acquire(tran1, lockname, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
        try {
            lockmgr.acquire(tran1, lockname, LockMode.SHARED, LockDuration.INSTANT_DURATION, 0);
        }
        catch (Exception e) {
            fail();
            e.printStackTrace();
        }
        assertTrue(handle.release(false));
    }
    
    /**
     * This test verifies that if an conversion lock request is asked for as
     * INSTANT_DURATION, then it is not actually granted. The caller is delayed until
     * the lock becomes available, but the lock remains in the previous mode.
     */
    public void testInstantLockConversionWait() throws Exception {
        final LockManager lockmgr = new LockManagerImpl(71);
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object lockname = new Integer(10);
        final AtomicInteger counter = new AtomicInteger(0);

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    LockHandle handle1 = lockmgr.acquire(tran2, lockname, LockMode.SHARED, LockDuration.MANUAL_DURATION, -1);
                    Thread.sleep(500);
                    LockHandle handle2 = lockmgr.acquire(tran2, lockname, LockMode.EXCLUSIVE, LockDuration.INSTANT_DURATION, -1);
                    assertTrue(handle2.getCurrentMode() == LockMode.SHARED);
                    assertTrue(handle1.release(false));
                } catch (Exception e) {
                    counter.incrementAndGet();
                    e.printStackTrace();
                }
            }
        });

        try {
            t1.start();
            Thread.sleep(200);
            LockHandle handle = lockmgr.acquire(tran1, lockname, LockMode.UPDATE, LockDuration.MANUAL_DURATION, -1);
            Thread.sleep(1000);
            assertTrue(handle.release(false));
            t1.join();
            assertTrue(!t1.isAlive());
            assertTrue(counter.get() == 0);
        } catch (Exception e) {
            fail();
        }
    }
    
}
