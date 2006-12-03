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
		LockHandle handle = lockmgr.acquire(tran1, lockname, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
		
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				try {
					// We expect this lock to be granted after the exclusive lock is released
					assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname));
					LockHandle handle1 = lockmgr.acquire(tran2, lockname, LockMode.SHARED, LockDuration.MANUAL_DURATION, -1);
					assertEquals(LockMode.SHARED, lockmgr.findLock(tran2, lockname));
					assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
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
					// We expect this lock to be granted after the exclusive lock is released
					assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname));
					LockHandle handle2 = lockmgr.acquire(tran3, lockname, LockMode.SHARED, LockDuration.MANUAL_DURATION, -1);
					assertEquals(LockMode.SHARED, lockmgr.findLock(tran3, lockname));
					assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
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
			// Allow the threads to do some work
			Thread.sleep(500);
			// We still hold the exclusive lock
			assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname));
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
	 * Stresses the concurrent access to same hash bucket. Two threads are started,
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
						LockHandle handle1 = lockmgr.acquire(tran1, lockname1, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
						assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname1));
						handle1.release(false);
						assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname1));
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
						LockHandle handle2 = lockmgr.acquire(tran2, lockname2, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
						assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran2, lockname2));
						handle2.release(false);
						assertEquals(LockMode.NONE, lockmgr.findLock(tran2, lockname2));
						counter.incrementAndGet();
					}
				} catch (LockException e) {
					setThreadFailed(Thread.currentThread(), e);
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
			assertTrue(counter.get() == 10000);
			checkThreadFailures();
		} catch (InterruptedException e) {
			fail();
		}
	}

	/**
	 * Simple lock acquire and release tests
	 */
	public void testLocks() throws Exception {
		final LockManager lockmgr = createLockManager();
		final Object tran1 = new Integer(1);
		final Object lockname = new Integer(10);

		LockHandle handle = lockmgr.acquire(tran1, lockname, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
		assertEquals(LockMode.EXCLUSIVE, lockmgr.findLock(tran1, lockname));
		handle.release(false);
		assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
		handle = lockmgr.acquire(tran1, lockname, LockMode.SHARED, LockDuration.MANUAL_DURATION, -1);
		assertEquals(LockMode.SHARED, lockmgr.findLock(tran1, lockname));
		handle.release(false);
		assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
		handle = lockmgr.acquire(tran1, lockname, LockMode.INTENTION_EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
		assertEquals(LockMode.INTENTION_EXCLUSIVE, lockmgr.findLock(tran1, lockname));
		handle.release(false);
		assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
		handle = lockmgr.acquire(tran1, lockname, LockMode.INTENTION_SHARED, LockDuration.MANUAL_DURATION, -1);
		assertEquals(LockMode.INTENTION_SHARED, lockmgr.findLock(tran1, lockname));
		handle.release(false);
		assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
		handle = lockmgr.acquire(tran1, lockname, LockMode.SHARED_INTENTION_EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
		assertEquals(LockMode.SHARED_INTENTION_EXCLUSIVE, lockmgr.findLock(tran1, lockname));
		handle.release(false);
		assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
		handle = lockmgr.acquire(tran1, lockname, LockMode.UPDATE, LockDuration.MANUAL_DURATION, -1);
		assertEquals(LockMode.UPDATE, lockmgr.findLock(tran1, lockname));
		handle.release(false);
		assertEquals(LockMode.NONE, lockmgr.findLock(tran1, lockname));
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
					// System.err.println("T1 sleeping for 5 seconds");
					// Wait until threads 2 and 3 start
					Thread.sleep(1500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.err.println("T1(6) locking " + lockname + " in exclusive mode (should trigger conversion request and block) ...");
				LockHandle handle2 = lockMgr.acquire(tran, lockname, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, 60);
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
					// System.err.println("T2 sleeping for 25 seconds");
					// Wait for thread 3 to start and thread 1 to block
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.err.println("T2(7) releasing lock (should grant conversion request T1)");
				handle1.release(false);
				try {
					// System.err.println("T2 sleeping for 1 second");
					// wait for thread 1 to get conversion request
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				System.err.println("T2(9) locking " + lockname + " in shared mode (should block) ...");
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

	public void testLocking() throws Exception {

		ThreadFactory factory = Executors.defaultThreadFactory();

		LockManager lockmgr = createLockManager();

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
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		locker2.start();
		try {
			Thread.sleep(500);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		locker3.start();
		locker1.join();
		locker2.join();
		locker3.join();
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
        final LockManager lockmgr = createLockManager();
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
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object lockname = new Integer(10);

        LockHandle handle = lockmgr.acquire(tran1, lockname, LockMode.UPDATE, LockDuration.MANUAL_DURATION, -1);
        try {
            handle.downgrade(LockMode.EXCLUSIVE);
            fail();
        }
        catch (Exception e) {
            e.printStackTrace(System.out);
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
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object lockname = new Integer(10);
        final AtomicInteger counter = new AtomicInteger(0);

        LockHandle handle = lockmgr.acquire(tran1, lockname, LockMode.UPDATE, LockDuration.MANUAL_DURATION, -1);

        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    lockmgr.acquire(tran2, lockname, LockMode.SHARED, LockDuration.INSTANT_DURATION, -1);
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
        final LockManager lockmgr = createLockManager();
        final Object tran1 = new Integer(1);
        final Object tran2 = new Integer(2);
        final Object lockname = new Integer(10);

        LockHandle handle = lockmgr.acquire(tran1, lockname, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, -1);
        try {
            lockmgr.acquire(tran2, lockname, LockMode.SHARED, LockDuration.INSTANT_DURATION, 0);
            fail();
        }
        catch (Exception e) {
            e.printStackTrace(System.out);
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
        final LockManager lockmgr = createLockManager();
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
    
    volatile int countDeadlocks = 0;
    
	public void testDeadlockDetection() throws Exception {
		final LockManager lockmgr = createLockManager();
		final Object tran1 = new Integer(1);
		final Object tran2 = new Integer(2);
		final Object lockname1 = new Integer(10);
		final Object lockname2 = new Integer(11);

		Thread t1 = new Thread(new Runnable() {
			public void run() {
				try {
					LockHandle handle1 = lockmgr.acquire(tran1, lockname1, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, 5);
					try {
						Thread.sleep(1000);
						LockHandle handle2 = lockmgr.acquire(tran1, lockname2, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, 10);
						handle2.release(false);
					}
					finally {
						handle1.release(false);
					}
				} catch (LockDeadlockException e) {
					System.err.println("T1 failed with " + e);
					e.printStackTrace(System.out);
					countDeadlocks++;
				} catch (Exception e) {
					System.err.println("Unexpected error: " + e);
					e.printStackTrace();
				}
				
			}
		});
		Thread t2 = new Thread(new Runnable() {
			public void run() {
				try {
					LockHandle handle2 = lockmgr.acquire(tran2, lockname2, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, 5);
					try {
						Thread.sleep(1000);	
						LockHandle handle1 = lockmgr.acquire(tran2, lockname1, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, 10);
						handle1.release(false);
					}
					finally {
						handle2.release(false);
					}
				} catch (LockDeadlockException e) {
					System.err.println("T1 failed with " + e);
					e.printStackTrace(System.out);
					countDeadlocks++;
				} catch (Exception e) {
					System.err.println("Unexpected error: " + e);
					e.printStackTrace();
				}
			}
		});

		try {
			countDeadlocks = 0;
			t1.start();
			t2.start();
			Thread.sleep(2000);
			((LockManagerImpl)lockmgr).detectDeadlocks();
            t1.join(10000);
            t2.join(10000);
            assertTrue(!t1.isAlive());
            assertTrue(!t2.isAlive());
            assertEquals(countDeadlocks, 1);
		} catch (Exception e) {
			fail("");
		}
	}    
    
	
	public void testConversionDeadlockDetection() throws Exception {
		final LockManager lockmgr = createLockManager();
		final Object tran1 = new Integer(1);
		final Object tran2 = new Integer(2);
		final Object lockname1 = new Integer(10);

		Thread t1 = new Thread(new Runnable() {
			public void run() {
				try {
					LockHandle handle1 = lockmgr.acquire(tran1, lockname1, LockMode.SHARED, LockDuration.MANUAL_DURATION, 5);
					try {
						Thread.sleep(1000);
						LockHandle handle2 = lockmgr.acquire(tran1, lockname1, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, 10);
						handle2.release(false);
					}
					finally {
						handle1.release(false);
					}
				} catch (LockDeadlockException e) {
					System.err.println("T1 failed with " + e);
					e.printStackTrace(System.out);
					countDeadlocks++;
				} catch (Exception e) {
					System.err.println("Unexpected error: " + e);
					e.printStackTrace();
				}
			}
		});
		Thread t2 = new Thread(new Runnable() {
			public void run() {
				try {
					LockHandle handle2 = lockmgr.acquire(tran2, lockname1, LockMode.SHARED, LockDuration.MANUAL_DURATION, 5);
					try {
						Thread.sleep(1000);	
						LockHandle handle1 = lockmgr.acquire(tran2, lockname1, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, 10);
						handle1.release(false);
					}
					finally {
						handle2.release(false);
					}
				} catch (LockDeadlockException e) {
					System.err.println("T1 failed with " + e);
					e.printStackTrace(System.out);
					countDeadlocks++;
				} catch (Exception e) {
					System.err.println("Unexpected error: " + e);
					e.printStackTrace();
				}
			}
		});

		try {
			countDeadlocks = 0;
			t1.start();
			t2.start();
			Thread.sleep(2000);
			((LockManagerImpl)lockmgr).detectDeadlocks();
            t1.join(10000);
            t2.join(10000);
            assertTrue(!t1.isAlive());
            assertTrue(!t2.isAlive());
            assertEquals(countDeadlocks, 1);
		} catch (Exception e) {
			fail("");
		}
	}    	

	public void testMultipleDeadlockDetection() throws Exception {
		final LockManager lockmgr = createLockManager();
		final Object tran1 = new Integer(1);
		final Object tran2 = new Integer(2);
		final Object tran3 = new Integer(3);
		final Object tran4 = new Integer(4);
		final Object lockname1 = new Integer(10);
		final Object lockname2 = new Integer(11);
		final Object lockname3 = new Integer(12);
		final Object lockname4 = new Integer(13);

		Thread t1 = new Thread(new Runnable() {
			public void run() {
				try {
					LockHandle handle1 = lockmgr.acquire(tran1, lockname1, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, 10);
					try {
						LockHandle handle2 = lockmgr.acquire(tran1, lockname2, LockMode.SHARED, LockDuration.MANUAL_DURATION, 10);
						try {
							Thread.sleep(1000);
							LockHandle handle3 = lockmgr.acquire(tran1, lockname3, LockMode.SHARED, LockDuration.MANUAL_DURATION, 10);
							handle3.release(false);
						}
						finally {
							handle2.release(false);
						}
					}
					finally {
						handle1.release(false);
					}
				} catch (LockDeadlockException e) {
					System.err.println("T1 failed with " + e);
					e.printStackTrace(System.out);
					countDeadlocks++;
				} catch (Exception e) {
					System.err.println("Unexpected error: " + e);
					e.printStackTrace();
				}
				
			}
		});
		Thread t2 = new Thread(new Runnable() {
			public void run() {
				try {
					LockHandle handle1 = lockmgr.acquire(tran2, lockname3, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, 10);
					try {
						LockHandle handle2 = lockmgr.acquire(tran2, lockname2, LockMode.SHARED, LockDuration.MANUAL_DURATION, 10);
						try {
							Thread.sleep(500);
							LockHandle handle3 = lockmgr.acquire(tran2, lockname2, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, 10);
							handle3.release(false);
						}
						finally {
							handle2.release(false);
						}
					}
					finally {
						handle1.release(false);
					}
				} catch (LockDeadlockException e) {
					System.err.println("T2 failed with " + e);
					e.printStackTrace(System.out);
					countDeadlocks++;
				} catch (Exception e) {
					System.err.println("Unexpected error: " + e);
					e.printStackTrace();
				}
			}
		});
		Thread t3 = new Thread(new Runnable() {
			public void run() {
				try {
					Thread.sleep(1000);
					LockHandle handle1 = lockmgr.acquire(tran3, lockname2, LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION, 10);
					handle1.release(false);
				} catch (LockDeadlockException e) {
					System.err.println("T3 failed with " + e);
					e.printStackTrace(System.out);
					countDeadlocks++;
				} catch (Exception e) {
					System.err.println("Unexpected error: " + e);
					e.printStackTrace();
				}
			}
		});

		try {
			countDeadlocks = 0;
			t1.start();
			t2.start();
			t3.start();
			Thread.sleep(2000);
			((LockManagerImpl)lockmgr).detectDeadlocks();
            t1.join(10000);
            t2.join(10000);
            t3.join(10000);
            assertTrue(!t1.isAlive());
            assertTrue(!t2.isAlive());
            assertTrue(!t3.isAlive());
            assertEquals(countDeadlocks, 2);
		} catch (Exception e) {
			fail("");
		}
	}    

	
	private static LockManager createLockManager() {
		return new LockManagerImpl(71);
	}

}
