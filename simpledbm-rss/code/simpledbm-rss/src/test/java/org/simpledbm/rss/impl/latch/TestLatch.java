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
package org.simpledbm.rss.impl.latch;

import junit.framework.TestCase;

import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.impl.latch.LatchFactoryImpl;
import org.simpledbm.rss.impl.latch.ReadWriteUpdateLatch;

/**
 * 
 * @author Dibyendu Majumdar
 * @since Aug 22, 2005
 */
public class TestLatch extends TestCase {

	static LatchFactory latchFactory = new LatchFactoryImpl();
	
	public TestLatch(String arg0) {
		super(arg0);
	}

	public void testCase1() throws Exception {
		final ReadWriteUpdateLatch latch = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatch();
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.FREE);
		latch.exclusiveLock();
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.EXCLUSIVE);
		assertTrue(latch.getOwner() == Thread.currentThread());
		latch.unlock();
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.FREE);
		latch.updateLock();
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.UPDATE);
		assertTrue(latch.getOwner() == Thread.currentThread());
		assertTrue(latch.getSharedCount() == 0);
		latch.unlock();
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.FREE);
		latch.sharedLock();
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.SHARED);
		assertTrue(latch.getOwner() == null);
		assertTrue(latch.getSharedCount() == 1);
		latch.sharedLock();
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.SHARED);
		assertTrue(latch.getOwner() == null);
		assertTrue(latch.getSharedCount() == 2);
		latch.updateLock();
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.UPDATE);
		assertTrue(latch.getOwner() == Thread.currentThread());
		assertTrue(latch.getSharedCount() == 2);
		latch.unlock();
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.SHARED);
		assertTrue(latch.getOwner() == null);
		assertTrue(latch.getSharedCount() == 2);
		latch.unlock();
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.SHARED);
		assertTrue(latch.getOwner() == null);
		assertTrue(latch.getSharedCount() == 1);
		latch.unlock();
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.FREE);
		latch.updateLock();
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.UPDATE);
		assertTrue(latch.getOwner() == Thread.currentThread());
		assertTrue(latch.getSharedCount() == 0);
		latch.upgradeUpdateLock();
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.EXCLUSIVE);
		assertTrue(latch.getOwner() == Thread.currentThread());
		assertTrue(latch.getSharedCount() == 0);
		latch.unlock();
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.FREE);
	}

	public void testWriteBlocksWrite() throws Exception {
		final ReadWriteUpdateLatch latch = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatch();
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				assertFalse(latch.tryExclusiveLock());
				latch.exclusiveLock();
				assertTrue(latch.getMode() == ReadWriteUpdateLatch.EXCLUSIVE);
				assertTrue(latch.getOwner() == Thread.currentThread());
				assertTrue(latch.getSharedCount() == 0);
				latch.unlock();
			}
		});
		latch.exclusiveLock();
		t1.start();
		Thread.sleep(100);
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.EXCLUSIVE);
		assertTrue(latch.getOwner() == Thread.currentThread());
		assertTrue(latch.getSharedCount() == 0);
		latch.unlock();
		t1.join(200);
		assertTrue(!t1.isAlive());
	}

	public void testReadBlocksWrite() throws Exception {
		final ReadWriteUpdateLatch latch = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatch();
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				assertFalse(latch.tryExclusiveLock());
				latch.exclusiveLock();
				assertTrue(latch.getMode() == ReadWriteUpdateLatch.EXCLUSIVE);
				assertTrue(latch.getOwner() == Thread.currentThread());
				assertTrue(latch.getSharedCount() == 0);
				latch.unlock();
			}
		});
		latch.sharedLock();
		t1.start();
		Thread.sleep(100);
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.SHARED);
		assertTrue(latch.getOwner() == null);
		assertTrue(latch.getSharedCount() == 1);
		latch.unlock();
		t1.join(200);
		assertTrue(!t1.isAlive());
	}

	public void testReadBlocksUpgrade() throws Exception {
		final ReadWriteUpdateLatch latch = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatch();
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				assertTrue(latch.trySharedLock());
				assertTrue(latch.getMode() == ReadWriteUpdateLatch.SHARED);
				assertTrue(latch.getOwner() == null);
				assertTrue(latch.getSharedCount() == 1);
				try {
					Thread.sleep(300);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				latch.unlock();
			}
		});
		t1.start();
		Thread.sleep(100);
        latch.updateLock();
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.UPDATE);
		assertTrue(latch.getOwner() == Thread.currentThread());
		assertTrue(latch.getSharedCount() == 1);
		assertFalse(latch.tryUpgradeUpdateLock());
		latch.upgradeUpdateLock();
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.EXCLUSIVE);
		assertTrue(latch.getOwner() == Thread.currentThread());
		assertTrue(latch.getSharedCount() == 0);
		latch.unlock();
		assertTrue(latch.getMode() == ReadWriteUpdateLatch.FREE);
		t1.join(300);
		assertTrue(!t1.isAlive());
	}

	boolean testfailed = false;

	public void testReadLockFailsIfUpgradePending() throws Exception {
		final ReadWriteUpdateLatch latch = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatch();
		latch.sharedLock();
		testfailed = false;
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				if (!latch.tryUpdateLock()) {
					testfailed = true;
					return;
				}
				latch.upgradeUpdateLock();
				latch.unlock();
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
		latch.unlock();
		t1.join();
		t2.join();
		assertFalse(testfailed);
	}

	public void testReadLockFailsIfWriteLockPending() throws Exception {
		final ReadWriteUpdateLatch latch = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatch();
		latch.sharedLock();
		testfailed = false;
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				latch.exclusiveLock();
				latch.unlock();
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
		latch.unlock();
		t1.join();
		t2.join();
		assertFalse(testfailed);
	}

	/**
	 * read tryLock succeeds when readlocked
	 */
	public void testTryReadLockWhenReadLocked() throws Exception {
		final ReadWriteUpdateLatch latch = new ReadWriteUpdateLatch();
		latch.sharedLock();
		testfailed = false;
		Thread t = new Thread(new Runnable() {
			public void run() {
				if (latch.trySharedLock()) {
					latch.unlock();
				} else {
					testfailed = true;
				}
			}
		});
		t.start();
		t.join();
		latch.unlock();

		assertFalse(testfailed);
	}

	/**
	 * update tryLock succeeds when readlocked
	 */
	public void testTryUpdateLockWhenReadLocked() throws Exception {
		final ReadWriteUpdateLatch latch = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatch();
		latch.sharedLock();
		testfailed = false;
		Thread t = new Thread(new Runnable() {
			public void run() {
				if (latch.tryUpdateLock()) {
					latch.unlock();
				} else {
					testfailed = true;
				}
			}
		});
		t.start();
		t.join();
		latch.unlock();

		assertFalse(testfailed);
	}

    /**
     * Try read lock fails when update locked.
     */
    public void testTryReadLockFailsWhenUpdateLocked() throws Exception {
        final ReadWriteUpdateLatch latch = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatch();
        latch.updateLock();
        testfailed = false;
        Thread t = new Thread(new Runnable() {
            public void run() {
                if (latch.trySharedLock()) {
                    testfailed = true;
                    latch.unlock();
                }
            }
        });
        t.start();
        t.join();
        latch.unlock();

        assertFalse(testfailed);
    }
    
    /**
     * Read lock delayed when update locked.
     */
    public void testReadLockDelayedWhenUpdateLocked() throws Exception {
        final ReadWriteUpdateLatch latch = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatch();
        latch.updateLock();
        Thread t = new Thread(new Runnable() {
            public void run() {
                latch.sharedLock();
                latch.unlock();
            }
        });
        t.start();
        Thread.sleep(100);
        assertTrue(latch.getMode() == ReadWriteUpdateLatch.UPDATE);
        assertTrue(latch.getOwner() == Thread.currentThread());
        assertTrue(latch.getSharedCount() == 0);
        latch.unlock();
        t.join(300);
        assertTrue(!t.isAlive());
    }
    
	/**
	 * write tryLock fails when readlocked
	 */
	public void testUpdateTryLockWhenWriteLocked() throws Exception {
		final ReadWriteUpdateLatch latch = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatch();
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
		latch.unlock();

		assertFalse(testfailed);
	}

	/**
	 * write tryLock fails when readlocked
	 */
	public void testWriteTryLockWhenReadLocked() throws Exception {
		final ReadWriteUpdateLatch latch = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatch();
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
		latch.unlock();

		assertFalse(testfailed);
	}

	/**
	 *  write lockInterruptibly succeeds if lock free else is interruptible
	 */
	public void testWriteLockInterruptibly() throws Exception {
		final ReadWriteUpdateLatch lock = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatch();
		lock.exclusiveLock();
		testfailed = false;
		Thread t = new Thread(new Runnable() {
			public void run() {
				try {
					lock.exclusiveLockInterruptibly();
					testfailed = true;
				} catch (InterruptedException success) {
				}
			}
		});
		t.start();
		t.interrupt();
		t.join();
		lock.unlock();
		
		assertFalse(testfailed);
	}
	
	
	/**
	 *  read lockInterruptibly succeeds if lock free else is interruptible
	 */
	public void testReadLockInterruptibly() throws Exception {
		final ReadWriteUpdateLatch lock = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatch();
		lock.exclusiveLock();
		testfailed = false;
		Thread t = new Thread(new Runnable() {
			public void run() {
				try {
					lock.sharedLockInterruptibly();
					testfailed = true;
				} catch (InterruptedException success) {
				}
			}
		});
		t.start();
		t.interrupt();
		t.join();
		lock.unlock();
		
		assertFalse(testfailed);
	}

	public void testReentrantWriteLock() throws Exception {
		final ReadWriteUpdateLatch lock = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatch();
		lock.exclusiveLock();
		assertTrue(lock.tryExclusiveLock());
		assertEquals(lock.getExclusiveCount(), 2);
		lock.unlock();
		lock.unlock();
		assertEquals(lock.getExclusiveCount(), 0);
	}

    /**
     * Read lock succeeds if write locked by current thread even if
     * other threads are waiting for readlock
     */
    public void testReadHoldingWriteLock2()  throws Exception { 
		final ReadWriteUpdateLatch lock = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatch();
		lock.exclusiveLock();
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				lock.sharedLock();
				lock.unlock();
			}
		});
		Thread t2 = new Thread(new Runnable() {
			public void run() {
				lock.sharedLock();
				lock.unlock();
			}
		});
		
		t1.start();
		t2.start();
		lock.sharedLock();
		lock.unlock();
		Thread.sleep(100);
		lock.sharedLock();
		lock.unlock();
		lock.unlock();
		t1.join(200);
		t2.join(200);
		assertTrue(!t1.isAlive());
		assertTrue(!t2.isAlive());
	}  	
	
    /**
     *  Read lock succeeds if write locked by current thread even if
     * other threads are waiting for writelock
     */
    public void testReadHoldingWriteLock3() throws Exception { 
		final ReadWriteUpdateLatch lock = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatch();
		lock.exclusiveLock();
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				lock.updateLock();
				lock.unlock();
			}
		});
		Thread t2 = new Thread(new Runnable() {
			public void run() {
				lock.exclusiveLock();
				lock.unlock();
			}
		});
		
		t1.start();
		t2.start();
		lock.sharedLock();
		lock.unlock();
		Thread.sleep(100);
		lock.sharedLock();
		lock.unlock();
		lock.unlock();
		t1.join(200);
		t2.join(200);
		assertTrue(!t1.isAlive());
		assertTrue(!t2.isAlive());
	} 
 	
	
    /**
     *  Write lock succeeds if write locked by current thread even if
     * other threads are waiting for writelock
     */
	public void testWriteHoldingWriteLock4() throws Exception { 
		final ReadWriteUpdateLatch lock = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatch();
		lock.exclusiveLock();
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				lock.exclusiveLock();
				lock.unlock();
			}
		});
		Thread t2 = new Thread(new Runnable() {
			public void run() {
				lock.exclusiveLock();
				lock.unlock();
			}
		});
		
		t1.start();
		t2.start();
		lock.exclusiveLock();
		lock.unlock();
		Thread.sleep(100);
		lock.exclusiveLock();
		lock.unlock();
		lock.unlock();
		t1.join(200);
		t2.join(200);
		assertTrue(!t1.isAlive());
		assertTrue(!t2.isAlive());
	} 
	
}
