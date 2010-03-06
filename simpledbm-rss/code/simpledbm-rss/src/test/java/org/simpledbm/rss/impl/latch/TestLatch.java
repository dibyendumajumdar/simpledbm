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
package org.simpledbm.rss.impl.latch;

import java.util.Properties;

import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.impl.platform.PlatformImpl;
import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.rss.api.latch.Latch;

/**
 * 
 * @author Dibyendu Majumdar
 * @since Aug 22, 2005
 */
public class TestLatch extends BaseTestCase {

    static Properties properties;
	static Platform platform;
	static LatchFactoryImpl latchFactory;

	static {
		properties = new Properties();
		properties.setProperty("logging.properties.file",
				"classpath:simpledbm.logging.properties");
		properties.setProperty("logging.properties.type", "log4j");
		platform = new PlatformImpl(properties);
		latchFactory = new LatchFactoryImpl(platform, properties);
	}
	
	public TestLatch(String arg0) {
		super(arg0);
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
	 * Try read lock fails when update locked.
         * NOTE: Works when SHARED is NOT compatible with UPDATE
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
	 * Try read lock succeeds when update locked.
         * NOTE Works when SHARED IS compaibel with UPDATE
	 */
	public void testTryReadLockFailsWhenUpdateLocked() throws Exception {
		final Latch latch = latchFactory.newReadWriteUpdateLatch();
		latch.updateLock();
		testfailed = false;
		Thread t = new Thread(new Runnable() {
			public void run() {
				if (!latch.trySharedLock()) {
					testfailed = true;
				}
                                else {
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

	/**
	 * write lockInterruptibly succeeds if lock free else is interruptible
	 */
	public void testWriteLockInterruptibly() throws Exception {
		final ReadWriteUpdateLatch lock = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatchV1();
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
	 * read lockInterruptibly succeeds if lock free else is interruptible
	 */
	public void testReadLockInterruptibly() throws Exception {
		final ReadWriteUpdateLatch lock = (ReadWriteUpdateLatch) latchFactory.newReadWriteUpdateLatchV1();
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

}
