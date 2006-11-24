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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.simpledbm.rss.api.locking.LockException;
import org.simpledbm.rss.api.locking.LockMode;

/**
 * The default implementation of the LockMgr interface is a memory based lock
 * management system modeled very closely on the description of a locking system
 * in <em>Transaction Processing: Concepts and Techniques, by Jim Gray and
 * Andreas Reuter</em>.
 * <p>
 * For each lock in the system, a queue of lock requests is maintained. The
 * queue has granted requests followed by waiting requests. To allow locks to be
 * quickly found, a hash table of all locks is maintained.
 * </p>
 * 
 * @author Dibyendu Majumdar
 */
public final class LockManagerImpl extends BaseLockManagerImpl {

	/**
	 * Creates a new LockMgrImpl, ready for use.
	 * 
	 * @param hashTableSize
	 *            The size of the lock hash table.
	 */
	public LockManagerImpl(int hashTableSize) {
		super(hashTableSize);
	}


	/**
	 * Acquires a lock in the specified mode. Handles most of the cases except
	 * the case where an INSTANT_DURATION lock needs to be waited for. This case
	 * requires the lock to be released after it has been granted; the lock
	 * release is handled by {@link #acquire acquire}.
	 * 
	 * <p>
	 * Algorithm:
	 * 
	 * <ol>
	 * <li>Search for the lock. </li>
	 * <li>If not found, this is a new lock and therefore grant the lock, and
	 * return success. </li>
	 * <li>Else check if requesting transaction already has a lock request.
	 * </li>
	 * <li>If not, this is the first request by the transaction. If yes, goto
	 * 11.</li>
	 * <li>Check if lock can be granted. This is true if there are no waiting
	 * requests and the new request is compatible with existing grant mode.
	 * </li>
	 * <li>If yes, grant the lock and return success. </li>
	 * <li>Otherwise, if nowait was specified, return failure. </li>
	 * <li>Otherwise, wait for the lock to be available/compatible. </li>
	 * <li>If after the wait, the lock has been granted, then return success.
	 * </li>
	 * <li> Else return failure.
	 * 
	 * </li>
	 * <li>If calling transaction already has a granted lock request then this
	 * must be a conversion request. </li>
	 * <li> Check whether the new request lock is same mode as previously held
	 * lock. </li>
	 * <li>If so, grant lock and return. </li>
	 * <li>Otherwise, check if requested lock is compatible with granted group.
	 * </li>
	 * <li>If so, grant lock and return. </li>
	 * <li>If not, and nowait specified, return failure. </li>
	 * <li>Goto 8. </li>
	 * </ol>
	 * </p>
	 */
	protected LockHandleImpl doAcquire(LockState lockState) throws LockException {

		if (log.isDebugEnabled()) {
			log.debug(LOG_CLASS_NAME, "acquire", "Lock requested by " + lockState.parms.owner
					+ " for " + lockState.parms.target + ", mode=" + lockState.parms.mode + ", duration="
					+ lockState.parms.duration);
		}

		lockState.handle = new LockHandleImpl(this, lockState.parms);
		lockState.converting = false;
		lockState.prevThread = Thread.currentThread();

		/* 1. Search for the lock. */
		int h = lockState.parms.target.hashCode() % hashTableSize;
		lockState.lockitem = null;
		lockState.bucket = LockHashTable[h];
		lockState.r = null;

		((ExtendedLockBucket)lockState.bucket).lock();
		try {
			lockState.lockitem = findLock(lockState);
			if (lockState.lockitem == null) {
				/*
				 * 2. If not found, this is a new lock and therefore grant the
				 * lock, and return success.
				 */
				handleNewLock(lockState);
				return lockState.handle;
			}
			/*
			 * 3. Else check if requesting transaction already has a lock
			 * request.
			 */
			lockState.r = lockState.lockitem.find(lockState.parms.owner);

			if (lockState.r == null) {
				if (handleNewRequest(lockState)) {
					return lockState.handle;
				}
			} else {
				if (handleConversionRequest(lockState)) {
					return lockState.handle;
				}
			}

			/* 8. Wait for the lock to be available/compatible. */
			prepareToWait(lockState);
		} finally {
			((ExtendedLockBucket)lockState.bucket).unlock();
		}
		notifyLockEventListeners();
		LockWaiter waiter = new LockWaiter(lockState.r, Thread.currentThread());
		waiters.put(lockState.r.owner, waiter);
		globalLock.unlockShared();
		long then = System.nanoTime();
		long timeToWait = lockState.parms.timeout;
		if (timeToWait != -1) {
			timeToWait = TimeUnit.NANOSECONDS.convert(
					lockState.parms.timeout, TimeUnit.SECONDS);
		}
		for (;;) {
			try {
				if (lockState.parms.timeout == -1) {
					LockSupport.park();
				} else {
					LockSupport.parkNanos(timeToWait);
				}
			} finally {
				globalLock.sharedLock();
			}
			long now = System.nanoTime();
			if (timeToWait > 0) {
				timeToWait -= (now-then);
				then = now;
			}
			/*
			 * As the hash table may have been resized while we were waiting,
			 * we need to recalculate the bucket.
			 */
			h = lockState.parms.target.hashCode() % hashTableSize;
			lockState.bucket = LockHashTable[h];		
			((ExtendedLockBucket)lockState.bucket).lock();
			if (lockState.r.status == LockRequestStatus.WAITING || lockState.r.status == LockRequestStatus.CONVERTING) {
				if (timeToWait > 0 || lockState.parms.timeout == -1) {
					System.err.println("Need to retry as this was a spurious wakeup");
					((ExtendedLockBucket)lockState.bucket).unlock();
					globalLock.unlockShared();
					continue;
				}
			}
			break;
		} 
		try {
			waiters.remove(lockState.r.owner);
			handleWaitResult(lockState);
			return lockState.handle;
		}
		finally {
			((ExtendedLockBucket)lockState.bucket).unlock();
		}
	}

	boolean doReleaseInternal(LockState lockState) throws LockException {
		lockState.r = null;

		if (log.isDebugEnabled()) {
			log.debug(LOG_CLASS_NAME, "doReleaseInternal", "Request by " + lockState.parms.owner
					+ " to release lock for " + lockState.parms.target);
		}
		int h = lockState.parms.target.hashCode() % hashTableSize;
		lockState.bucket = LockHashTable[h];
		((ExtendedLockBucket)lockState.bucket).lock();
		try {
			/* 1. Search for the lock. */
			lockState.lockitem = findLock(lockState);

			if (lockState.lockitem == null) {
				/* 2. If not found, return success. */
				if (log.isDebugEnabled()) {
					log.debug(LOG_CLASS_NAME, "doReleaseInternal",
							"lock not found, returning success");
				}
				throw new LockException(
						"SIMPLEDBM-ELOCK-003: Cannot release a lock on "
								+ lockState.parms.target
								+ " as it is is not locked at present; seems like invalid call to release lock");
			}
			return releaseLock(lockState);
		} finally {
			((ExtendedLockBucket) lockState.bucket).unlock();
		}
	}

	static final class ExtendedLockBucket extends LockBucket {

		final Lock lock = new ReentrantLock();

		void lock() {
			lock.lock();
		}

		void unlock() {
			lock.unlock();
		}
	}

	LockBucket getNewLockBucket() {
		return new ExtendedLockBucket();
	}
	
	LockItem getNewLockItem(Object target, LockMode mode) {
		return new LockItem(target, mode);
	}


}

