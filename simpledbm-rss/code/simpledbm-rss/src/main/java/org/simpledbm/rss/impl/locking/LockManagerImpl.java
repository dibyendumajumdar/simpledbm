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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.simpledbm.rss.api.latch.Latch;
import org.simpledbm.rss.api.locking.LockDeadlockException;
import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockException;
import org.simpledbm.rss.api.locking.LockHandle;
import org.simpledbm.rss.api.locking.LockInfo;
import org.simpledbm.rss.api.locking.LockManager;
import org.simpledbm.rss.api.locking.LockMode;
import org.simpledbm.rss.api.locking.LockTimeoutException;
import org.simpledbm.rss.impl.latch.ReadWriteUpdateLatch;
import org.simpledbm.rss.util.logging.Logger;

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
public final class LockManagerImpl implements LockManager {

	private static final String LOG_CLASS_NAME = LockManagerImpl.class.getName();

    private static final Logger log = Logger.getLogger(LockManagerImpl.class.getPackage().getName());

	static final int hashPrimes[] = {
		53, 97, 193, 389, 769, 1543, 3079, 6151, 12289, 24593, 49157,
		98317, 196613, 393241, 786433
	};
	
	private volatile int htsz = 0;
	
	/**
	 * Tracks the number of items in the hash table
	 */
	private volatile int count = 0;
	
	/**
	 * Upper limit of number of items that can be inserted into the
	 * hash table. Exceeding this causes the hash table to be resized.
	 */
	private volatile int threshold = 0;
	
	/**
	 * Used to calculate the threshold. Expressed as a percentage of hash table size
	 */
	private float loadFactor = 0.75f;
	
	/**
	 * Hash table of locks.
	 */
	private LockBucket[] LockHashTable;

	/**
	 * Size of the hash table.
	 */
	private int hashTableSize;

	/**
	 * List of lock event listeners
	 */
	private final ArrayList<LockEventListener> lockEventListeners = new ArrayList<LockEventListener>();
	
	/**
	 * Map of waiting lock requesters, to aid deadlock detection.
	 * Keyed by lock object.
	 */
	private final Map<Object, LockWaiter> waiters = Collections.synchronizedMap(new HashMap<Object, LockWaiter>());
	
	//FIXME Need to create the latch using the factory
	/**
	 * To keep the algorithm simple, the deadlock detector uses a global exclusive lock
	 * on the lock manager. The lock manager itself acquires shared locks during normal operations,
	 * thus avoiding conflict with the deadlock detector.
	 */
	private final Latch globalLock = new ReadWriteUpdateLatch(); 
	
	/**
	 * Defines the various lock release methods.
	 * 
	 * @author Dibyendu Majumdar
	 */
	enum ReleaseAction {
		RELEASE, FORCE_RELEASE, DOWNGRADE;
	}

	/**
	 * Creates a new LockMgrImpl, ready for use.
	 * 
	 * @param hashTableSize
	 *            The size of the lock hash table.
	 */
	public LockManagerImpl(int hashTableSize) {
		// this.hashTableSize = hashTableSize;
		this.hashTableSize = hashPrimes[htsz];
		LockHashTable = new LockBucket[hashTableSize];
		for (int i = 0; i < hashTableSize; i++) {
			LockHashTable[i] = getNewLockBucket();
		}
		threshold = (int)(hashTableSize * loadFactor);
	}

	/**
	 * Grow the hash table to the next size
	 */
	private void rehash() {

		if (htsz == hashPrimes.length-1) {
			return;
		}
		globalLock.exclusiveLock();
		try {
			if (htsz == hashPrimes.length-1) {
				return;
			}
			int newHashTableSize = hashPrimes[++htsz];
			// System.out.println("Growing hash table size from " + hashTableSize + " to " + newHashTableSize);
			LockBucket[] newLockHashTable = new LockBucket[newHashTableSize];
			for (int i = 0; i < newHashTableSize; i++) {
				newLockHashTable[i] = getNewLockBucket();
			}
			for (int i = 0; i < hashTableSize; i++) {
				LockBucket bucket = LockHashTable[i];
				for (Iterator<LockItem> iter = bucket.chain.iterator(); iter.hasNext();) {
					LockItem item = iter.next();
					if (item.target == null) {
						continue;
					}
					int h = item.target.hashCode() % newHashTableSize;
					// System.out.println("Moving lock item " + item + " from old bucket " + i + " to new bucket " + h);
					LockBucket newBucket = newLockHashTable[h];
					newBucket.chainAppend(item);
				}
			}
			LockHashTable = newLockHashTable;
			hashTableSize = newHashTableSize;
			threshold = (int)(hashTableSize * loadFactor);
			for (int i = 0; i < hashTableSize; i++) {
				LockBucket bucket = LockHashTable[i];
				bucket.chain.clear();
				LockHashTable[i] = null;
			}
		}
//		catch (Exception e) {
//			e.printStackTrace();
//		}
		finally {
			globalLock.unlockExclusive();
		}
	}
	
    /**
     * Checks whether the specified lock request is compatible with the granted group.
     * Also sets the otherHolders flag if the granted group contains other requests.
     */
	private boolean checkCompatible(LockItem lock, LockRequest request, LockMode mode, LockInfo lockInfo) {

        if (lockInfo != null) {
            lockInfo.setHeldByOthers(false);
        }
		boolean iscompatible = true;

		/* Check if there are other holders */
		for (LockRequest other : lock.getQueue()) {

			if (other == request)
				continue;
			else if (other.status == LockRequestStatus.WAITING)
				break;
			else {
                if (lockInfo != null) {
                    lockInfo.setHeldByOthers(true);
                }
				if (!mode.isCompatible(other.mode)) {
					iscompatible = false;
					break;
				}
			}
		}
		return iscompatible;
	}
	
	/**
	 * Holds parameters supplied to aquire, release or find APIs. 
	 */
	static final class LockParams {
		Object owner;
		Object lockable;
		LockMode mode;
		LockDuration duration;
		int timeout;
		LockInfo lockInfo;
		
		LockMode downgradeMode;
		ReleaseAction action;
	}

	static final class LockState {
		final LockParams parms;
		LockHandleImpl handle;
		LockRequest lockRequest;
		LockBucket bucket;
		boolean converting;
		Thread prevThread;
		LockItem lockitem;
		private LockStatus status;
	
		public LockState(LockParams parms) {
			this.parms = parms;
		}

		void setStatus(LockStatus status) {
			this.status = status;
		}

		LockStatus getStatus() {
			return status;
		}
	}

	/**
	 * Handles the case where there aren't any locks on the target.
	 */
	private void handleNewLock(LockState lockState) {
		if (log.isDebugEnabled()) {
			log.debug(LOG_CLASS_NAME, "acquire",
					"Lock not found, therefore granting immediately");
		}
		if (lockState.parms.duration != LockDuration.INSTANT_DURATION) {
			LockItem lockitem = getNewLockItem(lockState.parms.lockable, lockState.parms.mode);
			LockRequest r = new LockRequest(lockitem, lockState.parms.owner, lockState.parms.mode, lockState.parms.duration);
			lockitem.queueAppend(r);
			lockState.bucket.chainAppend(lockitem);
			count++;
			lockState.handle.lockRequest = r;
			lockState.setStatus(LockStatus.GRANTED);
		} else {
			lockState.setStatus(LockStatus.GRANTABLE);
		}
	}
	
	/**
	 * Handles the result of a lock wait. 
	 */
	private void handleWaitResult(LockState lockState) throws LockTimeoutException, LockDeadlockException, LockException {
		LockRequestStatus lockRequestStatus = lockState.lockRequest.status;
		if (lockRequestStatus == LockRequestStatus.GRANTED) {
			lockState.setStatus(LockStatus.GRANTED);
		} else if (lockRequestStatus == LockRequestStatus.DENIED) {
			lockState.setStatus(LockStatus.DEADLOCK);
		} else {
			lockState.setStatus(LockStatus.TIMEOUT);
		}

		if (lockState.getStatus() == LockStatus.GRANTED) {
			/*
			 * 9. If after the wait, the lock has been granted, then return
			 * success.
			 */
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "handleWaitResult",
						"Woken up, and lock granted");
			}
			checkCompatible(lockState.lockitem, lockState.lockRequest, lockState.parms.mode, lockState.parms.lockInfo);
			// lockState.handle.setStatus(lockState.lockRequest, LockStatus.GRANTED);
			// TODO lockState.setRequest()
			return;
		}

		/* 10. Else return failure. */
		if (log.isDebugEnabled()) {
			log.debug(LOG_CLASS_NAME, "handleWaitResult",
					"Woken up, and lock failed");
		}

		if (!lockState.converting) {
			/* If not converting the delete the newly created request. */
			lockState.lockitem.queueRemove(lockState.lockRequest);
			if (lockState.lockitem.queueIsEmpty()) {
				lockState.lockitem.reset(); // Setup lock for garbage collection
				count--;
			}
		} else {
			/* If converting, then restore old status */
			lockState.lockRequest.status = LockRequestStatus.GRANTED;
			lockState.lockRequest.convertMode = lockState.lockRequest.mode;
			lockState.lockRequest.thread = lockState.prevThread;
		}
		if (lockState.getStatus() == LockStatus.DEADLOCK) {
			/* 
			 * If we have been chosen as a deadlock victim, then we need to grant the
			 * lock to the waiter who has won the deadlock.
			 */
			grantWaiters(ReleaseAction.RELEASE, lockState.lockRequest, lockState.handle, lockState.lockitem, lockState.parms.lockInfo);
		}
		if (lockState.getStatus() == LockStatus.TIMEOUT)
			throw new LockTimeoutException("SIMPLEDBM-ELOCK: Lock request " + lockState.parms.toString() + " timed out");
		else if (lockState.getStatus() == LockStatus.DEADLOCK)
			throw new LockDeadlockException("SIMPLEDBM-ELOCK: Lock request " + lockState.parms.toString() + " failed due to a deadlock");
		else
			throw new LockException(
					"SIMPLEDBM-ELOCK-002: Unexpected error occurred while attempting to acqure lock " + lockState.parms.toString());
	}

	/**
	 * Prepare the lock request for waiting.
	 */
	private void prepareToWait(LockState lockState) {
		lockState.lockitem.waiting = true;
		if (!lockState.converting) {
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "prepareToWait",
						"Waiting for lock to be free");
			}
			lockState.lockRequest.status = LockRequestStatus.WAITING;
		} else {
			if (log.isDebugEnabled()) {
				log
						.debug(LOG_CLASS_NAME, "prepareToWait",
								"Conversion NOT compatible with granted group, therefore waiting ...");
			}
			lockState.lockRequest.convertMode = lockState.parms.mode;
			lockState.lockRequest.convertDuration = lockState.parms.duration;
			lockState.lockRequest.status = LockRequestStatus.CONVERTING;
			lockState.prevThread = lockState.lockRequest.thread;
			lockState.lockRequest.thread = Thread.currentThread();
		}
	}

	/**
	 * Handles a conversion request in the nowait situation. 
	 * @return true if conversion request was handled else false to indicate that requester must enter wait.
	 */
	private boolean handleConversionRequest(LockState lockState) throws LockException, LockTimeoutException {
		/*
		 * 11. If calling transaction already has a granted lock request
		 * then this must be a conversion request.
		 */
		if (log.isTraceEnabled()) {
			log.trace(LOG_CLASS_NAME, "handleConversionRequest",
					"Lock conversion request by transaction " + lockState.parms.owner
							+ " for target " + lockState.parms.lockable);
		}

		/*
		 * Limitation: a transaction cannot attempt to lock an object
		 * for which it is already waiting.
		 */
		if (lockState.lockRequest.status == LockRequestStatus.CONVERTING
				|| lockState.lockRequest.status == LockRequestStatus.WAITING) {
			throw new LockException(
					"SIMPLEDBM-ELOCK-001: Requested lock is already being waited for by requestor");
		}

		else if (lockState.lockRequest.status == LockRequestStatus.GRANTED) {
			/*
			 * 12. Check whether the new request lock is same mode as
			 * previously held lock.
			 */
			if (lockState.parms.lockInfo != null) {
				lockState.parms.lockInfo.setPreviousMode(lockState.lockRequest.mode);
			}
			if (lockState.parms.mode == lockState.lockRequest.mode) {
				/* 13. If so, grant lock and return. */
				if (log.isDebugEnabled()) {
					log
							.debug(LOG_CLASS_NAME, "handleConversionRequest",
									"Requested mode is the same as currently held mode, therefore granting");
				}
				if (lockState.parms.duration != LockDuration.INSTANT_DURATION) {
					lockState.lockRequest.count++;
				}
				checkCompatible(lockState.lockitem, lockState.lockRequest, lockState.parms.mode, lockState.parms.lockInfo);
				if (lockState.parms.duration == LockDuration.INSTANT_DURATION) {
					// lockState.handle.setStatus(lockState.lockRequest, LockStatus.GRANTABLE);
					lockState.setStatus(LockStatus.GRANTABLE);
				} else {
					// lockState.handle.setStatus(lockState.lockRequest, LockStatus.GRANTED);
					lockState.setStatus(LockStatus.GRANTED);
				}
				return true;
			}

			else {
				/*
				 * 14. Otherwise, check if requested lock is compatible
				 * with granted group
				 */
				boolean can_grant = checkCompatible(lockState.lockitem, lockState.lockRequest, lockState.parms.mode,
						lockState.parms.lockInfo);

				if (can_grant) {
					/* 13. If so, grant lock and return. */
					if (log.isDebugEnabled()) {
						log.debug(LOG_CLASS_NAME, "handleConversionRequest",
								"Conversion request is compatible with granted group "
										+ lockState.lockitem
										+ ", therefore granting");
					}
					if (lockState.parms.duration != LockDuration.INSTANT_DURATION) {
						lockState.lockRequest.mode = lockState.parms.mode.maximumOf(lockState.lockRequest.mode);
						lockState.lockRequest.count++;
						lockState.lockitem.grantedMode = lockState.lockRequest.mode
								.maximumOf(lockState.lockitem.grantedMode);
						//lockState.handle.setStatus(lockState.lockRequest, LockStatus.GRANTED);
						lockState.setStatus(LockStatus.GRANTED);
					} else {
						//lockState.handle
						//		.setStatus(lockState.lockRequest, LockStatus.GRANTABLE);
						lockState.setStatus(LockStatus.GRANTABLE);
					}
					return true;
				}

				else if (!can_grant && lockState.parms.timeout == 0) {
					/* 15. If not, and nowait specified, return failure. */
					if (log.isDebugEnabled()) {
						log.debug(LOG_CLASS_NAME, "handleConversionRequest",
								"Conversion request is not compatible with granted group "
										+ lockState.lockitem
										+ ", TIMED OUT since NOWAIT");
					}
					throw new LockTimeoutException(
							"Conversion request is not compatible with granted group "
									+ lockState.lockitem
									+ ", TIMED OUT since NOWAIT");
				}

				else {
					lockState.converting = true;
					return false;
				}
			}
		}
		else {
			throw new RuntimeException("Unexpected error occurred while handling a lock conversion request");
		}
	}

	/**
	 * Handles a new lock request when the lock is already held by some other.
	 * @return true if the lock request was processed, else false to indicate that the requester must 
	 * 		wait
	 */
	private boolean handleNewRequest(LockState lockState) throws LockTimeoutException {
		/* 4. If not, this is the first request by the transaction. */
		if (log.isDebugEnabled()) {
			log.debug(LOG_CLASS_NAME, "handleNewRequest",
					"New request by transaction " + lockState.parms.owner
							+ " for target " + lockState.parms.lockable);
		}

		if (lockState.parms.lockInfo != null) {
			lockState.parms.lockInfo.setHeldByOthers(true);
		}
		/*
		 * 5. Check if lock can be granted. This is true if there are no
		 * waiting requests and the new request is compatible with
		 * existing grant mode.
		 */
		boolean can_grant = (!lockState.lockitem.waiting && lockState.parms.mode
				.isCompatible(lockState.lockitem.grantedMode));

		if (lockState.parms.duration == LockDuration.INSTANT_DURATION && can_grant) {
			/* 6. If yes, grant the lock and return success. */
			// lockState.handle.setStatus(lockState.lockRequest, LockStatus.GRANTABLE);
			lockState.setStatus(LockStatus.GRANTABLE);
			return true;
		}

		else if (!can_grant && lockState.parms.timeout == 0) {
			/* 7. Otherwise, if nowait was specified, return failure. */
			if (log.isDebugEnabled()) {
				log
						.debug(
								LOG_CLASS_NAME,
								"handleNewRequest",
								"Lock "
										+ lockState.lockitem
										+ " is not compatible with requested mode, TIMED OUT since NOWAIT specified");
			}
			throw new LockTimeoutException(
					"Lock "
							+ lockState.lockitem
							+ " is not compatible with requested mode, TIMED OUT since NOWAIT specified");
		}

		/* Allocate new lock request */
		lockState.lockRequest = new LockRequest(lockState.lockitem, lockState.parms.owner, lockState.parms.mode, lockState.parms.duration);
		lockState.lockitem.queueAppend(lockState.lockRequest);
		lockState.handle.lockRequest = lockState.lockRequest;
		if (can_grant) {
			/* 6. If yes, grant the lock and return success. */
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "handleNewRequest",
						"There are no waiting locks and request is compatible with  "
								+ lockState.lockitem
								+ ", therefore granting lock");
			}
			lockState.lockitem.grantedMode = lockState.parms.mode.maximumOf(lockState.lockitem.grantedMode);
			// lockState.handle.setStatus(lockState.lockRequest, LockStatus.GRANTED);
			lockState.setStatus(LockStatus.GRANTED);
			return true;
		} else {
			lockState.converting = false;
			return false;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.locking.LockMgr#acquire(java.lang.Object,
	 *      java.lang.Object, org.simpledbm.locking.LockMode,
	 *      org.simpledbm.locking.LockDuration, int)
	 */
	public final LockHandle acquire(Object owner, Object target, LockMode mode, LockDuration duration, int timeout, LockInfo lockInfo) throws LockException {

		LockParams parms = new LockParams();
		parms.owner = owner;
		parms.lockable = target;
		parms.mode = mode;
		parms.duration = duration;
		parms.timeout = timeout;
		parms.lockInfo = lockInfo;
		
		LockState lockState = new LockState(parms);
		
		if (count > threshold) {
			rehash();
		}
		
		globalLock.sharedLock();
		LockHandleImpl handle = null;
		try {
			handle = doAcquire(lockState);
			if (duration == LockDuration.INSTANT_DURATION
					&& lockState.getStatus() == LockStatus.GRANTED) {
				/*
				 * Handle the case where the lock was granted after a wait.
				 */
				// System.err.println("Releasing instant lock " + handle);
				handle.release(false);
			}
		} finally {
			globalLock.unlockShared();
		}
		return handle;
	}

	public final LockMode findLock(Object owner, Object target) {

		LockParams parms = new LockParams();
		parms.owner = owner;
		parms.lockable = target;
		
		LockState lockState = new LockState(parms);
		
		globalLock.sharedLock();
		try {
			/* 1. Search for the lock. */
			int h = lockState.parms.lockable.hashCode() % hashTableSize;
			lockState.lockitem = null;
			lockState.bucket = LockHashTable[h];
			lockState.lockRequest = null;
			synchronized (lockState.bucket) {
				lockState.lockitem = findLock(lockState);
				if (lockState.lockitem == null) {
					return LockMode.NONE;
				}
				lockState.lockRequest = lockState.lockitem.find(lockState.parms.owner);
				if (lockState.lockRequest != null) {
					return lockState.lockRequest.mode;
				}
			}
		} finally {
			globalLock.unlockShared();
		}
		return LockMode.NONE;
	}

	
	/**
	 * Release or downgrade a specified lock.
	 * 
	 * <p>
	 * Algorithm:
	 * <ol>
	 * <li>1. Search for the lock. </li>
	 * <li>2. If not found, return Ok. </li>
	 * <li>3. If found, look for the transaction's lock request. </li>
	 * <li>4. If not found, return Ok. </li>
	 * <li>5. If lock request is in invalid state, return error. </li>
	 * <li>6. If noforce and not downgrading, and reference count greater than
	 * 0, then do not release the lock request. Decrement reference count and
	 * return Ok. </li>
	 * <li>7. If sole lock request and not downgrading, then release the lock
	 * and return Ok. </li>
	 * <li>8. If not downgrading, delete the lock request from the queue.
	 * Otherwise, downgrade the mode assigned to the lock request.
	 * 
	 * </li>
	 * <li>9. Recalculate granted mode by calculating max mode amongst all
	 * granted (including conversion) requests. 
	 * If a conversion request is compatible with all other granted requests,
	 * then grant the conversion, recalculating granted mode. If a waiting
	 * request is compatible with granted mode, and there are no pending
	 * conversion requests, then grant the request, and recalculate granted
	 * mode. Otherwise, we are done. </li>
	 * </ol>
	 * </p>
	 * <p>
	 * Note that this means that FIFO is respected
	 * for waiting requests, but conversion requests are granted as soon as they
	 * become compatible. Also, note that if a conversion request is pending,
	 * waiting requests cannot be granted.
	 * </p>
	 * </p>
	 */	
	private boolean releaseLock(LockState lockState) throws LockException {
		boolean released;
		/* 3. If lock found, look for the transaction's lock request. */
		lockState.lockRequest = lockState.lockitem.find(lockState.parms.owner);

		if (lockState.lockRequest == null) {
			/* 4. If not found, return success. */
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "release",
						"request not found, returning success");
			}
			throw new LockException(
					"SIMPLEDBM-ELOCK-003: Cannot release a lock on "
							+ lockState.parms.lockable
							+ " as it is is not locked at present; seems like invalid call to release lock");
		}

		if (lockState.lockRequest.status == LockRequestStatus.CONVERTING
				|| lockState.lockRequest.status == LockRequestStatus.WAITING) {
			/* 5. If lock in invalid state, return error. */
			if (log.isDebugEnabled()) {
				log
						.debug(LOG_CLASS_NAME, "release",
								"cannot release a lock request that is not granted");
			}
			throw new LockException(
					"SIMPLEDBM-ELOCK-004: Cannot release a lock that is being waited for");
		}

		if (lockState.parms.action == ReleaseAction.DOWNGRADE && lockState.lockRequest.mode == lockState.parms.downgradeMode) {
			/*
			 * If downgrade request and lock is already in target mode,
			 * return success.
			 */
			return false;
		}

		if (lockState.parms.action == ReleaseAction.RELEASE && lockState.lockRequest.count > 1) {
			/*
			 * 6. If noforce, and reference count greater than 0, then do
			 * not release the lock request. Decrement reference count if
			 * greater than 0, and, return Ok.
			 */
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "release",
						"count decremented but lock not released");
			}
			lockState.lockRequest.count--;
			return false;
		}

		/*
		 * Either the lock is being downgraded or it is being released and
		 * its reference count == 0 or it is being forcibly released.
		 */

		if (lockState.lockRequest == lockState.lockitem.queueHead() && lockState.lockRequest == lockState.lockitem.queueTail()
				&& lockState.parms.action != ReleaseAction.DOWNGRADE) {
			/* 7. If sole lock request, then release the lock and return Ok. */
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "release",
						"removing sole lock, releasing lock object");
			}
			lockState.lockitem.queueRemove(lockState.lockRequest);
			lockState.lockitem.reset();
			count--;
			return true;
		}

		/*
		 * 8. If not downgrading, delete the lock request from the queue.
		 * Otherwise, downgrade the mode assigned to the lock request.
		 */
		if (lockState.parms.action != ReleaseAction.DOWNGRADE) {
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "release",
						"Removing lock request " + lockState.lockRequest
								+ " and re-adjusting granted mode");
			}
			lockState.lockitem.queueRemove(lockState.lockRequest);
			released = true;
		} else {
			/*
			 * We need to determine whether is a valid downgrade request. To
			 * do so, we do a reverse check - ie, if the new mode could have
			 * been upgraded to current mode, then it is okay to downgrade.
			 */
			LockMode mode = lockState.parms.downgradeMode.maximumOf(lockState.lockRequest.mode);
			if (mode == lockState.lockRequest.mode) {
				if (log.isDebugEnabled()) {
					log.debug(LOG_CLASS_NAME, "release", "Downgrading " + lockState.lockRequest
							+ " to " + lockState.parms.downgradeMode
							+ " and re-adjusting granted mode");
				}
				lockState.lockRequest.convertMode = lockState.lockRequest.mode = lockState.parms.downgradeMode;
				if (lockState.parms.lockInfo != null) {
					lockState.parms.lockInfo.setPreviousMode(lockState.lockRequest.mode);
					// lockState.handle.setCurrentMode(lockState.parms.downgradeMode);
					lockState.parms.lockInfo.setHeldByOthers(false);
				}
			} else {
				throw new LockException(
						"SIMPLEDBM-ELOCK-005: Invalid downgrade request from "
								+ lockState.lockRequest.mode + " to " + lockState.parms.downgradeMode);
			}
			released = false;
		}
		/*
		 * 9. Recalculate granted mode by calculating max mode amongst all
		 * granted (including conversion) requests. If a conversion request
		 * is compatible with all other granted requests, then grant the
		 * conversion, recalculating granted mode. If a waiting request is
		 * compatible with granted mode, and there are no pending conversion
		 * requests, then grant the request, and recalculate granted mode.
		 * Otherwise, we are done. Note that this means that FIFO is
		 * respected for waiting requests, but conversion requests are
		 * granted as soon as they become compatible. Also, note that if a
		 * conversion request is pending, waiting requests cannot be
		 * granted.
		 */
		grantWaiters(lockState.parms.action, lockState.lockRequest, lockState.handle, lockState.lockitem, lockState.parms.lockInfo);
		return released;
	}

	private void grantWaiters(ReleaseAction action, LockRequest r, LockHandleImpl handleImpl, LockItem lockitem, LockInfo lockInfo) {
		/*
		 * 9. Recalculate granted mode by calculating max mode amongst all
		 * granted (including conversion) requests. If a conversion request
		 * is compatible with all other granted requests, then grant the
		 * conversion, recalculating granted mode. If a waiting request is
		 * compatible with granted mode, and there are no pending conversion
		 * requests, then grant the request, and recalculate granted mode.
		 * Otherwise, we are done. Note that this means that FIFO is
		 * respected for waiting requests, but conversion requests are
		 * granted as soon as they become compatible. Also, note that if a
		 * conversion request is pending, waiting requests cannot be
		 * granted.
		 */
		boolean converting;
		LockRequest myReq = r;
		lockitem.grantedMode = LockMode.NONE;
		lockitem.waiting = false;
		converting = false;
		for (LockRequest req : lockitem.getQueue()) {

			r = req;
			if (r.status == LockRequestStatus.GRANTED) {
				lockitem.grantedMode = r.mode.maximumOf(lockitem.grantedMode);
		        if (r != myReq && action == ReleaseAction.DOWNGRADE) {
		        	if (lockInfo != null) {
		        		lockInfo.setHeldByOthers(true);
		        	}
		        }
			}

			else if (r.status == LockRequestStatus.CONVERTING) {
				boolean can_grant;

				assert (!converting || lockitem.waiting);

				can_grant = checkCompatible(lockitem, r, r.convertMode, null);
				if (can_grant) {
					if (log.isDebugEnabled()) {
						log.debug(LOG_CLASS_NAME, "release", "Granting conversion request " + r + " because request is compatible with " + lockitem);
					}
		            if (r.convertDuration == LockDuration.INSTANT_DURATION) {
		                /*
		                 * If the request is for an instant duration lock then
		                 * don't perform the conversion.
		                 */
		                r.convertMode = r.mode; 
		            }
		            else {
		                r.mode = r.convertMode.maximumOf(r.mode);
		                r.convertMode = r.mode;
		                lockitem.grantedMode = r.mode.maximumOf(lockitem.grantedMode);
		            }
		            // TODO - TT1 
		            // System.err.println("Executed r.count++");
		            /*
		             * Treat conversions as lock recursion.
		             */
		            r.count++;
					r.status = LockRequestStatus.GRANTED;
					LockSupport.unpark(r.thread);
				} else {
					lockitem.grantedMode = r.mode.maximumOf(lockitem.grantedMode);
					converting = true;
					lockitem.waiting = true;
				}
			}

			else if (r.status == LockRequestStatus.WAITING) {
				if (!converting && r.mode.isCompatible(lockitem.grantedMode)) {
					if (log.isDebugEnabled()) {
						log.debug(LOG_CLASS_NAME, "release", "Granting waiting request " + r + " because not converting and request is compatible with " + lockitem);
					}
					r.status = LockRequestStatus.GRANTED;
		            lockitem.grantedMode = r.mode.maximumOf(lockitem.grantedMode);
					LockSupport.unpark(r.thread);
				} else {
					if (log.isDebugEnabled() && converting) {
						log.debug(LOG_CLASS_NAME, "release", "Cannot grant waiting request " + r + " because conversion request pending");
					}
					lockitem.waiting = true;
					break;
				}
			}
		}
	}

	/**
	 * Release or downgrade a specified lock.
	 * 
	 * <p>
	 * Algorithm:
	 * <ol>
	 * <li>1. Search for the lock. </li>
	 * <li>2. If not found, return Ok. </li>
	 * <li>3. If found, look for the transaction's lock request. </li>
	 * <li>4. If not found, return Ok. </li>
	 * <li>5. If lock request is in invalid state, return error. </li>
	 * <li>6. If noforce and not downgrading, and reference count greater than
	 * 0, then do not release the lock request. Decrement reference count and
	 * return Ok. </li>
	 * <li>7. If sole lock request and not downgrading, then release the lock
	 * and return Ok. </li>
	 * <li>8. If not downgrading, delete the lock request from the queue.
	 * Otherwise, downgrade the mode assigned to the lock request.
	 * 
	 * </li>
	 * <li>9. Recalculate granted mode by calculating max mode amongst all
	 * granted (including conversion) requests. 
	 * If a conversion request is compatible with all other granted requests,
	 * then grant the conversion, recalculating granted mode. If a waiting
	 * request is compatible with granted mode, and there are no pending
	 * conversion requests, then grant the request, and recalculate granted
	 * mode. Otherwise, we are done. </li>
	 * </ol>
	 * </p>
	 * <p>
	 * Note that this means that FIFO is respected
	 * for waiting requests, but conversion requests are granted as soon as they
	 * become compatible. Also, note that if a conversion request is pending,
	 * waiting requests cannot be granted.
	 * </p>
	 * </p>
	 */
	private boolean doRelease(LockHandle handle, ReleaseAction action, LockMode downgradeMode) throws LockException {
		
		LockParams parms = new LockParams();
		LockHandleImpl handleImpl = (LockHandleImpl) handle;
		parms.lockable = handleImpl.getLockable();
		parms.owner = handleImpl.getOwner();
		parms.action = action;
		parms.downgradeMode = downgradeMode;
		
		LockState lockState = new LockState(parms);
		lockState.handle = handleImpl;
		
		globalLock.sharedLock();
		try {
			return doReleaseInternal(lockState);
		}
		finally {
			globalLock.unlockShared();
		}
	}
	
	public boolean downgrade(Object owner, Object lockable, LockMode downgradeTo) throws LockException {
		LockParams parms = new LockParams();
		parms.lockable = lockable;
		parms.owner = owner;
		parms.action = ReleaseAction.DOWNGRADE;
		parms.downgradeMode = downgradeTo;
		
		LockState lockState = new LockState(parms);
		lockState.handle = new LockHandleImpl(this, lockState.parms);
		
		globalLock.sharedLock();
		try {
			return doReleaseInternal(lockState);
		}
		finally {
			globalLock.unlockShared();
		}
	}

	public boolean release(Object owner, Object lockable, boolean force) throws LockException {
		LockParams parms = new LockParams();
		parms.lockable = lockable;
		parms.owner = owner;
		parms.action = force ? ReleaseAction.FORCE_RELEASE : ReleaseAction.RELEASE;
		
		LockState lockState = new LockState(parms);
		lockState.handle = new LockHandleImpl(this, lockState.parms);
		
		globalLock.sharedLock();
		try {
			return doReleaseInternal(lockState);
		}
		finally {
			globalLock.unlockShared();
		}
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
	private LockHandleImpl doAcquire(LockState lockState)
			throws LockException {

		if (log.isDebugEnabled()) {
			log.debug(LOG_CLASS_NAME, "acquire", "Lock requested by " + lockState.parms.owner
					+ " for " + lockState.parms.lockable + ", mode=" + lockState.parms.mode + ", duration="
					+ lockState.parms.duration);
		}

		lockState.handle = new LockHandleImpl(this, lockState.parms);
		lockState.converting = false;
		lockState.prevThread = Thread.currentThread();

		/* 1. Search for the lock. */
		int h = lockState.parms.lockable.hashCode() % hashTableSize;
		lockState.lockitem = null;
		lockState.bucket = LockHashTable[h];
		lockState.lockRequest = null;
		synchronized (lockState.bucket) {

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
			lockState.lockRequest = lockState.lockitem.find(lockState.parms.owner);

			if (lockState.lockRequest == null) {
				if (handleNewRequest(lockState)) {
					return lockState.handle;
				}
			} else {
				lockState.handle.lockRequest = lockState.lockRequest;
				if (handleConversionRequest(lockState)) {
					return lockState.handle;
				}
			}

			/* 8. Wait for the lock to be available/compatible. */
			prepareToWait(lockState);
		}
		notifyLockEventListeners(lockState);
		LockWaiter waiter = new LockWaiter(lockState.lockRequest, Thread.currentThread());
		waiters.put(lockState.lockRequest.owner, waiter);
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
					LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(
							lockState.parms.timeout, TimeUnit.SECONDS));
				}
			} finally {
				globalLock.sharedLock();
			}
			long now = System.nanoTime();
			if (timeToWait > 0) {
				timeToWait -= (now - then);
				then = now;
			}
			/*
			 * As the hash table may have been resized while we were waiting, we
			 * need to recalculate the bucket.
			 */
			h = lockState.parms.lockable.hashCode() % hashTableSize;
			lockState.bucket = LockHashTable[h];
			synchronized (lockState.bucket) {
				if (lockState.lockRequest.status == LockRequestStatus.WAITING
						|| lockState.lockRequest.status == LockRequestStatus.CONVERTING) {
					if (timeToWait > 0 || lockState.parms.timeout == -1) {
						System.err
								.println("Need to retry as this was a spurious wakeup");
						continue;
					}
				}
				waiters.remove(lockState.lockRequest.owner);
				handleWaitResult(lockState);
				return lockState.handle;
			}
		}
	}

	private boolean doReleaseInternal(LockState lockState) throws LockException {
		lockState.lockRequest = null;
		boolean released = false;

		if (log.isDebugEnabled()) {
			log.debug(LOG_CLASS_NAME, "doReleaseInternal", "Request by " + lockState.parms.owner
					+ " to release lock for " + lockState.parms.lockable);
		}
		int h = lockState.parms.lockable.hashCode() % hashTableSize;
		lockState.bucket = LockHashTable[h];
		synchronized (lockState.bucket) {
			/* 1. Search for the lock. */
			lockState.lockitem = findLock(lockState);

			if (lockState.lockitem == null) {
				/* 2. If not found, return success. */
				throw new LockException(
						"SIMPLEDBM-ELOCK-003: Cannot release a lock on "
								+ lockState.parms.lockable
								+ " as it is is not locked at present; seems like invalid call to release lock");
			}
			released = releaseLock(lockState);
		}
		return released;
	}

	private LockBucket getNewLockBucket() {
		return new LockBucket();
	}
	
	private LockItem getNewLockItem(Object target, LockMode mode) {
		return new LockItem(target, mode);
	}

	public synchronized void addLockEventListener(LockEventListener listener) {
		lockEventListeners.add(listener);
	}
	
	public synchronized void clearLockEventListeners() {
		lockEventListeners.clear();
	}
	
	private void notifyLockEventListeners(LockState state) {
		for (LockEventListener listener: lockEventListeners) {
			try {
				listener.beforeLockWait(state.parms.owner, state.parms.lockable, state.parms.mode);
			}
			catch (Exception e) {
				log.error(LOG_CLASS_NAME, "notifyLockEventListeners", "Lister [" + listener + "] failed unexpectedly", e);
			}
		}
	}
	
	private boolean findDeadlockCycle(LockWaiter me) {
		if (me.visited) {
			return false;
		}
		else {
			me.visited = true;
		}
		LockWaiter him;
		
		LockMode mode = me.myLockRequest.mode;
		if (me.myLockRequest.status == LockRequestStatus.CONVERTING) {
			mode = me.myLockRequest.convertMode;
		}
		for (LockRequest them : me.myLockRequest.lockItem.queue) {
			if (them.status == LockRequestStatus.WAITING) {
				break;
			}
			if (them.status == LockRequestStatus.DENIED) {
				continue;
			}
			if (me.myLockRequest.status == LockRequestStatus.CONVERTING) {
				/*
				 * Need to check all holders of lock
				 */
				if (them.owner == me.myLockRequest.owner) {
					continue;
				}
			}
			else {
				/*
				 * No need to check locks after me
				 */
				if (them.owner == me.myLockRequest.owner) {
					break;
				}
			}
			boolean incompatible;
			if (me.myLockRequest.status == LockRequestStatus.CONVERTING) {
				incompatible = !them.mode.isCompatible(mode);
			} else {
				incompatible = !them.mode.isCompatible(me.myLockRequest.mode)
						|| them.status == LockRequestStatus.GRANTED
						|| them.status == LockRequestStatus.CONVERTING;
			}
			if (incompatible) {
				him = waiters.get(them.owner);
				me.cycle = him;
				if (him.cycle != null) {
					log.info(LOG_CLASS_NAME, "findDeadlockCycle", "DEADLOCK DETECTED: "
							+ me.myLockRequest + " waiting for "
							+ him.myLockRequest);
					log.info(LOG_CLASS_NAME, "findDeadlockCycle", " Other=> "
							+ him.myLockRequest.lockItem);
					log.info(LOG_CLASS_NAME, "findDeadlockCycle", " Victim=> " 
							+ me.myLockRequest.lockItem);
					me.myLockRequest.status = LockRequestStatus.DENIED;
					LockSupport.unpark(me.thread);
					return true;
				} else {
					return findDeadlockCycle(him);
				}
			}
		}
		return false;
	}
	
	void detectDeadlocks() {
		/*
		 * The deadlock detector is a very simple implementation
		 * based upon example shown in the Transaction Processing,
		 * by Jim Gray and Andreas Reuter.
		 * See sections 7.11.3 and section 8.5.
		 */
		globalLock.exclusiveLock();
		try {
			LockWaiter[] waiterArray = waiters.values().toArray(new LockWaiter[0]);
			for (LockWaiter waiter: waiterArray) {
				waiter.cycle = null;
				waiter.visited = false;
			}
			for (LockWaiter waiter: waiterArray) {
				findDeadlockCycle(waiter);
			}
		}
		finally {
			globalLock.unlockExclusive();
		}
	}
	
	/**
	 * Search for the specified lockable object.
	 * Garbage collect any items that are no longer needed.
	 */
	private LockItem findLock(LockState lockState) {
		for (Iterator<LockItem> iter = lockState.bucket.chain.iterator(); iter.hasNext();) {
			LockItem item = iter.next();
			if (item.target == null) {
				iter.remove();
				continue;
			}
			if (lockState.parms.lockable.equals(item.target)) {
				return item;
			}
		}
		return null;
	}

	static class LockBucket {

		final LinkedList<LockItem> chain = new LinkedList<LockItem>();

		LockItem chainHead() {
			return chain.getFirst();
		}

		LockItem chainTail() {
			return chain.getLast();
		}

		void chainAppend(LockItem item) {
			chain.add(item);
		}

		void chainRemove(LockItem item) {
			chain.remove(item);
		}

	}
	
	static class LockItem {

		Object target;

		final LinkedList<LockRequest> queue = new LinkedList<LockRequest>();

		LockMode grantedMode;

		boolean waiting;

		LockItem(Object target, LockMode mode) {
			this.target = target;
			this.grantedMode = mode;
		}

		void setLockMode(LockMode mode) {
			this.grantedMode = mode;
		}

		LockMode getLockMode() {
			return grantedMode;
		}

		Object getTarget() {
			return target;
		}

		boolean isWaiting() {
			return waiting;
		}

		void setWaiting(boolean waiting) {
			this.waiting = waiting;
		}

		void queueAppend(LockRequest request) {
			queue.add(request);
		}

		void queueRemove(LockRequest request) {
			queue.remove(request);
		}

		LockRequest queueHead() {
			return queue.getFirst();
		}

		LockRequest queueTail() {
			return queue.getLast();
		}

		boolean queueIsEmpty() {
			return queue.isEmpty();
		}

		LinkedList<LockRequest> getQueue() {
			return queue;
		}

		LockRequest find(Object owner) {
			for (LockRequest req : queue) {
				if (req.owner == owner || req.owner.equals(owner)) {
					return req;
				}
			}
			return null;
		}

		void reset() {
			target = null;
			grantedMode = LockMode.NONE;
		}

		@Override
		public String toString() {
			return "LockItem(target=" + target + ", grantedMode=" + grantedMode + ", waiting=" + waiting + ", queue=" + queue + ")";
		}
	}
	
	static enum LockRequestStatus {
		GRANTED, CONVERTING, WAITING, DENIED;
	}

	/**
	 * A LockRequest represents the request by a transaction for a lock.
	 * 
	 * @author Dibyendu Majumdar
	 * 
	 */
	static final class LockRequest {

		volatile LockRequestStatus status = LockRequestStatus.GRANTED;

		LockMode mode;

		LockMode convertMode;
        
        LockDuration convertDuration;

		short count = 1;

		final LockDuration duration;

		final Object owner;
		
		final LockItem lockItem;

		Thread thread;

		LockRequest(LockItem lockItem, Object owner, LockMode mode, LockDuration duration) {
			this.lockItem = lockItem;
			this.mode = mode;
			this.convertMode = mode;
			this.duration = duration;
			this.thread = Thread.currentThread();
			this.owner = owner;
		}

		Object getOwner() {
			return owner;
		}

		@Override
		public String toString() {
			return "LockRequest(mode=" + mode + ", convertMode=" + convertMode + ", status=" + status + ", count=" + count + ", owner=" + owner + ", thread=" + thread + ", duration=" + duration + ")";
		}
	}

    /**
     * LockHandleImpl is an implementation of LockHandle interface. Since LockHandles are
     * stored in Transactions, these handles need to be as compactly represented as possible.
     *
     * @author Dibyendu Majumdar
     * @since Nov 11, 2005
     */
    static final class LockHandleImpl implements LockHandle {

        private final LockManagerImpl lockMgr;

//		private final Object lockable;
//
//        private final Object owner;

        LockRequest lockRequest;
        
        // final LockMode mode;

        // final int timeout;

        // private LockMode previousMode = LockMode.NONE;

        // private boolean heldByOthers = false;

        // private LockMode currentMode = LockMode.NONE;

        //private LockStatus status;

		LockHandleImpl(LockManagerImpl lockMgr, LockParams parms) {
			this.lockMgr = lockMgr;
//			this.owner = parms.owner;
//			this.lockable = parms.lockable;
			// this.mode = parms.mode;
			// this.timeout = parms.timeout;
		}

//		final LockHandleImpl setStatus(LockRequest request, LockStatus status) {
//			if (request != null) {
//				currentMode = request.mode;
//			}
//			this.status = status;
//			return this;
//		}

		public final boolean release(boolean force) throws LockException {
			return lockMgr.doRelease(this, force ? LockManagerImpl.ReleaseAction.FORCE_RELEASE : LockManagerImpl.ReleaseAction.RELEASE, null);
			// return lockMgr.release(owner, lockable, force);
		}

		public final void downgrade(LockMode mode) throws LockException {
			lockMgr.doRelease(this, LockManagerImpl.ReleaseAction.DOWNGRADE, mode);
			// lockMgr.downgrade(owner, lockable, mode);
		}

//        public final LockMode getPreviousMode() {
//            return previousMode;
//        }
//
//        public final boolean isHeldByOthers() {
//            return heldByOthers;
//        }

        public final LockMode getCurrentMode() {
        	if (lockRequest == null) {
        		throw new IllegalStateException("Invalid call: no lock request associated with this handle");
        	}
            return lockRequest.mode;
        }

//        final LockStatus getStatus() {
//            return status;
//        }

//        final void setCurrentMode(LockMode currentMode) {
//            this.currentMode = currentMode;
//        }

//        final void setHeldByOthers(boolean heldByOthers) {
//            this.heldByOthers = heldByOthers;
//        }
//
//        final void setPreviousMode(LockMode previousMode) {
//            this.previousMode = previousMode;
//        }
//
        @Override
        public String toString() {
            return "LockHandleImpl(owner=" + getOwner() + ", target=" + getLockable() + ", currentMode=" + getCurrentMode() + ")";
        }

		Object getLockable() {
			// return lockable;
			return lockRequest.lockItem.target;
		}

		Object getOwner() {
			// return owner;
			return lockRequest.owner;
		}
	}

    /**
     * Describe the status of a lock acquistion request.
     * 
     * @author Dibyendu Majumdar
     */
    public enum LockStatus {
        GRANTED, GRANTABLE, TIMEOUT, DEADLOCK
    }
    
    /**
     * Holds information regarding a lock wait.
     * Purpose is to enable deadlock detection.
     * 
     * @since 15 Nov 2006
     */
    static final class LockWaiter {
    	final LockRequest myLockRequest;
    	LockWaiter cycle;
    	boolean visited = false;
    	final Thread thread;
    	
    	LockWaiter(LockRequest r, Thread t) {
    		myLockRequest = r;
    		thread = t;
    	}
    }
}
