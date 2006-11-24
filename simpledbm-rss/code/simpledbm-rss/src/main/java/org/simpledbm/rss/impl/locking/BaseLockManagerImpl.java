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
import java.util.concurrent.locks.LockSupport;

import org.simpledbm.rss.api.latch.Latch;
import org.simpledbm.rss.api.locking.LockDeadlockException;
import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockEventListener;
import org.simpledbm.rss.api.locking.LockException;
import org.simpledbm.rss.api.locking.LockHandle;
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
public abstract class BaseLockManagerImpl implements LockManager {

	// Changes in this version
	// Synchronise on hash bucket
	// Restructure doAcquire() - handleWait is now inline due to issues with synchronisation
	// Changed findLock() to work on bucket
	// Changed call to chainAppend() in doAcquireLock() to work on bucket
	// Recalculate hash bucket after the wait to cater for hash table resizing
	// Added new method rehash() and new instance variables hashPrimes and htsz
	
    protected static final String LOG_CLASS_NAME = BaseLockManagerImpl.class.getName();

    protected static final Logger log = Logger.getLogger(BaseLockManagerImpl.class.getPackage().getName());

	static final int hashPrimes[] = {
		53, 97, 193, 389, 769, 1543, 3079, 6151, 12289, 24593, 49157,
		98317, 196613, 393241, 786433
	};
	
	protected int htsz = 0;
	
	/**
	 * Tracks the number of items in the hash table
	 */
	protected volatile int count = 0;
	
	/**
	 * Upper limit of number of items that can be inserted into the
	 * hash table. Exceeding this causes the hash table to be resized.
	 */
	protected volatile int threshold = 0;
	
	/**
	 * Used to calculate the threshold. Expressed as a percentage of hash table size
	 */
	protected float loadFactor = 0.75f;
	
	/**
	 * Hash table of locks.
	 */
	protected volatile LockBucket[] LockHashTable;

	/**
	 * Size of the hash table.
	 */
	protected volatile int hashTableSize;

	/**
	 * List of lock event listeners
	 */
	protected final ArrayList<LockEventListener> lockEventListeners = new ArrayList<LockEventListener>();
	
	/**
	 * Map of waiting lock requesters, to aid deadlock detection.
	 * Keyed by lock object.
	 */
	protected final Map<Object, LockWaiter> waiters = Collections.synchronizedMap(new HashMap<Object, LockWaiter>());
	
	//FIXME Need to create the latch using the factory
	/**
	 * To keep the algorithm simple, the deadlock detector uses a global exclusive lock
	 * on the lock manager. The lock manager itself acquires shared locks during normal operations,
	 * thus avoiding conflict with the deadlock detector.
	 */
	protected final Latch globalLock = new ReadWriteUpdateLatch(); 
	
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
	public BaseLockManagerImpl(int hashTableSize) {
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
	protected void rehash() {

		globalLock.exclusiveLock();
		try {
			if (htsz == hashPrimes.length-1) {
				return;
			}
			int newHashTableSize = hashPrimes[++htsz];
			System.out.println("Growing hash table size from " + hashTableSize + " to " + newHashTableSize);
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
				bucket.chain.clear();
				LockHashTable[i] = null;
			}
			LockHashTable = newLockHashTable;
			hashTableSize = newHashTableSize;
			threshold = (int)(hashTableSize * loadFactor);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			globalLock.unlockExclusive();
		}
	}
	
    /**
     * Checks whether the specified lock request is compatible with the granted group.
     * Also sets the otherHolders flag if the granted group contains other requests.
     */
	protected boolean checkCompatible(LockItem lock, LockRequest request, LockMode mode, LockHandleImpl lockInfo) {

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
	
	static final class LockParams {
		Object owner;
		Object target;
		LockMode mode;
		LockDuration duration;
		int timeout;
		
		LockMode downgradeMode;
		ReleaseAction action;
	}

	static final class LockState {
		final LockParams parms;
		LockHandleImpl handle;
		LockRequest r;
		LockBucket bucket;
		boolean converting;
		Thread prevThread;
		LockItem lockitem;
		LockStatus status;
	
		public LockState(LockParams parms) {
			this.parms = parms;
		}
	}

	/**
	 * Handles the case where there aren't any locks on the target.
	 */
	protected void handleNewLock(LockState lockState) {
		if (log.isDebugEnabled()) {
			log.debug(LOG_CLASS_NAME, "acquire",
					"Lock not found, therefore granting immediately");
		}
		if (lockState.parms.duration != LockDuration.INSTANT_DURATION) {
			LockItem lockitem = getNewLockItem(lockState.parms.target, lockState.parms.mode);
			LockRequest r = new LockRequest(lockitem, lockState.parms.owner, lockState.parms.mode, lockState.parms.duration);
			lockitem.queueAppend(r);
			lockState.bucket.chainAppend(lockitem);
			count++;
			lockState.handle.setStatus(r, LockStatus.GRANTED);
		} else {
			lockState.handle.setStatus(null, LockStatus.GRANTABLE);
		}
	}
	
	/**
	 * Handles the result of a lock wait. 
	 */
	protected void handleWaitResult(LockState lockState) throws LockTimeoutException, LockDeadlockException, LockException {
		LockRequestStatus lockRequestStatus = lockState.r.status;
		if (lockRequestStatus == LockRequestStatus.GRANTED) {
			lockState.status = LockStatus.GRANTED;
		} else if (lockRequestStatus == LockRequestStatus.DENIED) {
			lockState.status = LockStatus.DEADLOCK;
		} else {
			System.out.println("Lock State = " + lockRequestStatus);
			lockState.status = LockStatus.TIMEOUT;
		}

		if (lockState.status == LockStatus.GRANTED) {
			/*
			 * 9. If after the wait, the lock has been granted, then return
			 * success.
			 */
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "handleWaitResult",
						"Woken up, and lock granted");
			}
			checkCompatible(lockState.lockitem, lockState.r, lockState.parms.mode, lockState.handle);
			lockState.handle.setStatus(lockState.r, LockStatus.GRANTED);
			return;
		}

		/* 10. Else return failure. */
		if (log.isDebugEnabled()) {
			log.debug(LOG_CLASS_NAME, "handleWaitResult",
					"Woken up, and lock failed");
		}

		if (!lockState.converting) {
			/* If not converting the delete the newly created request. */
			lockState.lockitem.queueRemove(lockState.r);
			if (lockState.lockitem.queueIsEmpty()) {
				lockState.lockitem.reset(); // Setup lock for garbage collection
				count--;
			}
		} else {
			/* If converting, then restore old status */
			lockState.r.status = LockRequestStatus.GRANTED;
			lockState.r.convertMode = lockState.r.mode;
			lockState.r.thread = lockState.prevThread;
		}
		if (lockState.status == LockStatus.DEADLOCK) {
			/* 
			 * If we have been chosen as a deadlock victim, then we need to grant the
			 * lock to the waiter who has won the deadlock.
			 */
			grantWaiters(ReleaseAction.RELEASE, lockState.r, lockState.handle, lockState.lockitem);
		}
		if (lockState.status == LockStatus.TIMEOUT)
			throw new LockTimeoutException("SIMPLEDBM-ELOCK: Lock request " + lockState.parms.toString() + " timed out");
		else if (lockState.status == LockStatus.DEADLOCK)
			throw new LockDeadlockException("SIMPLEDBM-ELOCK: Lock request " + lockState.parms.toString() + " failed due to a deadlock");
		else
			throw new LockException(
					"SIMPLEDBM-ELOCK-002: Unexpected error occurred while attempting to acqure lock " + lockState.parms.toString());
	}

	/**
	 * Prepare the lock request for waiting.
	 */
	protected void prepareToWait(LockState lockState) {
		lockState.lockitem.waiting = true;
		if (!lockState.converting) {
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "prepareToWait",
						"Waiting for lock to be free");
			}
			lockState.r.status = LockRequestStatus.WAITING;
		} else {
			if (log.isDebugEnabled()) {
				log
						.debug(LOG_CLASS_NAME, "prepareToWait",
								"Conversion NOT compatible with granted group, therefore waiting ...");
			}
			lockState.r.convertMode = lockState.handle.mode;
			lockState.r.convertDuration = lockState.parms.duration;
			lockState.r.status = LockRequestStatus.CONVERTING;
			lockState.prevThread = lockState.r.thread;
			lockState.r.thread = Thread.currentThread();
		}
	}

	/**
	 * Handles a conversion request in the nowait situation. 
	 * @return true if conversion request was handled else false to indicate that requester must enter wait.
	 */
	protected boolean handleConversionRequest(LockState lockState) throws LockException, LockTimeoutException {
		/*
		 * 11. If calling transaction already has a granted lock request
		 * then this must be a conversion request.
		 */
		if (log.isTraceEnabled()) {
			log.trace(LOG_CLASS_NAME, "handleConversionRequest",
					"Lock conversion request by transaction " + lockState.parms.owner
							+ " for target " + lockState.parms.target);
		}

		/*
		 * Limitation: a transaction cannot attempt to lock an object
		 * for which it is already waiting.
		 */
		if (lockState.r.status == LockRequestStatus.CONVERTING
				|| lockState.r.status == LockRequestStatus.WAITING) {
			throw new LockException(
					"SIMPLEDBM-ELOCK-001: Requested lock is already being waited for by requestor");
		}

		else if (lockState.r.status == LockRequestStatus.GRANTED) {
			/*
			 * 12. Check whether the new request lock is same mode as
			 * previously held lock.
			 */
			lockState.handle.setPreviousMode(lockState.r.mode);
			if (lockState.parms.mode == lockState.r.mode) {
				/* 13. If so, grant lock and return. */
				if (log.isDebugEnabled()) {
					log
							.debug(LOG_CLASS_NAME, "handleConversionRequest",
									"Requested mode is the same as currently held mode, therefore granting");
				}
				if (lockState.parms.duration != LockDuration.INSTANT_DURATION) {
					lockState.r.count++;
				}
				checkCompatible(lockState.lockitem, lockState.r, lockState.parms.mode, lockState.handle);
				if (lockState.parms.duration == LockDuration.INSTANT_DURATION) {
					lockState.handle.setStatus(lockState.r, LockStatus.GRANTABLE);
				} else {
					lockState.handle.setStatus(lockState.r, LockStatus.GRANTED);
				}
				return true;
			}

			else {
				/*
				 * 14. Otherwise, check if requested lock is compatible
				 * with granted group
				 */
				boolean can_grant = checkCompatible(lockState.lockitem, lockState.r, lockState.parms.mode,
						lockState.handle);

				if (can_grant) {
					/* 13. If so, grant lock and return. */
					if (log.isDebugEnabled()) {
						log.debug(LOG_CLASS_NAME, "handleConversionRequest",
								"Conversion request is compatible with granted group "
										+ lockState.lockitem
										+ ", therefore granting");
					}
					if (lockState.parms.duration != LockDuration.INSTANT_DURATION) {
						lockState.r.mode = lockState.parms.mode.maximumOf(lockState.r.mode);
						lockState.r.count++;
						lockState.lockitem.grantedMode = lockState.r.mode
								.maximumOf(lockState.lockitem.grantedMode);
						lockState.handle.setStatus(lockState.r, LockStatus.GRANTED);
					} else {
						lockState.handle
								.setStatus(lockState.r, LockStatus.GRANTABLE);
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
	protected boolean handleNewRequest(LockState lockState) throws LockTimeoutException {
		/* 4. If not, this is the first request by the transaction. */
		if (log.isDebugEnabled()) {
			log.debug(LOG_CLASS_NAME, "handleNewRequest",
					"New request by transaction " + lockState.parms.owner
							+ " for target " + lockState.parms.target);
		}

		lockState.handle.setHeldByOthers(true);
		/*
		 * 5. Check if lock can be granted. This is true if there are no
		 * waiting requests and the new request is compatible with
		 * existing grant mode.
		 */
		boolean can_grant = (!lockState.lockitem.waiting && lockState.parms.mode
				.isCompatible(lockState.lockitem.grantedMode));

		if (lockState.parms.duration == LockDuration.INSTANT_DURATION && can_grant) {
			/* 6. If yes, grant the lock and return success. */
			lockState.handle.setStatus(lockState.r, LockStatus.GRANTABLE);
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
		lockState.r = new LockRequest(lockState.lockitem, lockState.parms.owner, lockState.parms.mode, lockState.parms.duration);
		lockState.lockitem.queueAppend(lockState.r);

		if (can_grant) {
			/* 6. If yes, grant the lock and return success. */
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "handleNewRequest",
						"There are no waiting locks and request is compatible with  "
								+ lockState.lockitem
								+ ", therefore granting lock");
			}
			lockState.lockitem.grantedMode = lockState.parms.mode.maximumOf(lockState.lockitem.grantedMode);
			lockState.handle.setStatus(lockState.r, LockStatus.GRANTED);
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
	public final LockHandle acquire(Object owner, Object target, LockMode mode, LockDuration duration, int timeout) throws LockException {

		LockParams parms = new LockParams();
		parms.owner = owner;
		parms.target = target;
		parms.mode = mode;
		parms.duration = duration;
		parms.timeout = timeout;
		
		LockState lockState = new LockState(parms);
		
		if (count > threshold) {
			rehash();
		}
		
		globalLock.sharedLock();
		LockHandleImpl handle = null;
		try {
			handle = doAcquire(lockState);
			if (duration == LockDuration.INSTANT_DURATION
					&& handle.getStatus() == LockStatus.GRANTED) {
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

	protected abstract LockHandleImpl doAcquire(LockState lockState)
	throws LockException;

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
	protected boolean releaseLock(LockState lockState) throws LockException {
		boolean released;
		/* 3. If lock found, look for the transaction's lock request. */
		lockState.r = lockState.lockitem.find(lockState.parms.owner);

		if (lockState.r == null) {
			/* 4. If not found, return success. */
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "release",
						"request not found, returning success");
			}
			throw new LockException(
					"SIMPLEDBM-ELOCK-003: Cannot release a lock on "
							+ lockState.parms.target
							+ " as it is is not locked at present; seems like invalid call to release lock");
		}

		if (lockState.r.status == LockRequestStatus.CONVERTING
				|| lockState.r.status == LockRequestStatus.WAITING) {
			/* 5. If lock in invalid state, return error. */
			if (log.isDebugEnabled()) {
				log
						.debug(LOG_CLASS_NAME, "release",
								"cannot release a lock request that is not granted");
			}
			throw new LockException(
					"SIMPLEDBM-ELOCK-004: Cannot release a lock that is being waited for");
		}

		if (lockState.parms.action == ReleaseAction.DOWNGRADE && lockState.r.mode == lockState.parms.downgradeMode) {
			/*
			 * If downgrade request and lock is already in target mode,
			 * return success.
			 */
			return false;
		}

		if (lockState.parms.action == ReleaseAction.RELEASE && lockState.r.count > 1) {
			/*
			 * 6. If noforce, and reference count greater than 0, then do
			 * not release the lock request. Decrement reference count if
			 * greater than 0, and, return Ok.
			 */
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "release",
						"count decremented but lock not released");
			}
			lockState.r.count--;
			return false;
		}

		/*
		 * Either the lock is being downgraded or it is being released and
		 * its reference count == 0 or it is being forcibly released.
		 */

		if (lockState.r == lockState.lockitem.queueHead() && lockState.r == lockState.lockitem.queueTail()
				&& lockState.parms.action != ReleaseAction.DOWNGRADE) {
			/* 7. If sole lock request, then release the lock and return Ok. */
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "release",
						"removing sole lock, releasing lock object");
			}
			lockState.lockitem.queueRemove(lockState.r);
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
						"Removing lock request " + lockState.r
								+ " and re-adjusting granted mode");
			}
			lockState.lockitem.queueRemove(lockState.r);
			released = true;
		} else {
			/*
			 * We need to determine whether is a valid downgrade request. To
			 * do so, we do a reverse check - ie, if the new mode could have
			 * been upgraded to current mode, then it is okay to downgrade.
			 */
			LockMode mode = lockState.parms.downgradeMode.maximumOf(lockState.r.mode);
			if (mode == lockState.r.mode) {
				if (log.isDebugEnabled()) {
					log.debug(LOG_CLASS_NAME, "release", "Downgrading " + lockState.r
							+ " to " + lockState.parms.downgradeMode
							+ " and re-adjusting granted mode");
				}
				lockState.handle.setPreviousMode(lockState.r.mode);
				lockState.r.convertMode = lockState.r.mode = lockState.parms.downgradeMode;
				lockState.handle.setCurrentMode(lockState.parms.downgradeMode);
				lockState.handle.setHeldByOthers(false);
			} else {
				throw new LockException(
						"SIMPLEDBM-ELOCK-005: Invalid downgrade request from "
								+ lockState.r.mode + " to " + lockState.parms.downgradeMode);
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
		grantWaiters(lockState.parms.action, lockState.r, lockState.handle, lockState.lockitem);
		return released;
	}

	public static boolean monitoring = false;
	public static ArrayList<String> messages = new ArrayList<String>(10);
	
	protected void grantWaiters(ReleaseAction action, LockRequest r, LockHandleImpl handleImpl, LockItem lockitem) {
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
		            handleImpl.setHeldByOthers(true);
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
					if (monitoring) {
						messages.add("Unparked thread " + r.thread);
					}
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
					if (monitoring) {
						messages.add("Unparked thread " + r.thread);
					}
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
	boolean doRelease(LockHandle handle, ReleaseAction action, LockMode downgradeMode) throws LockException {
		
		LockParams parms = new LockParams();
		LockHandleImpl handleImpl = (LockHandleImpl) handle;
		parms.target = handleImpl.lockable;
		parms.owner = handleImpl.owner;
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

	abstract boolean doReleaseInternal(LockState lockState) throws LockException; 
	
	public synchronized void addLockEventListener(LockEventListener listener) {
		lockEventListeners.add(listener);
	}
	
	public synchronized void clearLockEventListeners() {
		lockEventListeners.clear();
	}
	
	public void notifyLockEventListeners() {
		for (LockEventListener listener: lockEventListeners) {
			try {
				listener.beforeLockWait();
			}
			catch (Exception e) {
				// FIXME
				e.printStackTrace();
			}
		}
	}
	
	protected boolean findDeadlockCycle(LockWaiter me) {
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
	
	public void detectDeadlocks() {
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
	protected LockItem findLock(LockState lockState) {
		for (Iterator<LockItem> iter = lockState.bucket.chain.iterator(); iter.hasNext();) {
			LockItem item = iter.next();
			if (item.target == null) {
				iter.remove();
				continue;
			}
			if (lockState.parms.target.equals(item.target)) {
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
	
	abstract LockBucket getNewLockBucket();

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
	
	abstract LockItem getNewLockItem(Object target, LockMode mode);

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

		final int lockPos = 0;

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

        protected final BaseLockManagerImpl lockMgr;

		final Object lockable;

        final Object owner;

        final LockMode mode;

        final int timeout;

        protected LockMode previousMode = LockMode.NONE;

        protected boolean heldByOthers = false;

        protected LockMode currentMode = LockMode.NONE;

        protected LockStatus status;

		LockHandleImpl(BaseLockManagerImpl lockMgr, LockParams parms) {
			this.lockMgr = lockMgr;
			this.owner = parms.owner;
			this.lockable = parms.target;
			this.mode = parms.mode;
			this.timeout = parms.timeout;
		}

		final LockHandleImpl setStatus(LockRequest request, LockStatus status) {
			if (request != null) {
				currentMode = request.mode;
			}
			this.status = status;
			return this;
		}

		public final boolean release(boolean force) throws LockException {
			return lockMgr.doRelease(this, force ? BaseLockManagerImpl.ReleaseAction.FORCE_RELEASE : BaseLockManagerImpl.ReleaseAction.RELEASE, null);
		}

		public final void downgrade(LockMode mode) throws LockException {
			lockMgr.doRelease(this, BaseLockManagerImpl.ReleaseAction.DOWNGRADE, mode);
		}

        public final LockMode getPreviousMode() {
            return previousMode;
        }

        public final boolean isHeldByOthers() {
            return heldByOthers;
        }

        public final LockMode getCurrentMode() {
            return currentMode;
        }

        final LockStatus getStatus() {
            return status;
        }

        final void setCurrentMode(LockMode currentMode) {
            this.currentMode = currentMode;
        }

        final void setHeldByOthers(boolean heldByOthers) {
            this.heldByOthers = heldByOthers;
        }

        final void setPreviousMode(LockMode previousMode) {
            this.previousMode = previousMode;
        }

        @Override
        public String toString() {
            return "LockHandleImpl(owner=" + owner + ", target=" + lockable + ", currentMode=" + getCurrentMode() + ", prevMode=" + getPreviousMode() + 
                ", otherHolders=" + isHeldByOthers() + ", status=" + getStatus() + ")";
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

