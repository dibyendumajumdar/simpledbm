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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.simpledbm.rss.api.latch.Latch;
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
public final class LockManagerImpl implements LockManager {

    private static final String LOG_CLASS_NAME = LockManagerImpl.class.getName();

    private static final Logger log = Logger.getLogger(LockManagerImpl.class.getPackage().getName());

	/**
	 * Hash table of locks.
	 */
	private final LockBucket[] LockHashTable;

	/**
	 * Size of the hash table.
	 */
	private final int hashTableSize;

	/**
	 * List of lock event listeners
	 */
	private final ArrayList<LockEventListener> lockEventListeners = new ArrayList<LockEventListener>();
	
	private final Map<Object, LockWaiter> waiters = Collections.synchronizedMap(new HashMap<Object, LockWaiter>());
	
	//FIXME
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
		this.hashTableSize = hashTableSize;
		LockHashTable = new LockBucket[hashTableSize];
		for (int i = 0; i < hashTableSize; i++) {
			LockHashTable[i] = new LockBucket();
		}
	}

    /**
     * Checks whether the specified lock request is compatible with the granted group.
     * Also sets the otherHolders flag if the granted group contains other requests.
     */
	private boolean checkCompatible(LockItem lock, LockRequest request, LockMode mode, LockHandleImpl lockInfo) {

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

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.locking.LockMgr#acquire(java.lang.Object,
	 *      java.lang.Object, org.simpledbm.locking.LockMode,
	 *      org.simpledbm.locking.LockDuration, int)
	 */
	public final LockHandle acquire(Object owner, Object target, LockMode mode, LockDuration duration, int timeout) throws LockException {

		globalLock.sharedLock();
		LockHandleImpl handle = null;
		try {
			handle = doAcquire(owner, target, mode, duration, timeout);
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
	private LockHandleImpl doAcquire(Object owner, Object target, LockMode mode, LockDuration duration, int timeout) throws LockException {

		LockHandleImpl handle = new LockHandleImpl(this, owner, target, mode, duration, timeout);

		LockRequest r = null;
		boolean converting = false;

		if (log.isDebugEnabled()) {
			log.debug(LOG_CLASS_NAME, "acquire", "Lock requested by " + owner + " for " + target + ", mode=" + mode + ", duration=" + duration);
		}

		/* 1. Search for the lock. */
		int h = target.hashCode() % hashTableSize;
		LockItem lockitem = null;
		chainLock(h);
		try {
			lockitem = findLock(target, h);
			if (lockitem == null) {
				/*
				 * 2. If not found, this is a new lock and therefore grant the
				 * lock, and return success.
				 */
				if (log.isDebugEnabled()) {
					log.debug(LOG_CLASS_NAME, "acquire", "Lock not found, therefore granting immediately");
				}
				r = null;
				if (duration != LockDuration.INSTANT_DURATION) {
					lockitem = new LockItem(target, mode);
					r = new LockRequest(lockitem, owner, mode, duration);
					lockitem.queueAppend(r);
					chainAppend(h, lockitem);
                    return handle.setStatus(r, LockStatus.GRANTED);
				}
                else {
                    return handle.setStatus(r, LockStatus.GRANTABLE);
                }
			}
			lockitem.lock();
		} finally {
			chainUnlock(h);
		}

		try {
			/*
			 * 3. Else check if requesting transaction already has a lock
			 * request.
			 */
			r = lockitem.find(owner);

			if (r == null) {

				/* 4. If not, this is the first request by the transaction. */
				if (log.isDebugEnabled()) {
					log.debug(LOG_CLASS_NAME, "acquire", "New request by transaction " + owner + " for target " + target);
				}

				handle.setHeldByOthers(true);
				/*
				 * 5. Check if lock can be granted. This is true if there are no
				 * waiting requests and the new request is compatible with
				 * existing grant mode.
				 */
				boolean can_grant = (!lockitem.waiting && mode.isCompatible(lockitem.grantedMode));

				if (duration == LockDuration.INSTANT_DURATION && can_grant) {
					/* 6. If yes, grant the lock and return success. */
					return handle.setStatus(r, LockStatus.GRANTABLE);
				}

				else if (!can_grant && timeout == 0) {
					/* 7. Otherwise, if nowait was specified, return failure. */
					if (log.isDebugEnabled()) {
						log.debug(LOG_CLASS_NAME, "acquire", "Lock " + lockitem + " is not compatible with requested mode, TIMED OUT since NOWAIT specified");
					}
					throw new LockTimeoutException("Lock " + lockitem + " is not compatible with requested mode, TIMED OUT since NOWAIT specified");
				}

				/* Allocate new lock request */
				r = new LockRequest(lockitem, owner, mode, duration);
				lockitem.queueAppend(r);

				if (can_grant) {
					/* 6. If yes, grant the lock and return success. */
					if (log.isDebugEnabled()) {
						log.debug(LOG_CLASS_NAME, "acquire", "There are no waiting locks and request is compatible with  " + lockitem + ", therefore granting lock");
					}
					lockitem.grantedMode = mode.maximumOf(lockitem.grantedMode);
					return handle.setStatus(r, LockStatus.GRANTED);
				} else {
					converting = false;
					return handleWait(handle, r, converting, duration, h, lockitem);
				}
			} else {
				/*
				 * 11. If calling transaction already has a granted lock request
				 * then this must be a conversion request.
				 */
				if (log.isTraceEnabled()) {
					log.trace(LOG_CLASS_NAME, "acquire", "Lock conversion request by transaction " + owner + " for target " + target);
				}

				/*
				 * Limitation: a transaction cannot attempt to lock an object
				 * for which it is already waiting.
				 */
				if (r.status == LockRequestStatus.CONVERTING || r.status == LockRequestStatus.WAITING) {
					throw new LockException("SIMPLEDBM-ELOCK-001: Requested lock is already being waited for by requestor");
				}

				else if (r.status == LockRequestStatus.GRANTED) {
					/*
					 * 12. Check whether the new request lock is same mode as
					 * previously held lock.
					 */
					handle.setPreviousMode(r.mode);
					if (mode == r.mode) {
						/* 13. If so, grant lock and return. */
						if (log.isDebugEnabled()) {
							log.debug(LOG_CLASS_NAME, "acquire", "Requested mode is the same as currently held mode, therefore granting");
						}
						if (duration != LockDuration.INSTANT_DURATION) {
							r.count++;
						}
						checkCompatible(lockitem, r, mode, handle);
                        if (duration == LockDuration.INSTANT_DURATION) {
                            return handle.setStatus(r, LockStatus.GRANTABLE);
                        }
                        else {
                            return handle.setStatus(r, LockStatus.GRANTED);
                        }
					}

					else {
						/*
						 * 14. Otherwise, check if requested lock is compatible
						 * with granted group
						 */
						boolean can_grant = checkCompatible(lockitem, r, mode, handle);

						if (can_grant) {
							/* 13. If so, grant lock and return. */
							if (log.isDebugEnabled()) {
								log.debug(LOG_CLASS_NAME, "acquire", "Conversion request is compatible with granted group " + lockitem + ", therefore granting");
							}
							if (duration != LockDuration.INSTANT_DURATION) {
								r.mode = mode.maximumOf(r.mode);
								r.count++;
								lockitem.grantedMode = r.mode.maximumOf(lockitem.grantedMode);
                                return handle.setStatus(r, LockStatus.GRANTED);
							}
                            else {
                                return handle.setStatus(r, LockStatus.GRANTABLE);
                            }
						}

						else if (!can_grant && timeout == 0) {
							/* 15. If not, and nowait specified, return failure. */
							if (log.isDebugEnabled()) {
								log.debug(LOG_CLASS_NAME, "acquire", "Conversion request is not compatible with granted group " + lockitem + ", TIMED OUT since NOWAIT");
							}
							throw new LockTimeoutException("Conversion request is not compatible with granted group " + lockitem + ", TIMED OUT since NOWAIT");
						}

						else {
							converting = true;
							return handleWait(handle, r, converting, duration, h, lockitem);
						}
					}
				}
			}

		} finally {
			lockitem.unlock();
		}
		assert false;
		return null;
	}

	/**
	 * TODO What happens if an Instant Duration lock is requested when the
	 * transaction already holds a Manual Duration lock?
	 * <p>
	 * TODO What if this is also a conversion request?
	 * 
	 * @param handle
	 * @param r
	 * @param converting
	 * @param h
	 * @param lockitem
	 * @return
	 * @throws LockException
	 */
	private LockHandleImpl handleWait(LockHandleImpl handle, LockRequest r, boolean converting, LockDuration duration, int h, LockItem lockitem) throws LockException {

		Thread prevThread = Thread.currentThread();

		/* 8. Wait for the lock to be available/compatible. */
		lockitem.waiting = true;
		if (!converting) {
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "handleWait", "Waiting for lock to be free");
			}
			r.status = LockRequestStatus.WAITING;
		} else {
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "handleWait", "Conversion NOT compatible with granted group, therefore waiting ...");
			}
			r.convertMode = handle.mode;
			r.convertDuration = duration;
			r.status = LockRequestStatus.CONVERTING;
			prevThread = r.thread;
			r.thread = Thread.currentThread();
		}
		lockitem.unlock();
		notifyLockEventListeners();
		LockWaiter waiter = new LockWaiter(r, Thread.currentThread());
		System.out.println("Adding " + r + " to list of waiters");
		waiters.put(r.owner, waiter);
		globalLock.unlockShared();
		if (handle.timeout == -1) {
			LockSupport.park();
		} else {
			LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(handle.timeout, TimeUnit.SECONDS));
		}
		globalLock.sharedLock();
		waiters.remove(r.owner);
		chainLock(h);
		lockitem.lock();
		chainUnlock(h);
		// if (converting)
		// chainUnlock(h);
		LockStatus status;
		if (r.status == LockRequestStatus.GRANTED) {
			status = LockStatus.GRANTED;
		} else {
			status = LockStatus.TIMEOUT;
		}

		if (status == LockStatus.GRANTED) {
			/*
			 * 9. If after the wait, the lock has been granted, then return
			 * success.
			 */
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "handleWait", "Woken up, and lock granted");
			}
			if (converting) {
                // TODO TT1
				// r.count++;
                // System.err.println("Would have executed r.count++");
			} else {
				// chainUnlock(h);
			}
			checkCompatible(lockitem, r, handle.mode, handle);
			return handle.setStatus(r, LockStatus.GRANTED);
		}

		/* 10. Else return failure. */
		if (log.isDebugEnabled()) {
			log.debug(LOG_CLASS_NAME, "handleWait", "Woken up, and lock failed");
		}

		if (status != LockStatus.DEADLOCK) {
			status = LockStatus.TIMEOUT;
		}

		if (!converting) {
			/* If not converting the delete the newly created request. */
			lockitem.queueRemove(r);
			if (lockitem.queueIsEmpty()) {
				// chainRemove(h, lock);
				lockitem.reset(); // Setup lock for garbage collection
			}
			// chainUnlock(h);
		} else {
			/* If converting, then restore old status */
			r.status = LockRequestStatus.GRANTED;
			r.convertMode = r.mode;
			r.thread = prevThread;
		}
		if (status == LockStatus.TIMEOUT)
			throw new LockTimeoutException();
		else
			throw new LockException("SIMPLEDBM-ELOCK-002: Unable to acquire lock.");
		// return new LockHandleImpl(this, null, lockInfo, status);
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
		globalLock.sharedLock();
		try {
			return doReleaseInternal(handle, action, downgradeMode);
		}
		finally {
			globalLock.unlockShared();
		}
	}
	boolean doReleaseInternal(LockHandle handle, ReleaseAction action, LockMode downgradeMode) throws LockException {
		LockRequest r = null;
		boolean converting = false;
		LockHandleImpl handleImpl = (LockHandleImpl) handle;
		Object target = handleImpl.lockable;
		Object owner = handleImpl.owner;
        boolean released = false;

		if (log.isDebugEnabled()) {
			log.debug(LOG_CLASS_NAME, "release", "Request by " + owner + " to release lock for " + target);
		}
		int h = target.hashCode() % hashTableSize;
		chainLock(h);
		LockItem lockitem = null;
		try {
			/* 1. Search for the lock. */
			lockitem = findLock(target, h);

			if (lockitem == null) {
				/* 2. If not found, return success. */
				if (log.isDebugEnabled()) {
					log.debug(LOG_CLASS_NAME, "release", "lock not found, returning success");
				}
				throw new LockException("SIMPLEDBM-ELOCK-003: Cannot release a lock on " + target + " as it is is not locked at present; seems like invalid call to release lock");
			}
			lockitem.lock();
		} finally {
			chainUnlock(h);
		}
		try {
			/* 3. If lock found, look for the transaction's lock request. */
			r = lockitem.find(owner);

			if (r == null) {
				/* 4. If not found, return success. */
				if (log.isDebugEnabled()) {
					log.debug(LOG_CLASS_NAME, "release", "request not found, returning success");
				}
				throw new LockException("SIMPLEDBM-ELOCK-003: Cannot release a lock on " + target + " as it is is not locked at present; seems like invalid call to release lock");
			}

			if (r.status == LockRequestStatus.CONVERTING || r.status == LockRequestStatus.WAITING) {
				/* 5. If lock in invalid state, return error. */
				if (log.isDebugEnabled()) {
					log.debug(LOG_CLASS_NAME, "release", "cannot release a lock request that is not granted");
				}
				throw new LockException("SIMPLEDBM-ELOCK-004: Cannot release a lock that is being waited for");
			}

			if (action == ReleaseAction.DOWNGRADE && r.mode == downgradeMode) {
				/*
				 * If downgrade request and lock is already in target mode,
				 * return success.
				 */
				return false;
			}

			if (action == ReleaseAction.RELEASE && r.count > 1) {
				/*
				 * 6. If noforce, and reference count greater than 0, then do
				 * not release the lock request. Decrement reference count if
				 * greater than 0, and, return Ok.
				 */
				if (log.isDebugEnabled()) {
					log.debug(LOG_CLASS_NAME, "release", "count decremented but lock not released");
				}
				r.count--;
				return false;
			}

            /*
             * Either the lock is being downgraded or it is being released and its
             * reference count == 0 or it is being forcibly released.
             */
            
			if (r == lockitem.queueHead() && r == lockitem.queueTail() && action != ReleaseAction.DOWNGRADE) {
				/* 7. If sole lock request, then release the lock and return Ok. */
				if (log.isDebugEnabled()) {
					log.debug(LOG_CLASS_NAME, "release", "removing sole lock, releasing lock object");
				}
				lockitem.queueRemove(r);
				lockitem.reset();
				return true;
			}

			/*
			 * 8. If not downgrading, delete the lock request from the queue.
			 * Otherwise, downgrade the mode assigned to the lock request.
			 */
			if (action != ReleaseAction.DOWNGRADE) {
				if (log.isDebugEnabled()) {
					log.debug(LOG_CLASS_NAME, "release", "Removing lock request " + r + " and re-adjusting granted mode");
				}
				lockitem.queueRemove(r);
                released = true;
			} else {
				/*
				 * We need to determine whether is a valid downgrade request.
				 * To do so, we do a reverse check - ie, if the new mode could have
				 * been upgraded to current mode, then it is okay to downgrade.
				 */
				LockMode mode = downgradeMode.maximumOf(r.mode);
				if (mode == r.mode) {
					if (log.isDebugEnabled()) {
						log.debug(LOG_CLASS_NAME, "release", "Downgrading " + r + " to " + downgradeMode + " and re-adjusting granted mode");
					}
                    handleImpl.setPreviousMode(r.mode);
					r.convertMode = r.mode = downgradeMode;
					handleImpl.setCurrentMode(downgradeMode);
                    handleImpl.setHeldByOthers(false);
				} else {
					throw new LockException("SIMPLEDBM-ELOCK-005: Invalid downgrade request from " + r.mode + " to " + downgradeMode);
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

		} finally {
			lockitem.unlock();
			// chainUnlock(h);
		}
        return released;
	}

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
	
	private void visit(LockWaiter me) {
		if (me.visited) {
			return;
		}
		else {
			me.visited = true;
		}
		LockWaiter him;
		for (LockRequest them : me.waitingFor.lockItem.queue) {
			if (them.owner != me.waitingFor.owner) {
				if (!them.mode.isCompatible(me.waitingFor.mode) || them.status != LockRequestStatus.GRANTED) {
					System.out.println("Them = " + them);
					him = waiters.get(them.owner);
					me.cycle = him;
					if (him.cycle != null) {
						System.out.println("DEADLOCK DETECTED: " + him.waitingFor + " waiting for " + me.waitingFor);
						System.out.println("HIM : " + him.waitingFor.lockItem);
						System.out.println("ME : " + me.waitingFor.lockItem);
						LockSupport.unpark(me.thread);
					}
					else {
						visit(him);
					}
					me.cycle = null;
				}
			}
			else {
				break;
			}
		}
	}
	
	public void detectDeadlocks() {
		globalLock.exclusiveLock();
		try {
			LockWaiter[] waiterArray = waiters.values().toArray(new LockWaiter[0]);
			for (LockWaiter waiter: waiterArray) {
				waiter.cycle = null;
				waiter.visited = false;
			}
			for (LockWaiter waiter: waiterArray) {
				visit(waiter);
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
	private LockItem findLock(Object target, int h) {
		for (Iterator<LockItem> iter = getChain(h).iterator(); iter.hasNext();) {
			LockItem item = iter.next();
			if (item.target == null) {
				iter.remove();
				continue;
			}
			if (target.equals(item.target)) {
				return item;
			}
		}
		return null;
	}

	private void chainLock(int h) {
		LockHashTable[h].lock();
	}

	private void chainUnlock(int h) {
		LockHashTable[h].unlock();
	}

	private void chainAppend(int h, LockItem item) {
		LockHashTable[h].chainAppend(item);
	}

	private LinkedList<LockItem> getChain(int h) {
		return LockHashTable[h].chain;
	}

	static final class LockBucket {

		final LinkedList<LockItem> chain = new LinkedList<LockItem>();

		final Lock lock = new ReentrantLock();

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

		void lock() {
			lock.lock();
		}

		void unlock() {
			lock.unlock();
		}
	}

	static final class LockItem {

		final Lock lock = new ReentrantLock();

		Object target;

		final LinkedList<LockRequest> queue = new LinkedList<LockRequest>();

		LockMode grantedMode;

		boolean waiting;

		void lock() {
			lock.lock();
		}

		void unlock() {
			lock.unlock();
		}

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
		GRANTED, CONVERTING, WAITING;
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

        private final LockManagerImpl lockMgr;

		final Object lockable;

        final Object owner;

        final LockMode mode;

        final int timeout;

        private LockMode previousMode = LockMode.NONE;

        private boolean heldByOthers = false;

        private LockMode currentMode = LockMode.NONE;

        private LockStatus status;

		LockHandleImpl(LockManagerImpl lockMgr, Object owner, Object target, LockMode mode, LockDuration duration, int timeout) {
			this.lockMgr = lockMgr;
			this.owner = owner;
			this.lockable = target;
			this.mode = mode;
			this.timeout = timeout;
		}

		final LockHandleImpl setStatus(LockRequest request, LockStatus status) {
			if (request != null) {
				currentMode = request.mode;
			}
			this.status = status;
			return this;
		}

		public final boolean release(boolean force) throws LockException {
			return lockMgr.doRelease(this, force ? LockManagerImpl.ReleaseAction.FORCE_RELEASE : LockManagerImpl.ReleaseAction.RELEASE, null);
		}

		public final void downgrade(LockMode mode) throws LockException {
			lockMgr.doRelease(this, LockManagerImpl.ReleaseAction.DOWNGRADE, mode);
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
    
    static final class LockWaiter {
    	final LockRequest waitingFor;
    	LockWaiter cycle;
    	boolean visited = false;
    	final Thread thread;
    	
    	LockWaiter(LockRequest r, Thread t) {
    		waitingFor = r;
    		thread = t;
    	}
    }
}

