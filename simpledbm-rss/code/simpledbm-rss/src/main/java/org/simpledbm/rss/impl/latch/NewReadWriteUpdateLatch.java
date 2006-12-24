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

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.simpledbm.rss.api.latch.Latch;
import org.simpledbm.rss.api.latch.LatchException;
import org.simpledbm.rss.api.locking.LockMode;
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
public final class NewReadWriteUpdateLatch implements Latch {

	private static final String LOG_CLASS_NAME = NewReadWriteUpdateLatch.class.getName();

    private static final Logger log = Logger.getLogger(NewReadWriteUpdateLatch.class.getPackage().getName());

	final LinkedList<LockRequest> queue = new LinkedList<LockRequest>();

	LockMode grantedMode = LockMode.NONE;

	boolean waiting = false;

	/**
	 * Defines the various lock release methods.
	 * 
	 * @author Dibyendu Majumdar
	 */
	enum ReleaseAction {
		RELEASE, DOWNGRADE;
	}

    /**
     * Describe the status of a lock acquistion request.
     * 
     * @author Dibyendu Majumdar
     */
    public enum LockStatus {
        GRANTED, GRANTABLE, TIMEOUT, DEADLOCK
    }

	static enum LockRequestStatus {
		GRANTED, CONVERTING, WAITING, DENIED;
	}

    /**
	 * Holds parameters supplied to aquire, release or find APIs. 
	 */
	static final class LockParams {
		Object owner;
		LockMode mode;
		int timeout;
		ReleaseAction action;
		LockMode downgradeMode;
		boolean upgrade;
	}

	static final class LockState {
		final LockParams parms;
		LockRequest lockRequest;
		boolean converting;
		Thread prevThread;
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
	 * Creates a new LockMgrImpl, ready for use.
	 * 
	 * @param hashTableSize
	 *            The size of the lock hash table.
	 */
	public NewReadWriteUpdateLatch() {
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.locking.LockMgr#acquire(java.lang.Object,
	 *      java.lang.Object, org.simpledbm.locking.LockMode,
	 *      org.simpledbm.locking.LockDuration, int)
	 */
	public final void acquire(LockMode mode, int timeout, boolean upgrade) {

		LockParams parms = new LockParams();
		parms.owner = Thread.currentThread();
		parms.mode = mode;
		parms.timeout = timeout;
		parms.upgrade = upgrade;
		
		LockState lockState = new LockState(parms);
		
		doAcquire(lockState);
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
	private void doAcquire(LockState lockState) {

		if (log.isDebugEnabled()) {
			log.debug(LOG_CLASS_NAME, "acquire", "Lock requested by " + lockState.parms.owner
					+ " for " + this + ", mode=" + lockState.parms.mode);
		}

		lockState.converting = false;
		lockState.prevThread = Thread.currentThread();

		synchronized (this) {
			/*
			 * 3. Else check if requesting thread already has a lock request.
			 */
			lockState.lockRequest = find(lockState.parms.owner);

			if (lockState.lockRequest == null) {
				if (handleNewRequest(lockState)) {
					return;
				}
			} else {
				if (handleConversionRequest(lockState)) {
					return;
				}
			}

			/* 8. Wait for the lock to be available/compatible. */
			prepareToWait(lockState);
		}
		long then = System.nanoTime();
		long timeToWait = lockState.parms.timeout;
		if (timeToWait != -1) {
			timeToWait = TimeUnit.NANOSECONDS.convert(
					lockState.parms.timeout, TimeUnit.SECONDS);
		}
		for (;;) {
			if (lockState.parms.timeout == -1) {
				LockSupport.park();
			} else {
				LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(
						lockState.parms.timeout, TimeUnit.SECONDS));
			}
			long now = System.nanoTime();
			if (timeToWait > 0) {
				timeToWait -= (now - then);
				then = now;
			}
			synchronized (this) {
				/*
				 * As the hash table may have been resized while we were
				 * waiting, we need to recalculate the bucket.
				 */
				if (lockState.lockRequest.status == LockRequestStatus.WAITING
						|| lockState.lockRequest.status == LockRequestStatus.CONVERTING) {
					if (timeToWait > 0 || lockState.parms.timeout == -1) {
						System.err
								.println("Need to retry as this was a spurious wakeup");
						continue;
					}
				}
				handleWaitResult(lockState);
				return;
			}
		}
	}

	LockRequest find(Object owner) {
		for (LockRequest req : queue) {
			if (req.owner == owner || req.owner.equals(owner)) {
				return req;
			}
		}
		return null;
	}

	/**
	 * Handles a new lock request when the lock is already held by some other.
	 * @return true if the lock request was processed, else false to indicate that the requester must 
	 * 		wait
	 */
	private boolean handleNewRequest(LockState lockState) {
		/* 4. If not, this is the first request by the transaction. */
		if (log.isDebugEnabled()) {
			log.debug(LOG_CLASS_NAME, "handleNewRequest",
					"New request by thread " + lockState.parms.owner);
		}

		if (lockState.parms.upgrade) {
			throw new LatchException("Invalid request for upgrade");
		}
		
		/*
		 * 5. Check if lock can be granted. This is true if there are no
		 * waiting requests and the new request is compatible with
		 * existing grant mode.
		 */
		boolean can_grant = (!waiting && lockState.parms.mode
				.isCompatible(grantedMode));

		if (!can_grant && lockState.parms.timeout == 0) {
			/* 7. Otherwise, if nowait was specified, return failure. */
			if (log.isDebugEnabled()) {
				log.debug(
					LOG_CLASS_NAME,	"handleNewRequest",
					"Lock "	+ this
					+ " is not compatible with requested mode, TIMED OUT since NOWAIT specified");
			}
			throw new LatchException(
					"Lock "	+ this
					+ " is not compatible with requested mode, TIMED OUT since NOWAIT specified");
		}

		/* Allocate new lock request */
		lockState.lockRequest = new LockRequest(this, lockState.parms.owner, lockState.parms.mode);
		queueAppend(lockState.lockRequest);
		if (can_grant) {
			/* 6. If yes, grant the lock and return success. */
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "handleNewRequest",
						"There are no waiting locks and request is compatible with  "
								+ this
								+ ", therefore granting lock");
			}
			grantedMode = lockState.parms.mode.maximumOf(grantedMode);
			lockState.setStatus(LockStatus.GRANTED);
			return true;
		} else {
			lockState.converting = false;
			return false;
		}
	}

	/**
	 * Handles a conversion request in the nowait situation. 
	 * @return true if conversion request was handled else false to indicate that requester must enter wait.
	 */
	private boolean handleConversionRequest(LockState lockState) {
		/*
		 * 11. If calling transaction already has a granted lock request
		 * then this must be a conversion request.
		 */
		if (log.isTraceEnabled()) {
			log.trace(LOG_CLASS_NAME, "handleConversionRequest",
					"Lock conversion request by thread " + lockState.parms.owner);
		}

		/*
		 * Limitation: a transaction cannot attempt to lock an object
		 * for which it is already waiting.
		 */
		if (lockState.lockRequest.status == LockRequestStatus.CONVERTING
				|| lockState.lockRequest.status == LockRequestStatus.WAITING) {
			throw new LatchException(
					"SIMPLEDBM-ELOCK-001: Requested lock is already being waited for by requestor");
		}

		else if (lockState.lockRequest.status == LockRequestStatus.GRANTED) {
			/*
			 * 12. Check whether the new request lock has the same mode as
			 * previously held lock.
			 */
			if (lockState.parms.mode == lockState.lockRequest.mode) {
				/* 13. If so, grant lock and return. */
				if (log.isDebugEnabled()) {
					log.debug(LOG_CLASS_NAME, "handleConversionRequest",
					"Requested mode is the same as currently held mode, therefore granting");
				}
				if (!lockState.parms.upgrade) {
					lockState.lockRequest.count++;
				}
				lockState.setStatus(LockStatus.GRANTED);
				return true;
			}

			else {
				/*
				 * 14. Otherwise, check if requested lock is compatible
				 * with granted group
				 */
				boolean can_grant = checkCompatible(lockState.lockRequest, lockState.parms.mode);

				if (can_grant) {
					/* 13. If so, grant lock and return. */
					if (log.isDebugEnabled()) {
						log.debug(LOG_CLASS_NAME, "handleConversionRequest",
							"Conversion request is compatible with granted group "
							+ this + ", therefore granting");
					}
					lockState.lockRequest.mode = lockState.parms.mode.maximumOf(lockState.lockRequest.mode);
					if (!lockState.parms.upgrade) {
						lockState.lockRequest.count++;
					}
					grantedMode = lockState.lockRequest.mode
								.maximumOf(grantedMode);
					lockState.setStatus(LockStatus.GRANTED);
					return true;
				}

				else if (!can_grant && lockState.parms.timeout == 0) {
					/* 15. If not, and nowait specified, return failure. */
					if (log.isDebugEnabled()) {
						log.debug(LOG_CLASS_NAME, "handleConversionRequest",
								"Conversion request is not compatible with granted group "
										+ this
										+ ", TIMED OUT since NOWAIT");
					}
					throw new LatchException(
							"Conversion request is not compatible with granted group "
									+ this
									+ ", TIMED OUT since NOWAIT");
				}

				else {
					lockState.converting = true;
					return false;
				}
			}
		}
		else {
			throw new LatchException("Unexpected error occurred while handling a lock conversion request");
		}
	}

	
	/**
     * Checks whether the specified lock request is compatible with the granted group.
     * Also sets the otherHolders flag if the granted group contains other requests.
     */
	private boolean checkCompatible(LockRequest request, LockMode mode) {

		boolean iscompatible = true;

		/* Check if there are other holders */
		for (LockRequest other : getQueue()) {

			if (other == request)
				continue;
			else if (other.status == LockRequestStatus.WAITING)
				break;
			else {
				if (!mode.isCompatible(other.mode)) {
					iscompatible = false;
					break;
				}
			}
		}
		return iscompatible;
	}
	
	/**
	 * Prepare the lock request for waiting.
	 */
	private void prepareToWait(LockState lockState) {
		waiting = true;
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
			lockState.lockRequest.status = LockRequestStatus.CONVERTING;
			lockState.lockRequest.upgrade = lockState.parms.upgrade;
			lockState.prevThread = lockState.lockRequest.thread;
			lockState.lockRequest.thread = Thread.currentThread();
		}
	}

	/**
	 * Handles the result of a lock wait. 
	 */
	private void handleWaitResult(LockState lockState) {
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
			return;
		}

		/* 10. Else return failure. */
		if (log.isDebugEnabled()) {
			log.debug(LOG_CLASS_NAME, "handleWaitResult",
					"Woken up, and lock failed");
		}

		if (!lockState.converting) {
			/* If not converting the delete the newly created request. */
			queueRemove(lockState.lockRequest);
			if (queueIsEmpty()) {
				grantedMode = LockMode.NONE; // Setup lock for garbage collection
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
			grantWaiters(ReleaseAction.RELEASE, lockState.lockRequest);
		}
		if (lockState.getStatus() == LockStatus.TIMEOUT)
			throw new LatchException("SIMPLEDBM-ELOCK: Lock request " + lockState.parms.toString() + " timed out");
		else if (lockState.getStatus() == LockStatus.DEADLOCK)
			throw new LatchException("SIMPLEDBM-ELOCK: Lock request " + lockState.parms.toString() + " failed due to a deadlock");
		else
			throw new LatchException(
					"SIMPLEDBM-ELOCK-002: Unexpected error occurred while attempting to acqure lock " + lockState.parms.toString());
	}

	private void grantWaiters(ReleaseAction action, LockRequest r) {
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
		grantedMode = LockMode.NONE;
		waiting = false;
		converting = false;
		for (LockRequest req : getQueue()) {

			r = req;
			if (r.status == LockRequestStatus.GRANTED) {
				grantedMode = r.mode.maximumOf(grantedMode);
			}

			else if (r.status == LockRequestStatus.CONVERTING) {
				boolean can_grant;

				assert (!converting || waiting);

				can_grant = checkCompatible(r, r.convertMode);
				if (can_grant) {
					if (log.isDebugEnabled()) {
						log.debug(LOG_CLASS_NAME, "release", "Granting conversion request " + r + " because request is compatible with " + this);
					}
	                r.mode = r.convertMode.maximumOf(r.mode);
	                r.convertMode = r.mode;
	                grantedMode = r.mode.maximumOf(grantedMode);
		            /*
		             * Treat conversions as lock recursion.
		             */
	                if (!r.upgrade) {
	                	r.count++;
	                }
					r.status = LockRequestStatus.GRANTED;
					LockSupport.unpark(r.thread);
				} else {
					grantedMode = r.mode.maximumOf(grantedMode);
					converting = true;
					waiting = true;
				}
			}

			else if (r.status == LockRequestStatus.WAITING) {
				if (!converting && r.mode.isCompatible(grantedMode)) {
					if (log.isDebugEnabled()) {
						log.debug(LOG_CLASS_NAME, "release", "Granting waiting request " + r + " because not converting and request is compatible with " + this);
					}
					r.status = LockRequestStatus.GRANTED;
		            grantedMode = r.mode.maximumOf(grantedMode);
					LockSupport.unpark(r.thread);
				} else {
					if (log.isDebugEnabled() && converting) {
						log.debug(LOG_CLASS_NAME, "release", "Cannot grant waiting request " + r + " because conversion request pending");
					}
					waiting = true;
					break;
				}
			}
		}
	}

	public synchronized final LockMode getMode() {

		LockRequest lockRequest = find(Thread.currentThread());
		if (lockRequest != null) {
			return lockRequest.mode;
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
	private boolean releaseLock(LockState lockState) {
		boolean released;
		/* 3. If lock found, look for the transaction's lock request. */
		lockState.lockRequest = find(lockState.parms.owner);

		if (lockState.lockRequest == null) {
			/* 4. If not found, return success. */
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "release",
						"request not found, returning success");
			}
			throw new LatchException(
					"SIMPLEDBM-ELOCK-003: Cannot release a lock on "
							+ this
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
			throw new LatchException(
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

		if (lockState.lockRequest == queueHead() && lockState.lockRequest == queueTail()
				&& lockState.parms.action != ReleaseAction.DOWNGRADE) {
			/* 7. If sole lock request, then release the lock and return Ok. */
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "release",
						"removing sole lock, releasing lock object");
			}
			queueRemove(lockState.lockRequest);
			grantedMode = LockMode.NONE;
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
			queueRemove(lockState.lockRequest);
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
			} else {
				throw new LatchException(
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
		grantWaiters(lockState.parms.action, lockState.lockRequest);
		return released;
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
	public synchronized boolean release(ReleaseAction action,
			LockMode downgradeMode) {

		LockParams parms = new LockParams();
		parms.owner = Thread.currentThread();
		parms.action = action;
		parms.downgradeMode = downgradeMode;

		LockState lockState = new LockState(parms);

		return releaseLock(lockState);
	}

	void setLockMode(LockMode mode) {
		this.grantedMode = mode;
	}

	LockMode getLockMode() {
		return grantedMode;
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
        
		short count = 1;

		final Object owner;
		
		Thread thread;
		
		NewReadWriteUpdateLatch lockItem;
		
		boolean upgrade;

		LockRequest(NewReadWriteUpdateLatch lockItem, Object owner, LockMode mode) {
			this.lockItem = lockItem;
			this.mode = mode;
			this.convertMode = mode;
			this.thread = Thread.currentThread();
			this.owner = owner;
		}

		Object getOwner() {
			return owner;
		}

		@Override
		public String toString() {
			return "LockRequest(mode=" + mode + ", convertMode=" + convertMode + ", status=" + status + ", count=" + count + ", owner=" + owner + ", thread=" + thread + ")";
		}
	}

	public void downgradeExclusiveLock() {
		release(ReleaseAction.DOWNGRADE, LockMode.UPDATE);
	}

	public void downgradeUpdateLock() {
		release(ReleaseAction.DOWNGRADE, LockMode.SHARED);
	}

	public void exclusiveLock() {
		acquire(LockMode.EXCLUSIVE, 10, false);
	}

	public void exclusiveLockInterruptibly() throws InterruptedException {
		acquire(LockMode.EXCLUSIVE, 10, false);
	}

	public boolean isLatchedExclusively() {
		return getMode() == LockMode.EXCLUSIVE;
	}

	public boolean isLatchedForUpdate() {
		return getMode() == LockMode.UPDATE;
	}

	public void sharedLock() {
		acquire(LockMode.SHARED, 10, false);
	}

	public void sharedLockInterruptibly() throws InterruptedException {
		acquire(LockMode.SHARED, 10, false);
	}

	public boolean tryExclusiveLock() {
		try {
			acquire(LockMode.EXCLUSIVE, 0, false);
		}
		catch (LatchException e) {
			return false;
		}
		return true;
	}

	public boolean trySharedLock() {
		try {
			acquire(LockMode.SHARED, 0, false);
		}
		catch (LatchException e) {
			return false;
		}
		return true;
	}

	public boolean tryUpdateLock() {
		try {
			acquire(LockMode.UPDATE, 0, false);
		}
		catch (LatchException e) {
			return false;
		}
		return true;
	}

	public boolean tryUpgradeUpdateLock() {
		try {
			acquire(LockMode.EXCLUSIVE, 0, true);
		}
		catch (LatchException e) {
			return false;
		}
		return true;
	}

	public void unlockExclusive() {
		release(ReleaseAction.RELEASE, null);
	}

	public void unlockShared() {
		release(ReleaseAction.RELEASE, null);
	}

	public void unlockUpdate() {
		release(ReleaseAction.RELEASE, null);
	}

	public void updateLock() {
		acquire(LockMode.UPDATE, 10, false);
	}

	public void updateLockInterruptibly() throws InterruptedException {
		acquire(LockMode.UPDATE, 10, false);
	}

	public void upgradeUpdateLock() {
		acquire(LockMode.EXCLUSIVE, 10, true);
	}

	public void upgradeUpdateLockInterruptibly() throws InterruptedException {
		upgradeUpdateLock();
	}

}
