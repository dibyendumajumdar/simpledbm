/**
 * DO NOT REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Contributor(s):
 *
 * The Original Software is SimpleDBM (www.simpledbm.org).
 * The Initial Developer of the Original Software is Dibyendu Majumdar.
 *
 * Portions Copyright 2005-2014 Dibyendu Majumdar. All Rights Reserved.
 *
 * The contents of this file are subject to the terms of the
 * Apache License Version 2 (the "APL"). You may not use this
 * file except in compliance with the License. A copy of the
 * APL may be obtained from:
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the APL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the APL, the GPL or the LGPL.
 *
 * Copies of GPL and LGPL may be obtained from:
 * http://www.gnu.org/licenses/license-list.html
 */
package org.simpledbm.rss.impl.latch;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.locking.LockMode;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.util.Linkable;
import org.simpledbm.common.util.SimpleLinkedList;
import org.simpledbm.common.util.SimpleTimer;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.rss.api.latch.Latch;
import org.simpledbm.rss.api.latch.LatchException;

/**
 * @author Dibyendu Majumdar
 * @since 23 Dec 2006
 */
public final class NewReadWriteUpdateLatch implements Latch {

    /*
     * This implementation of latch is based upon the lock manager implementation.
     * The differences are:
     * Firstly, there is no hash table of locks because each instance of this
     * class _is_ the lock. Clients have a reference to the latch itself so there
     * is no need for dynamic lookup.
     * There is no support for various lock durations as these do not make
     * sense here.
     * Apart from lock conversions and downgrades, we also support lock upgrades.
     * An upgrade is like a conversion except that it is explicitly requested and does
     * not cause the reference count to go up. Hence the difference is primarily in the way
     * clients use locks. For normal lock conversions, clients are expected to treat
     * each request as a separate request, and therefore release the lock as many
     * times as requested. Upgrade (and downgrade) requests do not modify the reference
     * count.
     * Unlike Lock Manager, the owner for latches is predefined - it is always the
     * requesting thread. Hence there is no need to supply an owner.
     * Latches do not support deadlock detection, unlike locks.
     * A benefit of this implementation is that lock attempts timeout in 10 seconds
     * which is a simple way of detecting latch deadlocks.
     * 
     * The reason for creating this new implementation was the realization that
     * ReentrantReadWriteLock does not support recursion
     * properly. This is because in both implementation details of shared requests
     * are not kept. This implementation naturally is less efficient compared to the
     * other two, but does support recursion of shared lock requests.
     */

    /**
     * Latch will timeout in 10 seconds. This is a cheap way of detecting
     * deadlocks.
     */
    private static final int LOCK_TIMEOUT = 10;

    final Logger log;

    final ExceptionHandler exceptionHandler;

    /**
     * Queue of latch requests, contains granted or conversion requests followed
     * by waiting requests.
     */
//    final LinkedList<LockRequest> queue = new LinkedList<LockRequest>();
    final SimpleLinkedList<LockRequest> queue = new SimpleLinkedList<LockRequest>();

    /**
     * Current mode of the latch.
     */
    LockMode grantedMode = LockMode.NONE;

    /**
     * Flags that there are threads waiting to be granted latch requests.
     */
    boolean waiting = false;

    /**
     * Cached value from logging to improve performance. Downside is that we
     * cannot change logging dynamically.
     */
    final boolean debugEnabled;

    /**
     * Cached value from logging to improve performance. Downside is that we
     * cannot change logging dynamically.
     */
    final boolean traceEnabled;

    /**
     * Defines the various lock release methods.
     * 
     * @author Dibyendu Majumdar
     */
    enum ReleaseAction {
        RELEASE, DOWNGRADE
    }

    /**
     * Describe the status of a latch acquistion request.
     * 
     * @author Dibyendu Majumdar
     */
    public enum LockStatus {
        GRANTED, TIMEOUT, DEADLOCK
    }

    static enum LockRequestStatus {
        GRANTED, CONVERTING, WAITING, DENIED
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

        @Override
        public String toString() {
            return "LockRequestParams(owner=" + owner + ", mode=" + mode
                    + ", timeout=" + timeout + ", releaseAction=" + action
                    + ", downgradeMode=" + downgradeMode + ", upgrade="
                    + upgrade + ")";
        }
    }

    static final class LockState {
        final LockParams parms;
        LockRequest lockRequest;
        boolean converting;
        Thread prevThread;

        public LockState(LockParams parms) {
            this.parms = parms;
        }
    }

    /**
     * Creates a new LockMgrImpl, ready for use.
     */
    public NewReadWriteUpdateLatch(PlatformObjects po) {
        this.log = po.getLogger();
        this.exceptionHandler = po.getExceptionHandler();
        if (log.isDebugEnabled()) {
            debugEnabled = true;
        } else {
            debugEnabled = false;
        }
        if (log.isTraceEnabled()) {
            traceEnabled = true;
        } else {
            traceEnabled = false;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.simpledbm.locking.LockMgr#acquire(java.lang.Object,
     *      java.lang.Object, org.simpledbm.locking.LockMode,
     *      org.simpledbm.locking.LockDuration, int)
     */
    private final void acquire(LockMode mode, int timeout, boolean upgrade) {

        LockParams parms = new LockParams();
        parms.owner = Thread.currentThread();
        parms.mode = mode;
        parms.timeout = timeout;
        parms.upgrade = upgrade;

        LockState lockState = new LockState(parms);

        doAcquire(lockState);
    }

    @Override
    public synchronized String toString() {
        return "Latch(grantedMode=" + grantedMode + ", waiting=" + waiting
                + ", queue=" + queue + ")";
    }

    /**
     * Acquires a lock in the specified mode.
     */
    private void doAcquire(LockState lockState) {

        if (debugEnabled) {
            log.debug(getClass(), "doAcquire",
                    "SIMPLEDBM-DEBUG: Lock requested by "
                            + lockState.parms.owner + " for " + this
                            + ", mode=" + lockState.parms.mode);
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

        SimpleTimer timer = new SimpleTimer((lockState.parms.timeout < 0) ? -1
                : TimeUnit.NANOSECONDS.convert(lockState.parms.timeout,
                        TimeUnit.SECONDS));
        for (;;) {
            timer.await();
            synchronized (this) {
                if (lockState.lockRequest.status == LockRequestStatus.WAITING
                        || lockState.lockRequest.status == LockRequestStatus.CONVERTING) {
                    if (!timer.isExpired()) {
                        continue;
                    }
                }
                handleWaitResult(lockState);
                return;
            }
        }
    }

    /**
     * Finds the lock request belonging to the specified owner
     */
    private LockRequest find(Object owner) {
        for (LockRequest req : queue) {
            if (req.owner == owner || req.owner.equals(owner)) {
                return req;
            }
        }
        return null;
    }

    /**
     * Handles a new lock request by a thread.
     * 
     * @return true if the lock request was processed, else false to indicate
     *         that the requester must wait
     */
    private boolean handleNewRequest(LockState lockState) {
        /* 4. If not, this is the first request */
        if (debugEnabled) {
            log.debug(getClass(), "handleNewRequest",
                    "SIMPLEDBM-DEBUG: New request by thread "
                            + lockState.parms.owner);
        }

        if (lockState.parms.upgrade) {
            /*
             * An upgrade request without a prior lock request is an error.
             */
            exceptionHandler.errorThrow(this.getClass(),
                    "handleNewRequest", new LatchException(new MessageInstance(
                            LatchFactoryImpl.m_EH0001, lockState.parms)));
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
            throw new LatchException(new MessageInstance(
                    LatchFactoryImpl.m_WH0002, this, lockState.parms.mode));
        }

        /* Allocate new lock request */
        lockState.lockRequest = new LockRequest(this, lockState.parms.owner,
                lockState.parms.mode);
        queueAppend(lockState.lockRequest);
        if (can_grant) {
            /* 6. If yes, grant the lock and return success. */
            if (debugEnabled) {
                log.debug(getClass(), "handleNewRequest",
                        "SIMPLEDBM-DEBUG: There are no waiting locks and request is compatible with  "
                                + this + ", therefore granting lock");
            }
            grantedMode = lockState.parms.mode.maximumOf(grantedMode);
            return true;
        } else {
            lockState.converting = false;
            return false;
        }
    }

    /**
     * Handles a conversion request in the nowait situation.
     * 
     * @return true if conversion request was handled else false to indicate
     *         that requester must enter wait.
     */
    private boolean handleConversionRequest(LockState lockState) {
        /*
         * 11. If caller already has a granted lock request
         * then this must be a conversion request.
         */
        if (traceEnabled) {
            log.trace(getClass(), "handleConversionRequest",
                    "SIMPLEDBM-DEBUG: Lock conversion request by thread "
                            + lockState.parms.owner);
        }

        /*
         * Limitation: caller cannot attempt to lock an object
         * for which it is already waiting.
         */
        if (lockState.lockRequest.status == LockRequestStatus.CONVERTING
                || lockState.lockRequest.status == LockRequestStatus.WAITING) {
            exceptionHandler.errorThrow(this.getClass(),
                    "handleConversionRequest", new LatchException(
                            new MessageInstance(LatchFactoryImpl.m_EH0003,
                                    this, lockState.parms.owner)));
        }

        else if (lockState.lockRequest.status == LockRequestStatus.GRANTED) {
            /*
             * 12. Check whether the new request lock has the same mode as
             * previously held lock.
             */
            if (lockState.parms.mode == lockState.lockRequest.mode) {
                /* 13. If so, grant lock and return. */
                if (debugEnabled) {
                    log
                            .debug(
                                    getClass(),
                                    "handleConversionRequest",
                                    "SIMPLEDBM-DEBUG: Requested mode is the same as currently held mode, therefore granting");
                }
                if (!lockState.parms.upgrade) {
                    lockState.lockRequest.count++;
                }
                return true;
            }

            else {
                /*
                 * 14. Otherwise, check if requested lock is compatible
                 * with granted group
                 */
                boolean can_grant = checkCompatible(lockState.lockRequest,
                        lockState.parms.mode);

                if (can_grant) {
                    /* 13. If so, grant lock and return. */
                    if (debugEnabled) {
                        log.debug(getClass(),
                                "handleConversionRequest",
                                "SIMPLEDBM-DEBUG: Conversion request is compatible with granted group "
                                        + this + ", therefore granting");
                    }
                    lockState.lockRequest.mode = lockState.parms.mode
                            .maximumOf(lockState.lockRequest.mode);
                    if (!lockState.parms.upgrade) {
                        lockState.lockRequest.count++;
                    }
                    grantedMode = lockState.lockRequest.mode
                            .maximumOf(grantedMode);
                    return true;
                }

                else if (!can_grant && lockState.parms.timeout == 0) {
                    /* 15. If not, and nowait specified, return failure. */
                    throw new LatchException(new MessageInstance(
                            LatchFactoryImpl.m_WH0004, lockState.parms, this));
                }

                else {
                    lockState.converting = true;
                    return false;
                }
            }
        } else {
            exceptionHandler.errorThrow(this.getClass(),
                    "handleConversionRequest", new LatchException(
                            new MessageInstance(LatchFactoryImpl.m_EH0005)));
        }
        return false;
    }

    /**
     * Checks whether the specified lock request is compatible with the granted
     * group. Also sets the otherHolders flag if the granted group contains
     * other requests.
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
            if (debugEnabled) {
                log.debug(getClass(), "prepareToWait",
                        "SIMPLEDBM-DEBUG: Waiting for lock to be free");
            }
            lockState.lockRequest.status = LockRequestStatus.WAITING;
        } else {
            if (debugEnabled) {
                log
                        .debug(
                                getClass(),
                                "prepareToWait",
                                "SIMPLEDBM-DEBUG: Conversion NOT compatible with granted group, therefore waiting ...");
            }
            lockState.lockRequest.convertMode = lockState.parms.mode;
            lockState.lockRequest.status = LockRequestStatus.CONVERTING;
            lockState.lockRequest.upgrading = lockState.parms.upgrade;
            lockState.prevThread = lockState.lockRequest.waitingThread;
            lockState.lockRequest.waitingThread = Thread.currentThread();
        }
    }

    /**
     * Handles the result of a lock wait.
     */
    private void handleWaitResult(LockState lockState) {
        LockRequestStatus lockRequestStatus = lockState.lockRequest.status;
        if (lockRequestStatus == LockRequestStatus.GRANTED) {
            /*
             * 9. If after the wait, the lock has been granted, then return
             * success.
             */
            if (debugEnabled) {
                log.debug(getClass(), "handleWaitResult",
                        "SIMPLEDBM-DEBUG: Woken up, and lock granted");
            }
            return;
        }

        /* 10. Else return failure. */
        if (debugEnabled) {
            log.debug(getClass(), "handleWaitResult",
                    "SIMPLEDBM-DEBUG: Woken up, and lock failed");
        }

        if (!lockState.converting) {
            /* If not converting the delete the newly created request. */
            queueRemove(lockState.lockRequest);
            if (queueIsEmpty()) {
                grantedMode = LockMode.NONE;
            }
        } else {
            /* If converting, then restore old status */
            lockState.lockRequest.status = LockRequestStatus.GRANTED;
            lockState.lockRequest.convertMode = lockState.lockRequest.mode;
            lockState.lockRequest.waitingThread = lockState.prevThread;
        }
        exceptionHandler.warnAndThrow(this.getClass(),
                "handleWaitResult", new LatchException(new MessageInstance(
                        LatchFactoryImpl.m_EH0006, lockState.parms)));
    }

    private void grantWaiters(ReleaseAction action) {
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
        for (LockRequest r : getQueue()) {

            if (r.status == LockRequestStatus.GRANTED) {
                grantedMode = r.mode.maximumOf(grantedMode);
            }

            else if (r.status == LockRequestStatus.CONVERTING) {
                boolean can_grant;

                assert (!converting || waiting);

                can_grant = checkCompatible(r, r.convertMode);
                if (can_grant) {
                    if (debugEnabled) {
                        log
                                .debug(
                                        getClass(),
                                        "release",
                                        "SIMPLEDBM-DEBUG: Granting conversion request "
                                                + r
                                                + " because request is compatible with "
                                                + this);
                    }
                    r.mode = r.convertMode.maximumOf(r.mode);
                    r.convertMode = r.mode;
                    grantedMode = r.mode.maximumOf(grantedMode);
                    /*
                     * Treat conversions as lock recursion unless upgrading.
                     */
                    if (!r.upgrading) {
                        r.count++;
                    }
                    r.status = LockRequestStatus.GRANTED;
                    LockSupport.unpark(r.waitingThread);
                } else {
                    grantedMode = r.mode.maximumOf(grantedMode);
                    converting = true;
                    waiting = true;
                }
            }

            else if (r.status == LockRequestStatus.WAITING) {
                if (!converting && r.mode.isCompatible(grantedMode)) {
                    if (debugEnabled) {
                        log
                                .debug(
                                        getClass(),
                                        "release",
                                        "SIMPLEDBM-DEBUG: Granting waiting request "
                                                + r
                                                + " because not converting and request is compatible with "
                                                + this);
                    }
                    r.status = LockRequestStatus.GRANTED;
                    grantedMode = r.mode.maximumOf(grantedMode);
                    LockSupport.unpark(r.waitingThread);
                } else {
                    if (debugEnabled && converting) {
                        log
                                .debug(
                                        getClass(),
                                        "release",
                                        "SIMPLEDBM-DEBUG: Cannot grant waiting request "
                                                + r
                                                + " because conversion request pending");
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
     */
    private boolean releaseLock(LockState lockState) {
        boolean released;
        /* 3. If lock found, look for the owner's lock request. */
        lockState.lockRequest = find(lockState.parms.owner);

        if (lockState.lockRequest == null) {
            /* 4. If not found, throw exception. */
            exceptionHandler.errorThrow(this.getClass(),
                    "releaseLock", new LatchException(new MessageInstance(
                            LatchFactoryImpl.m_EH0007, this)));
        }

        if (lockState.lockRequest.status == LockRequestStatus.CONVERTING
                || lockState.lockRequest.status == LockRequestStatus.WAITING) {
            /* 5. If lock in invalid state, return error. */
            exceptionHandler.errorThrow(this.getClass(),
                    "releaseLock", new LatchException(new MessageInstance(
                            LatchFactoryImpl.m_EH0008, this)));
        }

        if (lockState.parms.action == ReleaseAction.DOWNGRADE
                && lockState.lockRequest.mode == lockState.parms.downgradeMode) {
            /*
             * If downgrade request and lock is already in target mode,
             * return success.
             */
            return true;
        }

        if (lockState.parms.action == ReleaseAction.RELEASE
                && lockState.lockRequest.count > 1) {
            /*
             * 6. If noforce, and reference count greater than 0, then do
             * not release the lock request. Decrement reference count if
             * greater than 0, and, return Ok.
             */
            if (debugEnabled) {
                log
                        .debug(getClass(), "release",
                                "SIMPLEDBM-DEBUG: Count decremented but lock not released");
            }
            lockState.lockRequest.count--;
            return false;
        }

        /*
         * Either the lock is being downgraded or it is being released and
         * its reference count == 0 or it is being forcibly released.
         */

        if (lockState.lockRequest == queueHead()
                && lockState.lockRequest == queueTail()
                && lockState.parms.action != ReleaseAction.DOWNGRADE) {
            /* 7. If sole lock request, then release the lock and return Ok. */
            if (debugEnabled) {
                log
                        .debug(getClass(), "release",
                                "SIMPLEDBM-DEBUG: Removing sole lock, releasing lock object");
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
            if (debugEnabled) {
                log.debug(getClass(), "release",
                        "SIMPLEDBM-DEBUG: Removing lock request "
                                + lockState.lockRequest
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
            LockMode mode = lockState.parms.downgradeMode
                    .maximumOf(lockState.lockRequest.mode);
            if (mode == lockState.lockRequest.mode) {
                if (debugEnabled) {
                    log.debug(getClass(), "release",
                            "SIMPLEDBM-DEBUG: Downgrading "
                                    + lockState.lockRequest + " to "
                                    + lockState.parms.downgradeMode
                                    + " and re-adjusting granted mode");
                }
                lockState.lockRequest.convertMode = lockState.lockRequest.mode = lockState.parms.downgradeMode;
            } else {
                exceptionHandler.errorThrow(this.getClass(),
                        "releaseLock", new LatchException(new MessageInstance(
                                LatchFactoryImpl.m_EH0009,
                                lockState.lockRequest.mode,
                                lockState.parms.downgradeMode)));
            }
            released = false;
        }
        /*
         * 9. Recalculate granted mode by calculating max mode amongst all
         * granted (including conversion) requests. 
         */
        grantWaiters(lockState.parms.action);
        return released;
    }

    /**
     * Release or downgrade a specified lock.
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

    void queueAppend(LockRequest request) {
//        queue.add(request);
        queue.addLast(request);
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

//    LinkedList<LockRequest> getQueue() {
    SimpleLinkedList<LockRequest> getQueue() {
        return queue;
    }

    /**
     * A LockRequest represents the request by a transaction for a lock.
     * 
     * @author Dibyendu Majumdar
     * 
     */
    static final class LockRequest extends Linkable {

        volatile LockRequestStatus status = LockRequestStatus.GRANTED;

        volatile LockMode mode;

        volatile LockMode convertMode;

        volatile short count = 1;

        final Object owner;

        volatile Thread waitingThread;

//        final NewReadWriteUpdateLatch lockItem;

        volatile boolean upgrading;

        LockRequest(NewReadWriteUpdateLatch lockItem, Object owner,
                LockMode mode) {
//            this.lockItem = lockItem;
            this.mode = mode;
            this.convertMode = mode;
            this.waitingThread = Thread.currentThread();
            this.owner = owner;
        }

        Object getOwner() {
            return owner;
        }

        @Override
        public String toString() {
            return "LockRequest(mode=" + mode + ", convertMode=" + convertMode
                    + ", status=" + status + ", count=" + count + ", owner="
                    + owner + ", waitingThread=" + waitingThread + ")";
        }
    }

    public void downgradeExclusiveLock() {
        release(ReleaseAction.DOWNGRADE, LockMode.UPDATE);
    }

    public void downgradeUpdateLock() {
        release(ReleaseAction.DOWNGRADE, LockMode.SHARED);
    }

    public void exclusiveLock() {
        acquire(LockMode.EXCLUSIVE, LOCK_TIMEOUT, false);
    }

    public void exclusiveLockInterruptibly() throws InterruptedException {
        acquire(LockMode.EXCLUSIVE, LOCK_TIMEOUT, false);
    }

    public boolean isLatchedExclusively() {
        return getMode() == LockMode.EXCLUSIVE;
    }

    public boolean isLatchedForUpdate() {
        return getMode() == LockMode.UPDATE;
    }

    public void sharedLock() {
        acquire(LockMode.SHARED, LOCK_TIMEOUT, false);
    }

    public void sharedLockInterruptibly() throws InterruptedException {
        acquire(LockMode.SHARED, LOCK_TIMEOUT, false);
    }

    public boolean tryExclusiveLock() {
        try {
            acquire(LockMode.EXCLUSIVE, 0, false);
        } catch (LatchException e) {
            return false;
        }
        return true;
    }

    public boolean trySharedLock() {
        try {
            acquire(LockMode.SHARED, 0, false);
        } catch (LatchException e) {
            return false;
        }
        return true;
    }

    public boolean tryUpdateLock() {
        try {
            acquire(LockMode.UPDATE, 0, false);
        } catch (LatchException e) {
            return false;
        }
        return true;
    }

    public boolean tryUpgradeUpdateLock() {
        try {
            acquire(LockMode.EXCLUSIVE, 0, true);
        } catch (LatchException e) {
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
        acquire(LockMode.UPDATE, LOCK_TIMEOUT, false);
    }

    public void updateLockInterruptibly() throws InterruptedException {
        acquire(LockMode.UPDATE, LOCK_TIMEOUT, false);
    }

    public void upgradeUpdateLock() {
        acquire(LockMode.EXCLUSIVE, LOCK_TIMEOUT, true);
    }

    public void upgradeUpdateLockInterruptibly() throws InterruptedException {
        upgradeUpdateLock();
    }
}
