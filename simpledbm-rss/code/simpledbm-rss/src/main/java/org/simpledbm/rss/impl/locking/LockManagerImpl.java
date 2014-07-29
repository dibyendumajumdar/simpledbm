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
package org.simpledbm.rss.impl.locking;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.locking.LockMode;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.api.thread.Scheduler;
import org.simpledbm.common.api.thread.Scheduler.Priority;
import org.simpledbm.common.util.Dumpable;
import org.simpledbm.common.util.Linkable;
import org.simpledbm.common.util.SimpleLinkedList;
import org.simpledbm.common.util.SimpleTimer;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;
import org.simpledbm.rss.api.latch.Latch;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.locking.LockDeadlockException;
import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockException;
import org.simpledbm.rss.api.locking.LockHandle;
import org.simpledbm.rss.api.locking.LockInfo;
import org.simpledbm.rss.api.locking.LockManager;
import org.simpledbm.rss.api.locking.LockTimeoutException;

/**
 * The default implementation of the LockMgr interface is a memory based lock
 * management system modeled very closely on the description of a locking system
 * in <em>Transaction Processing: Concepts and Techniques, by Jim Gray and
 * Andreas Reuter</em>.
 * <p>
 * For each lock in the system, a queue of lock requests is maintained. The
 * queue has granted requests followed by waiting requests. To allow locks to be
 * quickly found, a hash table of all locks is maintained.
 * 
 * @author Dibyendu Majumdar
 */
public final class LockManagerImpl implements LockManager {

    /*
     * This version uses a global queue of threads to be woken
     * up. It relies on garbage collection rather
     * than maintaining pools of lockBuckets and lockRequests
     * A ConcurrentLinkedQueue is used to maintain the global
     * queue.
     */


    /*
     * 
    Algorithm
    ---------

    The main algorithm for the lock manager is shown in the form of use cases.

    a001 - lock acquire
    *******************

    Main Success Scenario
    .....................

    1) Search for the lock header
    2) Lock header not found
    3) Allocate new lock header
    4) Allocate new lock request
    5) Append lock request to queue with status = GRANTED and reference count of 1.
    6) Set lock granted mode to GRANTED

    Extensions
    ..........

    2. a) Lock header found but client has no prior request for the lock.
      
      1. Do `a003 - handle new request`_.

    b) Lock header found and client has a prior GRANTED lock request.
      
      1. Do `a002 - handle lock conversion`_.

    a003 - handle new request
    *************************

    Main Success Scenario
    .....................

    1) Allocate new request.
    2) Append lock request to queue with reference count of 1.
    3) Check for waiting requests.
    4) Check whether request is compatible with granted mode.
    5) There are no waiting requests and lock request is compatible with
    granted mode.
    6) Set lock's granted mode to maximum of this request and existing granted mode.
    7) Success.

    Extensions
    ..........

    5. a) There are waiting requests or lock request is not compatible with
      granted mode.
      
      1. Do `a004 - lock wait`_.

    a002 - handle lock conversion
    *****************************

    Main Success Scenario
    .....................

    1) Check lock compatibility with granted group.
    2) Lock request is compatible with granted group.
    3) Grant lock request, and update granted mode for the request.

    Extensions
    ..........

    2. a) Lock request is incompatible with granted group.
      
      1. Do `a004 - lock wait`_.


    a004 - lock wait
    ****************

    Main Success Scenario
    .....................

    1) Wait for lock.
    2) Lock granted.
    3) Success.

    Extensions
    ..........

    2. a) Lock was not granted.
      
      1. Failure!

    b001 - release lock
    *******************

    Main Success Scenario
    .....................

    1) Decrease reference count.
    2) Sole lock request and reference count is zero.
    3) Remove lock header from hash table.
    4) Success.

    Extensions
    ..........

    2. a) Reference count greater than zero.
      
      1. Success.

    2. b) Reference count is zero and there are other requests on the lock.
      
      1. Remove request from the queue.
      2. Do `b002 - grant waiters`_.

    b002 - grant waiters
    ********************

    Main Success Scenario
    .....................

    1) Get next granted lock.
    2) Recalculate granted mode.
    3) Repeat from 1) until no more granted requests.
    4) Get next waiting request.
    5) Request is compatible with granted mode.
    6) Grant request and wake up thread waiting for the lock. Increment reference count of
    the granted request and set granted mode to maximum of current mode and granted request.
    7) Repeat from 4) until no more waiting requests.

    Extensions
    ..........

    1. a) Conversion request.
      
      1. Do `b003 - grant conversion request`_.
      2. Resume from 2).

    4. a) "conversion pending" is set (via b003).
      
      1. Done.

    5. a) Request is incompatible with granted mode.
      
      1. Done.


    b003 - grant conversion request
    *******************************

    Main Success Scenario
    .....................

    1) Do `c001 - check request compatible with granted group`_.
    2) Request is compatible.
    3) Grant conversion request.

    Extensions
    ..........

    2. a) Conversion request incompatible with granted group.
      
      1. Set "conversion pending" flag.

    c001 - check request compatible with granted group
    **************************************************

    Main Success Scenario
    .....................

    1) Get next granted request.
    2) Request is compatible with this request.
    3) Repeat from 1) until no more granted requests.

    Extensions
    ..........

    1. a) Request belongs to the caller.
      
      1. Resume from step 3).

    2. a) Request is incompatible with this request.
      
      1. Failure!

     */

	/**
	 * Prime numbers used to size the hash bucket array
	 */
    static final int primes[] = {53, 97, 193, 389, 769, 1543, 3079, 6151,
            12289, 24593, 49157, 98317, 196613, 393241, 786433};

    final Platform platform;

    final Logger log;

    final ExceptionHandler exceptionHandler;

    /**
     * Offset into {@link #primes}
     */
    private volatile int htsz = 0;

    /**
     * Tracks the number of items in the hash table
     */
    private volatile int count = 0;

    /**
     * Upper limit of number of items that can be inserted into the hash table.
     * Exceeding this causes the hash table to be resized.
     */
    private volatile int threshold = 0;

    /**
     * Used to calculate the hash table size threshold. Expressed as a
     * percentage of hash table size.
     */
    private final float loadFactor = 0.75f;

    /**
     * Hash table of locks. The hash table is dynamically resized if necessary.
     */
    private LockBucket[] locktable;

    /**
     * Size of the hash table. This is always equal to primes[htsz]. Can
     * change when hash table is resized.
     */
    private int N;

    /**
     * List of lock event listeners.
     */
    private final ArrayList<LockEventListener> lockEventListeners = new ArrayList<LockEventListener>();

    /**
     * Map of waiting lock requesters, to aid deadlock detection. Keyed by lock
     * owner.
     */
    private final Map<Object, LockWaiter> waiters = Collections
            .synchronizedMap(new HashMap<Object, LockWaiter>());

    /**
     * A queue of threads to be woken up is maintained in the
     * wakeupQ.
     */
    final ConcurrentLinkedQueue<Thread> wakeupQ = new ConcurrentLinkedQueue<Thread>();
    
    /**
     * To keep the algorithm simple, the deadlock detector uses a global
     * exclusive lock on the lock manager. The lock manager itself acquires
     * shared locks during normal operations, thus avoiding conflict with the
     * deadlock detector.
     */
    private final Latch globalLock;

    /**
     * If set, this instructs the LockManager to stop. No further requests
     * should be accepted.
     */
    volatile boolean stop = false;

    /**
     * Handle to the deadlock detector task, which is run via the SimpleDBM
     * scheduler.
     */
    ScheduledFuture<?> deadlockDetectorThread;

    /**
     * The interval between deadlock detection scans, specified in seconds.
     * Default is 10 seconds.
     */
    int deadlockDetectorInterval = 10;

    /**
     * Cached value from logging to improve performance. Downside is that we
     * cannot change logging dynamically.
     */
    static boolean debugEnabled;

    /**
     * Cached value from logging to improve performance. Downside is that we
     * cannot change logging dynamically.
     */
    static boolean traceEnabled;

    /**
     * Defines the various lock release methods.
     *
     * @author Dibyendu Majumdar
     */
    enum ReleaseAction {
        RELEASE, DOWNGRADE
    }

    // Lock Manager messages
    static Message m_EC0001 = new Message('R', 'C', MessageType.ERROR, 1,
            "Lock request {0} timed out");
    static Message m_WC0002 = new Message('R', 'C', MessageType.WARN, 2,
            "Lock request {0} failed due to a deadlock");
    static Message m_EC0099 = new Message('R', 'C', MessageType.ERROR, 99,
            "Unexpected error occurred while attempting to acquire lock request {0}");
    static Message m_EC0003 = new Message(
            'R',
            'C',
            MessageType.ERROR,
            3,
            "Invalid request because lock requested {0} is already being waited for by requester {1}");
    static Message m_WC0004 = new Message(
            'R',
            'C',
            MessageType.WARN,
            4,
            "Lock request {0} is not compatible with granted group {1}, timing out because this is a conditional request");
    static Message m_EC0005 = new Message('R', 'C', MessageType.ERROR, 5,
            "Unexpected error while handling conversion request");
    static Message m_EC0006 = new Message('R', 'C', MessageType.ERROR, 6,
            "Invalid call: no lock request associated with this handle");
    static Message m_EC0008 = new Message('R', 'C', MessageType.ERROR, 8,
            "Invalid request to release lock {0} as it is being waited for");
    static Message m_EC0009 = new Message('R', 'C', MessageType.ERROR, 9,
            "Invalid downgrade request: mode held {0}, mode to downgrade to {1}");
    static Message m_EC0010 = new Message('R', 'C', MessageType.ERROR, 10,
            "Listener {0} failed unexpectedly: lock parameters {1}");
    static Message m_WC0011 = new Message('R', 'C', MessageType.WARN, 11,
            "Detected deadlock cycle: {2} {3}");
    static Message m_IC0012 = new Message('R', 'C', MessageType.INFO, 12,
            "Deadlock detector STARTED");
    static Message m_IC0013 = new Message('R', 'C', MessageType.INFO, 13,
            "Deadlock detector STOPPED");
    static Message m_IC0014 = new Message('R', 'C', MessageType.INFO, 14,
            "Dumping lock table");
    static Message m_IC0015 = new Message('R', 'C', MessageType.INFO, 15,
            "LockItem = {0}");


    /**
     * Creates a new LockMgrImpl, ready for use.
     */
    public LockManagerImpl(PlatformObjects po, LatchFactory latchFactory,
            Properties p) {
        platform = po.getPlatform();
        log = po.getLogger();
        exceptionHandler = po.getExceptionHandler();
        globalLock = latchFactory.newReadWriteLatch();
        htsz = 0;
        count = 0;
        N = primes[htsz];
        locktable = new LockBucket[N];
        for (int i = 0; i < N; i++) {
            locktable[i] = new LockBucket();
        }
        threshold = (int) (N * loadFactor);
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

    public void start() {
        deadlockDetectorThread = platform.getScheduler()
                .scheduleWithFixedDelay(Scheduler.Priority.SERVER_TASK,
                        new DeadlockDetector(this), deadlockDetectorInterval,
                        deadlockDetectorInterval, TimeUnit.SECONDS);
        log.info(getClass(), "start", new MessageInstance(
                m_IC0012).toString());
    }

    public void shutdown() {
        stop = true;
        deadlockDetectorThread.cancel(false);
        log.info(getClass(), "shutdown", new MessageInstance(
                m_IC0013).toString());
    }

    /**
     * Grow the hash table to the next size
     */
    private void rehash() {

        if (htsz == primes.length - 1) {
            return;
        }
        if (!globalLock.tryExclusiveLock()) {
            return;
        }
        try {
            if (htsz == primes.length - 1) {
                /*
                 * Already at maximum size.
                 */
                return;
            }
            int newHashTableSize = primes[++htsz];
            if (debugEnabled) {
                log.debug(getClass(), "rehash",
                        "SIMPLEDBM-DEBUG: Growing hash table size from "
                                + N + " to " + newHashTableSize);
            }
            LockBucket[] newLockHashTable = new LockBucket[newHashTableSize];
            for (int i = 0; i < newHashTableSize; i++) {
                newLockHashTable[i] = new LockBucket();
            }
            for (int i = 0; i < N; i++) {
                LockBucket bucket = locktable[i];
                for (LockItem item = bucket.head; item != null; item = item.next) {
                    int h = (item.target.hashCode() & 0x7FFFFFFF)
                            % newHashTableSize;
                    LockBucket newBucket = newLockHashTable[h];
                    newBucket.addLast(item);
                }
            }
            LockBucket[] oldLockHashTable = locktable;
            int oldHashTableSize = N;
            locktable = newLockHashTable;
            N = newHashTableSize;
            threshold = (int) (N * loadFactor);
            for (int i = 0; i < oldHashTableSize; i++) {
                LockBucket bucket = oldLockHashTable[i];
                bucket.head = null;
                bucket.tail = null;
                oldLockHashTable[i] = null;
            }
        } finally {
            globalLock.unlockExclusive();
        }
    }

    public void dumpLockTable() {
        globalLock.sharedLock();
        log.info(getClass(), "dumpLockTable", new MessageInstance(
                m_IC0014).toString());
        try {
            for (int i = 0; i < N; i++) {
                LockBucket bucket = locktable[i];
                synchronized (bucket) {
                    for (LockItem item = bucket.head; item != null; item = item.next) {
                        log.info(getClass(), "dumpLockTable",
                                new MessageInstance(m_IC0015, item).toString());
                    }
                }
            }
        } finally {
            globalLock.unlockShared();
        }
    }

    final LockBucket h(Object lockable) {
        int h = (lockable.hashCode() & 0x7FFFFFFF) % N;
        return locktable[h];
    }

    public final LockMode findLock(Object owner, Object lockable) {

        LockMode m = LockMode.NONE;
        globalLock.sharedLock();
        try {
            LockBucket b = h(lockable);
            synchronized (b) {
                LockItem lockItem = b.findLock(lockable);
                if (lockItem != null) {
                    LockRequest r = lockItem.find(owner);
                    if (r != null) {
                        m = r.mode;
                    }
                }
            }
        } finally {
            globalLock.unlockShared();
        }
        return m;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.simpledbm.locking.LockMgr#acquire(java.lang.Object,
     *      java.lang.Object, org.simpledbm.locking.LockMode,
     *      org.simpledbm.locking.LockDuration, int)
     */

    public final boolean acquire(Object owner, Object lockable, LockMode mode,
                                 LockDuration duration, int timeout) {

        if (count > threshold) {
            rehash();
        }

        globalLock.sharedLock();
        LockStatus status = LockStatus.TIMEOUT;
        try {
            status = doAcquire(owner, lockable, mode, duration, timeout);
            if (duration == LockDuration.INSTANT_DURATION
                    && status == LockStatus.GRANTED) {
                /*
                 * Handle the case where the lock was granted after a wait.
                 */
                doReleaseInternal(ReleaseAction.RELEASE, owner, lockable, LockMode.NONE, false);
            }
        } finally {
            globalLock.unlockShared();
        }
        return status == LockStatus.GRANTED || status == LockStatus.GRANTABLE;
    }


    /**
     * Acquires a lock in the specified mode. Handles most of the cases except
     * the case where an INSTANT_DURATION lock needs to be waited for. This case
     * requires the lock to be released after it has been granted; the lock
     * release is handled by {@link #acquire acquire}.
     */
    private LockStatus doAcquire(Object o, Object lockable, LockMode m, LockDuration d, int timeout) {

        if (debugEnabled) {
            log.debug(getClass(), "acquire",
                    "SIMPLEDBM-DEBUG: Lock requested by " + o
                            + " for " + lockable + ", mode="
                            + m + ", duration="
                            + d);
        }

        LockItem l;
        LockRequest r;
        boolean converting;
        LockStatus status;

        LockBucket b = h(lockable);
        synchronized (b) {
            /* 1. Search for the lock. */
            l = b.findLock(lockable);
            if (l == null) {
                /*
             * 2. If not found, this is a new lock and therefore grant the
             * lock, and return success.
             */
                return handleNewLock(b, o, lockable, m, d);
            }
            /*
            * 3. Else check if requesting transaction already has a lock
            * request.
            */
            r = l.find(o);
            if (r == null) {
                r = l.handleNewRequest(b, o, lockable, m, d, timeout);
                status = checkStatus(d, r);
            } else {
                status = l.handleConversionRequest(r, m, d, timeout);
            }
            if (status == LockStatus.GRANTED || status == LockStatus.GRANTABLE) {
                return status;
            }
            converting = status == LockStatus.DOWAITCONVERT;
            /* 8. Wait for the lock to be available/compatible. */
            l.prepareToWait(r, m, d, converting);
            notifyLockEventListeners(o, lockable, m);
            /*
            * Add request to list of waiters to allow deadlock detection.
            */
            LockWaiter waiter = new LockWaiter(r, Thread
                    .currentThread());
            waiters.put(o, waiter);
        }

        /*
        * Global lock is released only after the waiter list has been updated.
        */
        SimpleTimer timer = new SimpleTimer((timeout < 0) ? -1
                : TimeUnit.NANOSECONDS.convert(timeout,
                TimeUnit.SECONDS));
        for (; ;) {
            globalLock.unlockShared();
            try {
                timer.await();
            } finally {
                globalLock.sharedLock();
            }
            /*
            * As the hash table may have been resized while we were waiting, we
            * need to recalculate the bucket.
            */

            b = h(lockable);
            synchronized (b) {
                if (r.status == LockRequestStatus.WAITING || r.status == LockRequestStatus.CONVERTING) {
                    if (!timer.isExpired()) {
                        continue;
                    }
                }
                waiters.remove(o);
                status = handleWaitResult(b, l, r, converting, wakeupQ);
            }
            wakeupThreads();
            break;
        }
        return status;
    }

    private final void wakeupThreads() {
        Thread t;
        while ( (t = wakeupQ.poll()) != null ) {
            LockSupport.unpark(t);
        }
    }

    private final LockStatus checkStatus(LockDuration d, LockRequest r) {
        LockStatus status;
        if (r == null) {
            assert (d == LockDuration.INSTANT_DURATION);
            status = LockStatus.GRANTABLE;
        } else {
            if (r.status == LockRequestStatus.WAITING) {
                status = LockStatus.DOWAIT;
            } else {
                status = LockStatus.GRANTED;
            }
        }
        return status;
    }

    /**
     * Handles the case where there aren't any locks on the target.
     */
    private final LockStatus handleNewLock(LockBucket b, Object o, Object lockable, LockMode m, LockDuration d) {
        if (debugEnabled) {
            log
                    .debug(getClass(), "acquire",
                            "SIMPLEDBM-DEBUG: Lock not found, therefore granting immediately");
        }

        if (d != LockDuration.INSTANT_DURATION) {
            LockItem l = new LockItem(lockable, m);
            LockRequest r = new LockRequest(l, o, m, d, LockRequestStatus.GRANTED, (short) 1);
            l.addLast(r);
            b.addLast(l);
            count++;
            return LockStatus.GRANTED;
        }
        return LockStatus.GRANTABLE;
    }

    /**
     * Handles the result of a lock wait.
     */
    private LockStatus handleWaitResult(LockBucket b, LockItem l, LockRequest r, boolean converting, final ConcurrentLinkedQueue<Thread> wakeupQ) {
        LockStatus status;
        if (r.status == LockRequestStatus.GRANTED) {
            status = LockStatus.GRANTED;
        } else if (r.status == LockRequestStatus.DENIED) {
            status = LockStatus.DEADLOCK;
        } else {
            status = LockStatus.TIMEOUT;
        }

        if (status == LockStatus.GRANTED) {
            /*
             * 9. If after the wait, the lock has been granted, then return
             * success.
             */
            if (debugEnabled) {
                log.debug(getClass(), "handleWaitResult",
                        "SIMPLEDBM-DEBUG: Woken up, and lock granted");
            }
            return status;
        }

        /* 10. Else return failure. */
        if (debugEnabled) {
            log.debug(getClass(), "handleWaitResult",
                    "SIMPLEDBM-DEBUG: Woken up, and lock failed");
        }

        /* 10. Else return failure. */
        if (!converting) {
            /* If not converting the delete the newly created request. */
            l.remove(r, b);
            if (l.head == null) {
                b.remove(l);
                count--;
                l = null;
            }
        } else {
            /* If converting, then restore old status */
            r.status = LockRequestStatus.GRANTED;
            r.convertMode = r.mode;
        }
        if (status == LockStatus.DEADLOCK && l != null) {
            /*
            * If we have been chosen as a deadlock victim, then we need to grant the
            * lock to the waiter who has won the deadlock.
            */
            l.grantWaiters(wakeupQ);
        }
        if (status == LockStatus.TIMEOUT) {
            exceptionHandler.warnAndThrow(getClass(),
                    "handleWaitResult", new LockTimeoutException(
                            new MessageInstance(m_EC0001, r)));
        } else if (status == LockStatus.DEADLOCK) {
            throw new LockDeadlockException(new MessageInstance(m_WC0002,
                    r));
        } else {
            exceptionHandler.warnAndThrow(getClass(),
                    "handleWaitResult", new LockException(new MessageInstance(
                            m_EC0099, r)));
        }
        return LockStatus.ERROR;
    }


    public final boolean release(Object o, Object lockable, boolean force) {
        boolean ok;
        globalLock.sharedLock();
        try {
            ok = doReleaseInternal(ReleaseAction.RELEASE, o, lockable, LockMode.NONE, force);
        }
        finally {
            globalLock.unlockShared();
        }
        return ok;
    }

    public final boolean downgrade(Object o, Object lockable, LockMode downgradeTo) {
        boolean ok;
        globalLock.sharedLock();
        try {
            ok = doReleaseInternal(ReleaseAction.DOWNGRADE, o, lockable, downgradeTo, false);
        }
        finally {
            globalLock.unlockShared();
        }
        return ok;
    }

    final boolean doReleaseInternal(ReleaseAction a, Object o, Object lockable, LockMode downgradeMode, boolean force) {
        LockBucket b = h(lockable);
        boolean ok = false;
        synchronized (b) {
            /* 1. Search for the lock. */
            LockItem l = b.findLock(lockable);
            if (l == null) {
                return true;
            }
            ok = releaseLock(a, o, downgradeMode, force, b, l);
        }
        wakeupThreads();
        return ok;
    }

    /**
     * Release or downgrade a specified lock.
     * <p/>
     * <p>
     * Algorithm:
     * <ol>
     * <li>1. Search for the lock.</li>
     * <li>2. If not found, return Ok.</li>
     * <li>3. If found, look for the transaction's lock request.</li>
     * <li>4. If not found, return Ok.</li>
     * <li>5. If lock request is in invalid state, return error.</li>
     * <li>6. If noforce and not downgrading, and reference count greater than
     * 0, then do not release the lock request. Decrement reference count and
     * return Ok.</li>
     * <li>7. If sole lock request and not downgrading, then release the lock
     * and return Ok.</li>
     * <li>8. If not downgrading, delete the lock request from the queue.
     * Otherwise, downgrade the mode assigned to the lock request.
     * <p/>
     * </li>
     * <li>9. Recalculate granted mode by calculating max mode amongst all
     * granted (including conversion) requests. If a conversion request is
     * compatible with all other granted requests, then grant the conversion,
     * recalculating granted mode. If a waiting request is compatible with
     * granted mode, and there are no pending conversion requests, then grant
     * the request, and recalculate granted mode. Otherwise, we are done.</li>
     * </ol>
     * </p>
     * <p>
     * Note that this means that FIFO is respected for waiting requests, but
     * conversion requests are granted as soon as they become compatible. Also,
     * note that if a conversion request is pending, waiting requests cannot be
     * granted.
     * </p>
     * </p>
     */
    boolean releaseLock(ReleaseAction a, Object o, LockMode downgradeMode, boolean force, LockBucket b, LockItem l) {

        /* look for the transaction's lock request. */
        LockRequest r = l.find(o);
        if (r == null) {
            /* 4. If not found, return success. */
            /*
            * Rather than throwing an exception, we return success. This allows
            * us to use this method in situations where for reasons of efficiency,
            * the client cannot track the status of lock objects, and therefore
            * may end up trying to release the same lock multiple times.
            */
            if (debugEnabled) {
                log
                        .debug(getClass(), "release",
                                "SIMPLEDBM-DEBUG: request not found, returning success");
            }
            return true;
        }
        if (r.status == LockRequestStatus.CONVERTING
                || r.status == LockRequestStatus.WAITING) {
            /* 5. If lock in invalid state, return error. */
            exceptionHandler.errorThrow(getClass(),
                    "releaseLock", new LockException(new MessageInstance(
                            m_EC0008, r)));
        }


        if (a == ReleaseAction.RELEASE) {
            if (!force) {
                if (r.duration == LockDuration.COMMIT_DURATION) {
                    /*
                      * If noforce, and lock is held for commit duration, then do
                      * not release the lock request.
                      */
                    if (debugEnabled) {
                        log
                                .debug(getClass(), "release",
                                        "SIMPLEDBM-DEBUG: Lock not released as it is held for commit duration");
                    }

                    return false;
                }
                r.count--;
                if (r.count > 0) {
                    /*
                      * If noforce, and reference count greater than 0, then do
                      * not release the lock request.
                      */
                    if (debugEnabled) {
                        log
                                .debug(getClass(), "release",
                                        "SIMPLEDBM-DEBUG: Count decremented but lock not released");
                    }

                    return false;
                }
            }
            if (debugEnabled) {
                log.debug(getClass(), "release",
                        "SIMPLEDBM-DEBUG: Removing lock request "
                                + r
                                + " and re-adjusting granted mode");
            }

            l.remove(r, b);
            if (l.head == null) {
                if (debugEnabled) {
                    log
                            .debug(getClass(), "release",
                                    "SIMPLEDBM-DEBUG: Removing sole lock, releasing lock object");
                }

                b.remove(l);
                count--;
                return true;
            }
        } else /* if a == DOWNGRADE */ {
            /*
            * We need to determine whether is a valid downgrade request. To
            * do so, we do a reverse check - ie, if the new mode could have
            * been upgraded to current mode, then it is okay to downgrade.
            */
            if (r.mode == downgradeMode) {
                /*
                 * If downgrade request and lock is already in target mode,
                 * return success.
                 */
                return true;
            }
            LockMode mode = downgradeMode.maximumOf(r.mode);
            if (mode == r.mode) {
                if (debugEnabled) {
                    log.debug(getClass(), "release",
                            "SIMPLEDBM-DEBUG: Downgrading "
                                    + r + " to "
                                    + downgradeMode
                                    + " and re-adjusting granted mode");
                }

                r.mode = downgradeMode;
                r.convertMode = downgradeMode;
            } else {
                exceptionHandler.errorThrow(getClass(),
                        "releaseLock", new LockException(new MessageInstance(
                                m_EC0009, r.mode,
                                downgradeMode)));
                return false;
            }
        }
        /*
       * 9. Recalculate granted mode by calculating max mode amongst all
       * granted (including conversion) requests.
       */
        l.grantWaiters(wakeupQ);
        return true;
    }


    /* (non-Javadoc)
    * @see org.simpledbm.rss.api.locking.LockManager#getLocks(java.lang.Object, org.simpledbm.rss.api.locking.LockMode)
    */

    public Object[] getLocks(Object owner, LockMode mode) {
        ArrayList<Object> locks = new ArrayList<Object>();
        globalLock.sharedLock();
        try {
            for (int i = 0; i < locktable.length; i++) {
                LockBucket bucket = locktable[i];
                synchronized (bucket) {
                    for (LockItem item = bucket.head; item != null; item = item.next) {
                        LockRequest lockRequest = item.find(owner);
                        if (lockRequest != null
                                && lockRequest.status == LockRequestStatus.GRANTED) {
                            if (mode == null || lockRequest.mode == mode) {
                                locks.add(item.target);
                            }
                        }
                    }
                }
            }
            return locks.toArray();
        } finally {
            globalLock.unlockShared();
        }
    }

    public synchronized void addLockEventListener(LockEventListener listener) {
        lockEventListeners.add(listener);
    }

    public synchronized void clearLockEventListeners() {
        lockEventListeners.clear();
    }

    private void notifyLockEventListeners(Object o, Object lockable, LockMode m) {
        for (LockEventListener listener : lockEventListeners) {
            try {
                listener.beforeLockWait(o, lockable, m);
            } catch (Exception e) {
                log.error(getClass(),
                        "notifyLockEventListeners", new MessageInstance(
                                m_EC0010, listener, o, lockable, m).toString(),
                        e);
            }
        }
    }

    private boolean findDeadlockCycle(LockWaiter me) {
        if (me.visited) {
            return false;
        } else {
            me.visited = true;
        }
        LockWaiter him;

        assert me.cycle == null;
        assert me.myLockRequest.status == LockRequestStatus.WAITING
                || me.myLockRequest.status == LockRequestStatus.CONVERTING;

        if (me.myLockRequest.status == LockRequestStatus.CONVERTING) {
            LockMode mode = me.myLockRequest.convertMode;

            /*
             * Look at everyone that holds the lock
             */
            for (LockRequest them = me.myLockRequest.lockItem.head; them != null; them = them.next) {
                if (them.owner == me.myLockRequest.owner) {
                    continue;
                }
                if (them.status == LockRequestStatus.DENIED) {
                    continue;
                }
                if (them.status == LockRequestStatus.WAITING) {
                    break;
                }
                boolean incompatible = !them.mode.isCompatible(mode);
                if (incompatible) {
                    him = waiters.get(them.owner);
                    if (him == null) {
                        return false;
                    }
                    me.cycle = him;
                    if (him.cycle != null) {
                        /*
                         * Choose the victim:
                         * Prefer the manual duration locker to be a victim
                         */
                        if (him.myLockRequest.duration == LockDuration.MANUAL_DURATION
                                && me.myLockRequest.duration == LockDuration.COMMIT_DURATION) {
                            /*
                             * Swap him and me.
                             */
                            LockWaiter temp = me;
                            me = him;
                            him = temp;
                        }
                        log.warn(log.getClass(), "findDeadlockCycle",
                                new MessageInstance(m_WC0011, me.myLockRequest,
                                        him.myLockRequest,
                                        me.myLockRequest.lockItem,
                                        him.myLockRequest.lockItem).toString());

                        me.myLockRequest.status = LockRequestStatus.DENIED;
                        LockSupport.unpark(me.thread);
                        return true;
                    }
                    boolean result = findDeadlockCycle(him);
                    //me.cycle = null;
                    if (result) {
                        return result;
                    }
                }
            }
        } else if (me.myLockRequest.status == LockRequestStatus.WAITING) {
            LockMode mode = me.myLockRequest.mode;

            /*
             * Look at everyone ahead of me in the queue
             */
            for (LockRequest them = me.myLockRequest.lockItem.head; them != null; them = them.next) {
                if (them.owner == me.myLockRequest.owner) {
                    break;
                }
                if (them.status == LockRequestStatus.DENIED) {
                    continue;
                }
                boolean incompatible = !them.mode.isCompatible(mode);
                if (incompatible || them.status == LockRequestStatus.CONVERTING
                        || them.status == LockRequestStatus.WAITING) {
                    him = waiters.get(them.owner);
                    if (him == null) {
                        return false;
                    }
                    me.cycle = him;
                    if (him.cycle != null) {
                        /*
                         * Choose the victim:
                         * Prefer the manual duration locker to be a victim
                         */
                        if (him.myLockRequest.duration == LockDuration.MANUAL_DURATION
                                && me.myLockRequest.duration == LockDuration.COMMIT_DURATION) {
                            /*
                             * Swap him and me.
                             */
                            LockWaiter temp = me;
                            me = him;
                            him = temp;
                        }
                        log.warn(log.getClass(), "findDeadlockCycle",
                                new MessageInstance(m_WC0011, me.myLockRequest,
                                        him.myLockRequest,
                                        me.myLockRequest.lockItem,
                                        him.myLockRequest.lockItem).toString());
                        me.myLockRequest.status = LockRequestStatus.DENIED;
                        LockSupport.unpark(me.thread);
                        return true;
                    }
                    boolean result = findDeadlockCycle(him);
                    //me.cycle = null;
                    if (result) {
                        return result;
                    }
                }
            }
        }

        me.cycle = null;
        return false;
    }

    void detectDeadlocks() {
        /*
         * The deadlock detector is a very simple implementation
         * based upon example shown in the Transaction Processing,
         * by Jim Gray and Andreas Reuter.
         * See sections 7.11.3 and section 8.5.
         */
        int retry = 0;
        for (; retry < 5; retry++) {
            if (globalLock.tryExclusiveLock()) {
                break;
            }
            Thread.yield();
        }
        if (retry == 5) {
            return;
        }
        try {
            LockWaiter[] waiterArray = waiters.values().toArray(
                    new LockWaiter[0]);
            for (LockWaiter waiter : waiterArray) {
                waiter.cycle = null;
                waiter.visited = false;
            }
            for (LockWaiter waiter : waiterArray) {
                findDeadlockCycle(waiter);
            }
        } finally {
            globalLock.unlockExclusive();
        }
    }

    static enum LockRequestStatus {
        GRANTED, CONVERTING, WAITING, DENIED
    }

    /**
     * A LockRequest represents the request by a transaction for a lock.
     *
     * @author Dibyendu Majumdar
     */
    static final class LockRequest {

        LockRequestStatus status = LockRequestStatus.WAITING;

        LockMode mode;

        LockMode convertMode;

        LockDuration convertDuration;

        int count = 0;

        LockDuration duration;

        final Object owner;

        final LockItem lockItem;

        volatile Thread thread;

        LockRequest next;
        LockRequest prev;

        LockRequest(LockItem l, Object o, LockMode m, LockDuration d, LockRequestStatus status, int ref_count) {
            lockItem = l;
            duration = convertDuration = d;
            mode = convertMode = m;
            this.status = status;
            count = ref_count;
            owner = o;
            thread = null;
            next = null;
            prev = null;
        }

        @Override
        public String toString() {
            return "LockRequest(mode=" + mode + ", convertMode=" + convertMode
                    + ", status=" + status + ", count=" + count + ", owner="
                    + owner + ", thread=" + thread + ", duration=" + duration
                    + ")";
        }
    }

    /**
     * Describe the status of a lock acquisition request.
     *
     * @author Dibyendu Majumdar
     */
    public enum LockStatus {
        GRANTED, GRANTABLE, TIMEOUT, DEADLOCK, ERROR, DOWAIT, DOWAITCONVERT
    }

    static class LockBucket {

        LockItem head;
        LockItem tail;

        final void addLast(LockItem l) {
            l.prev = tail;
            l.next = null;
            if (head == null) {
                head = l;
            }
            if (tail != null) {
                tail.next = l;
            }
            tail = l;
        }

        final void remove(LockItem l) {
            LockItem next = l.next;
            LockItem prev = l.prev;
            if (next != null) {
                next.prev = prev;
            } else {
                tail = prev;
            }
            if (prev != null) {
                prev.next = next;
            } else {
                head = next;
            }
            l.prev = null;
        }

        final LockItem findLock(Object lockable) {
            for (LockItem l = head; l != null; l = l.next) {
                if (l.target == lockable || l.target.equals(lockable)) {
                    return l;
                }
            }
            return null;
        }

    }

    /**
     * LockItem is the lock header for each lockable target. Against each
     * lockable object, there is a queue of lock requests, a granted mode, and
     * waiting status.
     *
     * @author Dibyendu
     */
    static class LockItem implements Dumpable {

        final Object target;

        LockMode grantedMode;

        boolean waiting;

        LockItem next;
        LockItem prev;

        LockRequest head;
        LockRequest tail;

        LockItem(Object t, LockMode m) {
            target = t;
            grantedMode = m;
        }

        final void addLast(LockRequest r) {
            r.prev = tail;
            r.next = null;
            if (head == null) {
                head = r;
            }
            if (tail != null) {
                tail.next = r;
            }
            tail = r;
        }

        final void remove(LockRequest r, LockBucket b) {
            LockRequest next = r.next;
            LockRequest prev = r.prev;
            if (next != null) {
                next.prev = prev;
            } else {
                tail = prev;
            }
            if (prev != null) {
                prev.next = next;
            } else {
                head = next;
            }
        }

        final LockRequest find(Object owner) {
            for (LockRequest r = head; r != null; r = r.next) {
                if (r.owner == owner || r.owner.equals(owner)) {
                    return r;
                }
            }
            return null;
        }

        /**
         * Checks whether the specified lock request is compatible with the granted
         * group. Also sets the otherHolders flag if the granted group contains
         * other requests.
         */
        final boolean checkCompatible(LockRequest request,
                                      LockMode mode) {

            boolean iscompatible = true;

            /* Check if there are other holders */
            for (LockRequest other = head; other != null; other = other.next) {
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
         * Handles a new lock request when the lock is already held by some other.
         *
         * @return null if lock was granted and the request was instantduration, else return new request
         *         with status granted or waiting.
         */
        final LockRequest handleNewRequest(LockBucket b, Object o, Object lockable, LockMode m, LockDuration d, int timeout) {
            /* 4. If not, this is the first request by the transaction. */
//                if (debugEnabled) {
//                    log.debug(getClass(), "handleNewRequest",
//                            "SIMPLEDBM-DEBUG: New request by transaction "
//                                    + o + " for target "
//                                    + lockable);
//                }

            /*
            * 5. Check if lock can be granted. This is true if there are no
            * waiting requests and the new request is compatible with
            * existing grant mode.
            */
            boolean can_grant = (!waiting && m.isCompatible(grantedMode));
            if (d == LockDuration.INSTANT_DURATION && can_grant) {
                return null;
            } else if (!can_grant && timeout == 0) {
                // FIXME
                throw new LockTimeoutException(new MessageInstance(m_WC0004,
                        lockable, this));
            }
            LockRequest r = new LockRequest(this, o, m, d, LockRequestStatus.WAITING, (short) 0);
            addLast(r);
            if (can_grant) {
                /* 6. If yes, grant the lock and return success. */
//        if (debugEnabled) {
//            log.debug(getClass(), "handleNewRequest",
//                    "SIMPLEDBM-DEBUG: There are no waiting locks and request is compatible with  "
//                            + context.lockitem
//                            + ", therefore granting lock");
//        }
                r.status = LockRequestStatus.GRANTED;
                r.count = 1;
                grantedMode = m.maximumOf(grantedMode);
            }
            return r;
        }

        /**
         * Handles a conversion request in the nowait situation.
         *
         * @return true if conversion request was handled else false to indicate
         *         that requester must enter wait.
         */
        final LockStatus handleConversionRequest(LockRequest r, LockMode m, LockDuration d, int timeout) {

//            if (traceEnabled) {
//                log.trace(getClass(), "handleConversionRequest",
//                        "SIMPLEDBM-DEBUG: Lock conversion request by transaction "
//                                + context.parms.owner + " for target "
//                                + context.parms.lockable);
//            }

            /*
             * Limitation: a transaction cannot attempt to lock an object
             * for which it is already waiting.
             */


            /*
            * 11. If calling transaction already has a granted lock request
            * then this must be a conversion request.
            */

            if (r.status == LockRequestStatus.CONVERTING || r.status == LockRequestStatus.WAITING) {
//                exceptionHandler.errorThrow(getClass(),
//                        "handleConversionRequest",
//                        new LockException(new MessageInstance(m_EC0003,
//                                context.parms.lockable, context.parms.owner)));
                throw new LockException(new MessageInstance(m_EC0003,
                        target, r.owner));
            }
            LockStatus status = LockStatus.ERROR;
            if (r.status == LockRequestStatus.GRANTED) {
                /*
                 * 12. Check whether the new request lock is same mode as
                 * previously held lock.
                 */
                if (m == r.mode) {
                    /* 13. If so, grant lock and return. */
//                    if (debugEnabled) {
//                        log
//                                .debug(
//                                        getClass(),
//                                        "handleConversionRequest",
//                                        "SIMPLEDBM-DEBUG: Requested mode is the same as currently held mode, therefore granting");
//                    }
                    if (d == LockDuration.INSTANT_DURATION) {
                        status = LockStatus.GRANTABLE;
                    } else {
                        r.count++;
                        if (r.duration == LockDuration.MANUAL_DURATION &&
                                d == LockDuration.COMMIT_DURATION) {
                            r.duration = LockDuration.COMMIT_DURATION;
                        }
                        status = LockStatus.GRANTED;
                    }
                } else {
                    /*
                     * 14. Otherwise, check if requested lock is compatible
                     * with granted group
                     */
                    boolean can_grant = checkCompatible(r, m);
                    if (can_grant) {
                        /* 13. If so, grant lock and return. */
//                        if (debugEnabled) {
//                            log.debug(getClass(),
//                                    "handleConversionRequest",
//                                    "SIMPLEDBM-DEBUG: Conversion request is compatible with granted group "
//                                            + context.lockitem
//                                            + ", therefore granting");
//                        }
                        if (d != LockDuration.INSTANT_DURATION) {
                            r.mode = m.maximumOf(r.mode);
                            r.count++;
                            if (r.duration == LockDuration.MANUAL_DURATION &&
                                    d == LockDuration.COMMIT_DURATION) {
                                r.duration = LockDuration.COMMIT_DURATION;
                            }
                            grantedMode = r.mode.maximumOf(grantedMode);
                            status = LockStatus.GRANTED;
                        } else {
                            status = LockStatus.GRANTABLE;
                        }
                    } else if (!can_grant && timeout == 0) {
                        /* 15. If not, and nowait specified, return failure. */
                        throw new LockTimeoutException(new MessageInstance(
                                m_WC0004, r, this));
                    } else {
                        status = LockStatus.DOWAITCONVERT;
                    }
                }
            } else {
//                exceptionHandler.errorThrow(this.getClass(),
//                        "handleConversionRequest", new LockException(
//                                new MessageInstance(m_EC0005)));
                throw new LockException(
                        new MessageInstance(m_EC0005));
            }
            return status;
        }

        final void prepareToWait(LockRequest r, LockMode m, LockDuration d, boolean converting) {
            waiting = true;
            if (!converting) {
                r.status = LockRequestStatus.WAITING;
            } else {
                r.convertMode = m;
                r.convertDuration = d;
                r.status = LockRequestStatus.CONVERTING;
            }
            r.thread = Thread.currentThread();

        }

        final void grantWaiters(final ConcurrentLinkedQueue<Thread> wakeupQ) {
            /*
        * Recalculate granted mode by calculating max mode amongst all
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
            grantedMode = LockMode.NONE;
            waiting = false;
            boolean converting = false;
            for (LockRequest r = head; r != null; r = r.next) {
                if (r.status == LockRequestStatus.GRANTED) {
                    grantedMode = r.mode.maximumOf(grantedMode);
                } else if (r.status == LockRequestStatus.CONVERTING) {
                    boolean can_grant = checkCompatible(r, r.convertMode);
                    if (can_grant) {
//                if (debugEnabled) {
//                    log
//                            .debug(
//                                    getClass(),
//                                    "release",
//                                    "SIMPLEDBM-DEBUG: Granting conversion request "
//                                            + r
//                                            + " because request is compatible with "
//                                            + lockitem);
//                }
                        if (r.convertDuration == LockDuration.INSTANT_DURATION) {
                            /*
                            * If the request is for an instant duration lock then
                            * don't perform the conversion.
                            */
                            r.convertMode = r.mode;
                        } else {
                            r.mode = r.convertMode.maximumOf(r.mode);
                            r.convertMode = r.mode;
                            if (r.convertDuration == LockDuration.COMMIT_DURATION && r.duration == LockDuration.MANUAL_DURATION) {
                                r.duration = LockDuration.COMMIT_DURATION;
                            }
                            grantedMode = r.mode.maximumOf(grantedMode);
                        }
                        /*
                       * Treat conversions as lock recursion.
                       */
                        r.count++;
                        r.status = LockRequestStatus.GRANTED;
                        wakeupQ.add(r.thread);
                    } else {
                        grantedMode = r.mode.maximumOf(grantedMode);
                        converting = true;
                        waiting = true;
                    }
                } else if (r.status == LockRequestStatus.WAITING) {
                    if (!converting && r.mode.isCompatible(grantedMode)) {
                        r.status = LockRequestStatus.GRANTED;
                        r.count = 1;
                        grantedMode = r.mode.maximumOf(grantedMode);
                        wakeupQ.add(r.thread);
                    } else {
                        waiting = true;
                        break;
                    }
                }
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            return appendTo(sb).toString();
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append(NEW_LINE).append("LockItem(").append(NEW_LINE).append(TAB)
                    .append("target=");
            if (target instanceof Dumpable) {
                ((Dumpable) target).appendTo(sb);
            } else {
                sb.append(target);
            }
            sb.append(NEW_LINE).append(TAB).append("granted mode=").append(
                    grantedMode).append(NEW_LINE);
            sb.append(TAB).append("queue={").append(NEW_LINE);
            for (LockRequest req = head; req != null; req = req.next) {
                sb.append(TAB).append(TAB).append("LockRequest(").append(
                        NEW_LINE);
                sb.append(TAB).append(TAB).append(TAB).append("status=")
                        .append(req.status).append(" mode=").append(req.mode)
                        .append(NEW_LINE);
                sb.append(TAB).append(TAB).append(TAB).append("convertMode=")
                        .append(req.convertMode).append(" convertDuration=")
                        .append(req.convertDuration).append(NEW_LINE);
                sb.append(TAB).append(TAB).append(TAB).append("owner=");
                if (req.owner instanceof Dumpable) {
                    ((Dumpable) req.owner).appendTo(sb);
                } else {
                    sb.append(req.owner);
                }
                sb.append(NEW_LINE);
                sb.append(TAB).append(TAB).append(TAB).append("thread=")
                        .append(req.thread).append(NEW_LINE);
                sb.append(TAB).append(TAB).append(")").append(NEW_LINE);
            }
            sb.append(TAB).append("}").append(NEW_LINE);
            return sb;
        }
    }



    /**
     * Holds information regarding a lock wait. Purpose is to enable deadlock
     * detection.
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

    static final class DeadlockDetector implements Runnable {

        final LockManagerImpl lockManager;

        public DeadlockDetector(LockManagerImpl lockManager) {
            this.lockManager = lockManager;
        }

        public void run() {
            if (lockManager.stop) {
                return;
            }
            lockManager.detectDeadlocks();
        }

    }

    public void setDeadlockDetectorInterval(int seconds) {
        this.deadlockDetectorInterval = seconds;
    }
}
