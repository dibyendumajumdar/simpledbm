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
package org.simpledbm.rss.api.locking;

import org.simpledbm.common.api.locking.LockMode;

/**
 * LockMgr is the primary interface of acquiring locks. Typical usage is shown
 * below:
 * <p>
 * 
 * <pre>
 * {
 *     &#064;code
 *     LockMgrFactory factory = new LockMgrFactoryImpl();
 *     Properties props = new Properties();
 *     LockMgr lockmgr = factory.create(props);
 *     LockHandle handle = lockmgr.acquire(transaction, row, LockMode.SHARED,
 *             LockDuration.MANUAL_DURATION, 60);
 *     handle.release(false);
 * }
 * </pre>
 * 
 * @author Dibyendu Majumdar
 * @since 26-July-2005
 */
public interface LockManager {

    public final String LOGGER_NAME = "org.simpledbm.lockmgr";

    /**
     * Acquires a lock on an object in the specified mode.
     * <p>
     * A lock may be acquired in different modes such as Shared, Exclusive,
     * Intention Shared, Intention Exclusive, Shared Intention Exclusive, and
     * Update. The lock mode determines which locks are compatible; multiple
     * transactions can hold locks on an object simultaneously only if those
     * locks are compatible. For example, Shared locks are compatible with each
     * other, but an Exclusive lock is not compatible with any other lock mode.
     * These rules mean that multiple transactions can hold a Shared lock on an
     * object, but only one transaction can obtain an Exclusive lock on the
     * object.
     * <p>
     * Locks are acquired for a duration. If a lock is requested for
     * {@link LockDuration#INSTANT_DURATION}, then the caller is delayed until
     * the lock becomes grantable, but the lock is not actually granted.
     * {@link LockDuration#MANUAL_DURATION} locks are released either when the
     * lock has been released as many times as it was acquired, or when the
     * transaction commits. {@link LockDuration#COMMIT_DURATION} locks are held
     * until the transaction commits. Note that once a lock request is made with
     * COMMIT_DURATION, the lock is not released until the transaction commits,
     * regardless of whether it was originally or subsequently requested for
     * other durations.
     * <p>
     * The transaction that requested a lock is said to be the owner of the
     * lock. The owner parameter is intentionally an opaque type, to reduce
     * dependency between the Locking module and the Transaction module. Lock
     * owner objects must implement the {@link Object#equals(Object) equals}
     * method.
     * <p>
     * The object being locked is also opaque to the Locking sub-system.
     * Lockable objects must implement the {@link Object#equals equals} and
     * {@link Object#hashCode() hashCode} methods.
     * <p>
     * The locking system supports lock conversions, whereby if the owner
     * already holds a lock on the object in a particular mode, and subsequently
     * requests another lock on the object in a different mode, the existing
     * lock is upgraded to the more restrictive of the two, as defined by the
     * lock conversion matrix in {@link LockMode}. A lock conversion request may
     * also impact the lock duration, if COMMIT_DURATION was specified.
     * <p>
     * A timeout can be specified. If the value of timeout is -1, the Locking
     * system will wait indefinitely for the lock. This is also known as an
     * unconditional request. If timeout is 0, the Locking system will not wait
     * at all, and return failure if the lock is not immediately grantable. If
     * timeout is &gt; 0, the Locking system will wait for the specified amount of
     * time (in seconds) before giving up the attempt to acquire the lock. In
     * all cases, if a lock cannot be acquired, an exception will be thrown.
     * Timeouts are indicated by throwing a {@link LockTimeoutException}.
     * 
     * @param owner Prospective owner of the lock, must implement equals()
     *            method.
     * @param lockable The object that is to be locked, must implement equals()
     *            and hashcode() methods.
     * @param mode The desired lock mode.
     * @param duration The duration for which the lock will be held.
     * @param timeout Either -1, 0, or &gt; 0 value indicating wait forever,
     *            nowait, wait specified number of seconds.
     * @return A LockHandle that can be used to further manipulate the lock.
     * @throws LockException Thrown if the lock could not be acquired.
     */
    boolean acquire(Object owner, Object lockable, LockMode mode,
            LockDuration duration, int timeout);

    /**
     * Downgrades a lock to the desired mode. Downgrading a lock may result in
     * other compatible locks being granted. For example, if an
     * {@link LockMode#UPDATE} lock is downgraded to {@link LockMode#SHARED}, it
     * may result in pending shared lock requests being granted.
     */
    boolean downgrade(Object owner, Object lockable, LockMode downgradeTo);

    /**
     * Releases a lock; if force is true the lock is released unconditionally,
     * regardless of the duration of the lock. MANUAL_DURATION locks are
     * reentrant, therefore the Lock Manager must keep track of the number of
     * times such a lock is acquired. A call to release causes the lock
     * reference count to be decremented until it is zero, when the lock can be
     * actually deleted.
     * <p>
     * If force option is set to true, the lock is released unconditionally,
     * regardless of the lock duration or reference count. The force option is
     * meant to be used only when a transaction commits, or rolls back
     * (including to a savepoint).
     */
    boolean release(Object owner, Object lockable, boolean force);

    /**
     * Searches for a specified lock, held by a specified owner.
     * 
     * @param owner Owner of the lock, must implement equals() method.
     * @param lockable The object that is tested, must implement equals() and
     *            hashcode() methods.
     * @return {@link LockMode} The mode in which lock is held, or
     *         {@link LockMode#NONE} if lock is not held.
     */
    LockMode findLock(Object owner, Object lockable);

    /**
     * Searches for and returns all locks owned by specified owner.
     * 
     * @param owner Owner of the lock, must implement equals() method.
     * @param mode The lock mode or null if all locks are required.
     */
    Object[] getLocks(Object owner, LockMode mode);

    /**
     * Starts the Lock Manager instance.
     */
    void start();

    /**
     * Shuts down the Lock Manager instance.
     */
    void shutdown();

    /**
     * Sets the time interval between deadlock detections.
     * 
     * @param seconds Interval in seconds
     */
    void setDeadlockDetectorInterval(int seconds);
}
