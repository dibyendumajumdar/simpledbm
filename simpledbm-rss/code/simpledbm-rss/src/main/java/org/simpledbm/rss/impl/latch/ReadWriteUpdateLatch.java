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

import java.util.concurrent.locks.LockSupport;

import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.rss.api.latch.Latch;
import org.simpledbm.rss.api.latch.LatchException;

/**
 * Implements a Latch that supports three lock modes, Shared, Update and
 * Exclusive.
 * 
 * @author Dibyendu Majumdar
 * @since 22 Aug, 2005
 */
public final class ReadWriteUpdateLatch implements Latch {

    /*
     * Implementation issue:
     * At present, a recursive request for shared or update
     * lock will deadlock if there are pending wait or upgrade
     * requests. 
     */

    static final int SHARED = 0;

    static final int EXCLUSIVE = 1;

    static final int UPDATE = 2;

    static final int FREE = -1;

    PlatformObjects po;

    static final class Link {
        final Thread thread;

        final int requestedMode;

        volatile Link next = null;

        Link(Thread thread, int mode) {
            this.thread = thread;
            this.requestedMode = mode;
        }
    }

    private volatile Thread owner = null;

    private volatile int grantedMode = FREE;

    private volatile Link waitQueueHead = null;

    private volatile Link waitQueueTail = null;

    private volatile Thread pendingUpgrade = null;

    private volatile int sharedCount = 0;

    private volatile int exclusiveCount = 0;

    public ReadWriteUpdateLatch(PlatformObjects po) {
        this.po = po;
    }

    /**
     * Adds a waiter to the end of the wait queue.
     */
    private void appendWaiters(Link link) {
        if (waitQueueHead == null) {
            waitQueueHead = link;
        }
        if (waitQueueTail != null) {
            waitQueueTail.next = link;
        }
        waitQueueTail = link;
    }

    /**
     * Removes a waiter from the queue - used primarily to cleanup upon lock
     * failures.
     */
    private synchronized void cleanup(Link link) {
        Link next = waitQueueHead;
        Link prev = null;
        while (next != link && next != null) {
            prev = next;
            next = next.next;
        }
        if (next == link) {
            if (prev != null) {
                prev.next = next.next;
            }
            if (waitQueueHead == link) {
                waitQueueHead = link.next;
            }
            if (waitQueueTail == link) {
                waitQueueTail = prev;
            }
        }
    }

    /**
     * Workhorse for acquiring latch in exclusive mode. Supports recursive
     * requests for exclusive access. Exclusive access is not compatible with
     * any other type of access.
     */
    private boolean doExclusiveLock(boolean wait, boolean allowInterrupts) {
        Link link = null;
        Thread me = Thread.currentThread();
        synchronized (this) {
            if (grantedMode == EXCLUSIVE && owner == me) {
                exclusiveCount++;
                return true;
            }
            if (grantedMode == FREE) {
                owner = me;
                grantedMode = EXCLUSIVE;
                exclusiveCount = 1;
                return true;
            }
            if (!wait) {
                return false;
            }
            link = new Link(me, EXCLUSIVE);
            appendWaiters(link);
        }
        /*
         * Following is needed because the LockSupport.park()
         * interface allows spurious wakeups.
         */
        for (;;) {
            LockSupport.park();
            if (owner == me && grantedMode == EXCLUSIVE) {
                break;
            }
            if (allowInterrupts && me.isInterrupted()) {
                cleanup(link);
                return false;
            }
        }
        return true;
    }

    /* (non-Javadoc)
     * @see org.simpledbm.latch.impl.Latch#tryExclusiveLock()
     */
    public boolean tryExclusiveLock() {
        return doExclusiveLock(false, false);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.latch.impl.Latch#exclusiveLock()
     */
    public void exclusiveLock() {
        doExclusiveLock(true, false);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.latch.Latch#exclusiveLockInterruptibly()
     */
    public void exclusiveLockInterruptibly() throws InterruptedException {
        if (!doExclusiveLock(true, true)) {
            throw new InterruptedException();
        }
    }

    /**
     * Workhorse for acquiring latch in Update mode. Note that Update mode
     * latches are asymmetrical. An Update mode latch is compatible with Shared
     * requests, but Shared latches are not compatible with Update latches.
     * Also, an Update latch is incompatible with other Update or Exclusive
     * requests.
     * <p>
     * If requestor already holds the latch for Update or Exclusively, then the
     * recursion count in incremented.
     */
    private boolean doUpdateLock(boolean wait, boolean allowInterrupts) {
        Link link = null;
        Thread me = Thread.currentThread();
        synchronized (this) {
            if ((grantedMode == UPDATE || grantedMode == EXCLUSIVE)
                    && owner == me) {
                exclusiveCount++;
                return true;
            }
            if (grantedMode == FREE
                    || (grantedMode == SHARED && waitQueueHead == null)) {
                owner = me;
                grantedMode = UPDATE;
                exclusiveCount = 1;
                return true;
            }
            if (!wait) {
                return false;
            }
            link = new Link(me, UPDATE);
            appendWaiters(link);
        }
        /*
         * Following is needed because the LockSupport.park()
         * interface allows spurious wakeups.
         */
        for (;;) {
            LockSupport.park();
            if (owner == me && grantedMode == UPDATE) {
                break;
            }
            if (allowInterrupts && me.isInterrupted()) {
                cleanup(link);
                return false;
            }
        }
        return true;
    }

    /* (non-Javadoc)
     * @see org.simpledbm.latch.impl.Latch#tryUpdateLock()
     */
    public boolean tryUpdateLock() {
        return doUpdateLock(false, false);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.latch.impl.Latch#updateLock()
     */
    public void updateLock() {
        doUpdateLock(true, false);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.latch.Latch#updateLockInterruptibly()
     */
    public void updateLockInterruptibly() throws InterruptedException {
        if (!doUpdateLock(true, true)) {
            throw new InterruptedException();
        }
    }

    /**
     * Workhorse for acquiring latch in shared mode. Shared mode is compatible
     * with Shared mode but not with Update or Exclusive. Also, we do not grant
     * shared requests if there are pending upgrade/exclusive requests.
     */
    private boolean doSharedLock(boolean wait, boolean allowInterrupts) {
        Link link = null;
        Thread me = Thread.currentThread();
        synchronized (this) {
            if (grantedMode == FREE) {
                assert exclusiveCount == 0;
                sharedCount = 1;
                owner = null;
                grantedMode = SHARED;
                return true;
            }
            if ((grantedMode == UPDATE || grantedMode == EXCLUSIVE)
                    && owner == me) {
                exclusiveCount++;
                return true;
            }
            /*
             * Update locks should be asymmetric according to Jim Gray - see Transaction Processing,
             * page 409, section 7.8.2.
             */
            // if ((grantedMode == SHARED || grantedMode == UPDATE) && waitQueueHead == null && pendingUpgrade == null) {
            if (grantedMode == SHARED && waitQueueHead == null
                    && pendingUpgrade == null) {
                sharedCount++;
                return true;
            }
            if (!wait) {
                return false;
            }
            link = new Link(me, SHARED);
            appendWaiters(link);
        }
        /*
         * Following is needed because the LockSupport.park()
         * interface allows spurious wakeups. Since we do not
         * keep track of the granted shared requests, the only way to
         * check is to walk the wait queue list.
         */
        for (;;) {
            LockSupport.park();
            if (sharedCount > 0) {
                Link next = waitQueueHead;
                while (next != null && next != link) {
                    next = next.next;
                }
                if (next == null) {
                    break;
                }
            }
            if (allowInterrupts && me.isInterrupted()) {
                cleanup(link);
                return false;
            }
        }
        return true;
    }

    /* (non-Javadoc)
     * @see org.simpledbm.latch.impl.Latch#sharedLock()
     */
    public void sharedLock() {
        doSharedLock(true, false);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.latch.Latch#sharedLockInterruptibly()
     */
    public void sharedLockInterruptibly() throws InterruptedException {
        if (!doSharedLock(true, true)) {
            throw new InterruptedException();
        }
    }

    /* (non-Javadoc)
     * @see org.simpledbm.latch.impl.Latch#trySharedLock()
     */
    public boolean trySharedLock() {
        return doSharedLock(false, false);
    }

    /**
     * Requests an update lock to be upgraded to exclusive. Does not affect
     * recursion count.
     */
    private boolean doUpgradeUpdateLock(boolean wait, boolean allowInterrupts) {
        Thread me = Thread.currentThread();
        synchronized (this) {
            assert grantedMode == UPDATE;
            assert owner == me;

            if (sharedCount == 0) {
                owner = me;
                grantedMode = EXCLUSIVE;
                return true;
            }
            if (!wait) {
                return false;
            }
            pendingUpgrade = me;
        }
        /*
         * Following is needed because the LockSupport.park()
         * interface allows spurious wakeups.
         */
        for (;;) {
            LockSupport.park();
            if (owner == me && grantedMode == EXCLUSIVE) {
                break;
            }
            if (allowInterrupts && me.isInterrupted()) {
                pendingUpgrade = null;
                return false;
            }
        }
        return true;
    }

    /* (non-Javadoc)
     * @see org.simpledbm.latch.impl.Latch#upgrade()
     */
    public void upgradeUpdateLock() {
        doUpgradeUpdateLock(true, false);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.latch.Latch#upgradeUpdateLockInterruptibly()
     */
    public void upgradeUpdateLockInterruptibly() throws InterruptedException {
        if (!doUpgradeUpdateLock(true, true)) {
            throw new InterruptedException();
        }
    }

    /* (non-Javadoc)
     * @see org.simpledbm.latch.impl.Latch#tryUpgrade()
     */
    public boolean tryUpgradeUpdateLock() {
        return doUpgradeUpdateLock(false, false);
    }

    /**
     * Performs grants to waiting threads after some thread has released the
     * latch.
     */
    private void performGrants() {
        if (pendingUpgrade != null) {
            if (owner == pendingUpgrade && sharedCount == 0
                    && grantedMode == UPDATE) {
                grantedMode = EXCLUSIVE;
                pendingUpgrade = null;
                LockSupport.unpark(owner);
            }
        } else {
            while (waitQueueHead != null) {
                Link candidate = null;
                if (waitQueueHead.requestedMode == SHARED) {
                    if (grantedMode == FREE) {
                        grantedMode = SHARED;
                    }
                    /*
                     * Update locks should be asymmetric according to Jim Gray - see Transaction Processing,
                     * page 409, section 7.8.2.
                     */
                    // if (grantedMode == SHARED || grantedMode == UPDATE) {
                    if (grantedMode == SHARED) {
                        sharedCount++;
                        candidate = waitQueueHead;
                    } else {
                        break;
                    }
                } else if (waitQueueHead.requestedMode == UPDATE) {
                    if (grantedMode == SHARED || grantedMode == FREE) {
                        grantedMode = UPDATE;
                        owner = waitQueueHead.thread;
                        exclusiveCount = 1;
                        candidate = waitQueueHead;
                    } else {
                        break;
                    }
                } else if (waitQueueHead.requestedMode == EXCLUSIVE) {
                    if (grantedMode == FREE) {
                        grantedMode = EXCLUSIVE;
                        owner = waitQueueHead.thread;
                        exclusiveCount = 1;
                        candidate = waitQueueHead;
                    } else {
                        break;
                    }
                } else {
                    assert false;
                    break;
                }
                waitQueueHead = waitQueueHead.next;
                if (candidate != null) {
                    LockSupport.unpark(candidate.thread);
                }
            }
            if (waitQueueHead == null) {
                waitQueueTail = null;
            }
        }
    }

    /**
     * Workhorse for releasing latches. Grants requests that become eligible
     * after the latch is released.
     */
    public synchronized void unlock() {
        boolean grant = false;
        if (grantedMode == SHARED) {
            sharedCount--;
            assert sharedCount >= 0;
            if (sharedCount == 0) {
                grantedMode = FREE;
                grant = true;
            }
        } else if (grantedMode == EXCLUSIVE) {
            if (owner != Thread.currentThread()) {
                po.getExceptionHandler().errorThrow(
                        this.getClass(),
                        "unlock",
                        new LatchException(new MessageInstance(
                                LatchFactoryImpl.m_EH0007, this)));
            }
            exclusiveCount--;
            assert exclusiveCount >= 0;
            if (exclusiveCount == 0) {
                grantedMode = FREE;
                owner = null;
                grant = true;
            }
        } else if (grantedMode == UPDATE) {
            if (owner == Thread.currentThread()) {
                exclusiveCount--;
                if (exclusiveCount == 0) {
                    if (sharedCount > 0) {
                        grantedMode = SHARED;
                    } else {
                        grantedMode = FREE;
                    }
                    owner = null;
                    grant = true;
                }
            } else {
                sharedCount--;
                assert sharedCount >= 0;
                if (sharedCount == 0) {
                    grant = true;
                }
            }
        }

        if (grant) {
            performGrants();
        }
    }

    /* (non-Javadoc)
     * @see org.simpledbm.latch.Latch#unlockShared()
     */
    public void unlockShared() {
        unlock();
    }

    /* (non-Javadoc)
     * @see org.simpledbm.latch.Latch#unlockExclusive()
     */
    public void unlockExclusive() {
        unlock();
    }

    /* (non-Javadoc)
     * @see org.simpledbm.latch.Latch#unlockUpdate()
     */
    public void unlockUpdate() {
        unlock();
    }

    /* (non-Javadoc)
     * @see org.simpledbm.latch.impl.Latch#downgradeExclusiveLock()
     */
    public synchronized void downgradeExclusiveLock() {
        if (owner == Thread.currentThread() && grantedMode == EXCLUSIVE) {
            grantedMode = UPDATE;
            performGrants();
        } else {
            po.getExceptionHandler().errorThrow(
                    this.getClass(),
                    "unlock",
                    new LatchException(new MessageInstance(
                            LatchFactoryImpl.m_EH0009, grantedMode, UPDATE)));
        }
    }

    /* (non-Javadoc)
     * @see org.simpledbm.latch.impl.Latch#downgradeUpdateLock()
     */
    public synchronized void downgradeUpdateLock() {
        if (owner == Thread.currentThread() && grantedMode == UPDATE
                && exclusiveCount == 1) {
            grantedMode = SHARED;
            sharedCount++;
            owner = null;
            exclusiveCount = 0;
            performGrants();
        } else {
            po.getExceptionHandler().errorThrow(
                    this.getClass(),
                    "unlock",
                    new LatchException(new MessageInstance(
                            LatchFactoryImpl.m_EH0009, grantedMode, SHARED)));
        }
    }

    public synchronized final int getMode() {
        return grantedMode;
    }

    public synchronized final int getSharedCount() {
        return sharedCount;
    }

    public synchronized final int getExclusiveCount() {
        return exclusiveCount;
    }

    public synchronized final Thread getOwner() {
        return owner;
    }

    public synchronized boolean isLatchedExclusively() {
        return grantedMode == EXCLUSIVE;
    }

    public synchronized boolean isLatchedForUpdate() {
        return grantedMode == UPDATE;
    }
}
