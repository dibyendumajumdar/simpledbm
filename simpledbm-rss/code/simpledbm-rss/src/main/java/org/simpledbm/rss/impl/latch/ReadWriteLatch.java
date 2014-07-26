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

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.rss.api.latch.Latch;

/**
 * Implements a Latch that supports two lock modes: Shared and Exclusive. This
 * implementation is based upon standard Java ReentrantReadWriteLock. Update
 * mode locks are implemented as exclusive locks.
 */
public final class ReadWriteLatch implements Latch {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReadLock rlock = lock.readLock();
    private final WriteLock wlock = lock.writeLock();
    PlatformObjects po;

    public ReadWriteLatch(PlatformObjects po) {
        this.po = po;
    }

    public boolean tryExclusiveLock() {
        return wlock.tryLock();
    }

    public void exclusiveLock() {
        wlock.lock();
    }

    public void exclusiveLockInterruptibly() throws InterruptedException {
        wlock.lockInterruptibly();
    }

    public void unlockExclusive() {
        wlock.unlock();
    }

    public boolean tryUpdateLock() {
        return wlock.tryLock();
    }

    public void updateLock() {
        wlock.lock();
    }

    public void updateLockInterruptibly() throws InterruptedException {
        wlock.lockInterruptibly();
    }

    public void unlockUpdate() {
        wlock.unlock();
    }

    public boolean trySharedLock() {
        return rlock.tryLock();
    }

    public void sharedLock() {
        rlock.lock();
    }

    public void sharedLockInterruptibly() throws InterruptedException {
        rlock.lockInterruptibly();
    }

    public void unlockShared() {
        rlock.unlock();
    }

    public boolean tryUpgradeUpdateLock() {
        assert lock.isWriteLocked();
        return true;
    }

    public void upgradeUpdateLock() {
        assert lock.isWriteLocked();
    }

    public void upgradeUpdateLockInterruptibly() {
        assert lock.isWriteLocked();
    }

    public void downgradeExclusiveLock() {
        assert lock.isWriteLocked();
    }

    public void downgradeUpdateLock() {
        rlock.lock();
        wlock.unlock();
    }

    public boolean isLatchedExclusively() {
        return lock.isWriteLocked();
    }

    public boolean isLatchedForUpdate() {
        return lock.isWriteLocked();
    }
}
