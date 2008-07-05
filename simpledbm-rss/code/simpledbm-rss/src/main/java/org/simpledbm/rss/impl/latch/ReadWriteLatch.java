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
 *    Email  : d dot majumdar at gmail dot com ignore
 */

package org.simpledbm.rss.impl.latch;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.simpledbm.rss.api.latch.Latch;

/**
 * Implements a Latch that supports two lock modes: Shared and Exclusive. This
 * implementation is based upon standard Java ReentrantReadWriteLock. Update mode locks
 * are implemented as exclusive locks.
 */
public final class ReadWriteLatch implements Latch {

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReadLock rlock = lock.readLock();
    private final WriteLock wlock = lock.writeLock();

    public ReadWriteLatch() {
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
