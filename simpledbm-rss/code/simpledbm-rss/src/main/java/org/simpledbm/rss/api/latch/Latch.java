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
 *    Linking this library statically or dynamically with other modules 
 *    is making a combined work based on this library. Thus, the terms and
 *    conditions of the GNU General Public License cover the whole
 *    combination.
 *    
 *    As a special exception, the copyright holders of this library give 
 *    you permission to link this library with independent modules to 
 *    produce an executable, regardless of the license terms of these 
 *    independent modules, and to copy and distribute the resulting 
 *    executable under terms of your choice, provided that you also meet, 
 *    for each linked independent module, the terms and conditions of the 
 *    license of that module.  An independent module is a module which 
 *    is not derived from or based on this library.  If you modify this 
 *    library, you may extend this exception to your version of the 
 *    library, but you are not obligated to do so.  If you do not wish 
 *    to do so, delete this exception statement from your version.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.rss.api.latch;

/**
 * A Latch is an efficient lock that is used by the system to manage concurrent
 * access to physical structures. As explained by C. Mohan, latches are used to
 * ensure physical consistency of data. In some ways, a Latch is similar to a
 * Mutex in purpose, except that a latch supports additional lock modes, such as
 * Shared locks and Update locks.
 * <p>
 * An exclusive latch is incompatible with any other latch. Update latches are
 * compatible with Shared latches, but incompatible with other latch modes. Note
 * that Shared latches are incompatible with Update latches. The behavior of
 * Update latch is deliberately asymmetric to help avoid deadlocks. See p. 409,
 * section 7.8.2 in Transaction Processing: Concepts and Techniques, for a
 * discussion of why this is so.
 * <p>
 * An Update latch can be upgraded to an Exclusive latch. An Exclusive latch can
 * be downgraded to an Update latch, and an Update latch can be downgraded to a
 * Shared latch.
 * <p>
 * The Latch interface is designed to be compatible with the Java 5.0
 * ReentrantReadWriteLock interface. This allows an implementation based upon
 * the Java primitive. Such an implementation however will not support the
 * Update mode, and may throw UnsupportedOperationException on all functions
 * related to Update modes.
 * 
 * @author Dibyendu Majumdar
 * @since 23-Aug-2005
 */
public interface Latch {

    /**
     * Conditional request for exclusive latch.
     * 
     * @return false if latch cannot be acquired.
     */
    public boolean tryExclusiveLock();

    /**
     * Unconditional request for exclusive latch.
     */
    public void exclusiveLock();

    /**
     * Unconditional request for exclusive latch. Allows interruptions.
     */
    public void exclusiveLockInterruptibly() throws InterruptedException;

    /**
     * Unlock an exclusive lock request.
     */
    public void unlockExclusive();

    /**
     * Conditional request for Update latch.
     * 
     * @return false if latch cannot be acquired.
     */
    public boolean tryUpdateLock();

    /**
     * Unconditional request for Update latch.
     */
    public void updateLock();

    /**
     * Unconditional request for Update latch. Allows interruptions.
     */
    public void updateLockInterruptibly() throws InterruptedException;

    /**
     * Unlock an update lock.
     */
    public void unlockUpdate();

    /**
     * Conditional request for Shared latch.
     * 
     * @return false if latch cannot be acquired.
     */
    public boolean trySharedLock();

    /**
     * Unconditional request for Shared latch.
     */
    public void sharedLock();

    /**
     * Unconditional request for Shared latch. Allows interruptions.
     */
    public void sharedLockInterruptibly() throws InterruptedException;

    /**
     * Unlock shared lock.
     */
    public void unlockShared();

    /**
     * Conditional attempt to upgrade an Update latch to Exclusive latch.
     * 
     * @return false if latch cannot be upgraded.
     */
    public boolean tryUpgradeUpdateLock();

    /**
     * Unconditional attempt to upgrade an Update latch to Exclusive latch.
     */
    public void upgradeUpdateLock();

    /**
     * Unconditional attempt to upgrade an Update latch to Exclusive latch.
     * Allows interruptions.
     */
    public void upgradeUpdateLockInterruptibly() throws InterruptedException;

    /**
     * Downgrade Exclusive latch to Update latch.
     */
    public void downgradeExclusiveLock();

    /**
     * Downgrade Update latch to Shared latch.
     */
    public void downgradeUpdateLock();

    public boolean isLatchedExclusively();

    public boolean isLatchedForUpdate();
}