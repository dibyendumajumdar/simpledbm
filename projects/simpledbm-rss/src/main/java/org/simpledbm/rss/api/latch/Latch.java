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