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
 * LockHandle provides a handle to an acquired lock. It provides methods for
 * releasing the lock.
 * 
 * @author Dibyendu Majumdar
 * @since 26-July-2005
 * @see LockManager
 */
public interface LockHandle {
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
     * 
     * @param force Forcibly release the lock regardless of lock count.
     * @throws LockException Thrown if the lock doesn't exist
     * @return True if lock was released, false if lock is still held.
     */
    boolean release(boolean force);

    /**
     * Downgrades a lock to the desired mode. Downgrading a lock may result in
     * other compatible locks being granted. For example, if an
     * {@link LockMode#UPDATE} lock is downgraded to {@link LockMode#SHARED}, it
     * may result in pending shared lock requests being granted.
     * 
     * @param mode
     * @throws LockException
     */
    void downgrade(LockMode mode);

    /**
     * Returns the currently held LockMode.
     * 
     * @return Currently held LockMode.
     */
    LockMode getCurrentMode();
}
