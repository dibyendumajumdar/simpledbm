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
