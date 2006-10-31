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
 *    Email  : dibyendu@mazumdar.demon.co.uk
 */
package org.simpledbm.rss.api.locking;

/**
 * LockHandle provides a handle to an acquired lock. It provides methods for releasing the lock.
 * @author Dibyendu Majumdar
 * @since 26-July-2005
 * @see LockManager
 */
public interface LockHandle {
	/**
	 * Releases a lock; if force is true the lock is released unconditionally. Since locks are
	 * reentrant, the system keeps track of the number of times a lock is acquired.
	 * A call to release causes the lock count to be decremented until it is zero, when the
	 * lock is actually deleted. If force option is true, the lock is released even if the
	 * count is greater than zero. When a transaction commits, it should set the force
	 * option to true when releasing locks.
	 * 
	 * @param force Forcibly release the lock regardless of lock count.
	 * @throws LockException Thrown if the lock doesn't exist
     * @return True if lock was released, false if lock is still held.
	 */
	boolean release(boolean force) throws LockException;
	
	/**
	 * Downgrades a lock to the desired mode. Downgrading a lock may result in other
	 * compatible locks being granted. For example, if an {@link LockMode#UPDATE}
	 * lock is downgraded to {@link LockMode#SHARED}, it may result in pending shared
	 * lock requests being granted.
	 * @param mode
	 * @throws LockException
	 */
    void downgrade(LockMode mode) throws LockException;
    
    /**
     * If the current holder of the lock already held the lock, then
     * this method returns the previous LockMode.
     * @return The previously held LockMode.
     */
    LockMode getPreviousMode();
    
    /**
     * Determines if this lock is also held by other transactions.
     * @return True if the lock is also held by others, otherwise false.
     */
    boolean isHeldByOthers();
    
    /**
     * Returns the currently held LockMode.
     * @return Currently held LockMode.
     */
    LockMode getCurrentMode();    
}
