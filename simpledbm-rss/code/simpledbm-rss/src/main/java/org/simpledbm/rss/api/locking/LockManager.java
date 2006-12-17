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
 * LockMgr is the primary interface of acquiring locks. Typical usage is
 * shown below:
 * <p>
 * <pre>
 * LockMgrFactory factory = new LockMgrFactoryImpl(); 
 * Properties props = new Properties();
 * LockMgr lockmgr = factory.create(props);
 * LockHandle handle = lockmgr.acquire(transaction, row, 
 *  LockMode.SHARED, LockDuration.MANUAL_DURATION, 60);
 * handle.release(false);
 * </pre>
 * </p>
 *
 * @author Dibyendu Majumdar
 * @since 26-July-2005
 */
public interface LockManager {
	
	/**
	 * Acquires a lock on an object in the specified mode. 
	 * <p>
	 * A lock may be acquired in different modes such as Shared, Exclusive,
	 * Intention Shared, Intention Exclusive, Shared Intention Exclusive, and Update.
	 * The lock mode determines which locks are compatible; multiple transactions
	 * can hold locks on an object simultaneously only if those locks are compatible.
	 * For example, Shared locks are compatible with each other, but an Exclusive 
	 * lock is not compatible with any other lock mode.
	 * These rules mean that multiple transactons can hold a Shared lock on an
	 * object, but only one transaction can obtain an Exclusive lock on the object.
	 * <p>
	 * Locks are acquired for a duration. If a lock is requested for
	 * {@link LockDuration#INSTANT_DURATION}, then the caller is delayed until the lock 
	 * becomes grantable, but the lock is not actually granted. {@link LockDuration#MANUAL_DURATION} locks are
	 * released either when the lock has been released as many times as it was acquired,
	 * or when the transaction commits. {@link LockDuration#COMMIT_DURATION} locks are 
	 * held until the transaction commits.
	 * <p>The transaction that requested a lock
	 * is said to be the owner of the lock. The owner parameter is intentionally an opaque type, to reduce
	 * dependency between the Locking module and the Transaction module. 
	 * <p>The object being locked is also opaque to the Locking sub-system.
	 * Lockable objects must implement the {@link Object#equals} and {@link Object#hashCode()} methods, and transaction owner objects
	 * must implement the {@link Object#equals(Object)} method.
	 * <p>The locking system supports lock conversions,
	 * whereby if the owner already holds a lock on the object in a particular mode,
	 * and subsequently requests another lock on the object in a different mode, the existing lock is 
	 * upgraded to the more restrictive of the two, as defined by the lock 
	 * conversion matrix in {@link LockMode}.  
	 * <p>A timeout can be specified. If the value of timeout is -1, the Locking system
	 * will wait indefinitely for the lock. This is also known as an unconditional request.
	 * If timeout is 0, the Locking system will not
	 * wait at all, and return failure if the lock is not immediately grantable. 
	 * If timeout is > 0, the Locking system will wait for the specified amount of time
	 * (in seconds) before giving up the attempt to acquire the lock.
	 * In all cases, if a lock cannot be acquired, an exception will be thrown.
	 * Timeouts are indicated by throwing a {@link LockTimeoutException}.
	 * 
	 * @param owner Prospective owner of the lock, must implement equals() method.
	 * @param lockable The object that is to be locked, must implement equals() and hashcode() methods.
	 * @param mode The desired lock mode.
	 * @param duration The duration for which the lock will be held.
	 * @param timeout Either -1, 0, or > 0 value indicating wait forever, nowait, wait specified
	 * number of seconds.
	 * @return A LockHandle that can be used to further manipulate the lock.
	 * @throws LockException Thrown if the lock could not be acquired.
	 */
	LockHandle acquire(Object owner,
			Object lockable, LockMode mode, LockDuration duration,
			int timeout, LockInfo lockInfo) throws LockException;

	boolean downgrade(Object owner, Object lockable, LockMode downgradeTo) throws LockException;
	
	boolean release(Object owner, Object lockable, boolean force) throws LockException;
	
	/**
	 * Searches for a specified lock, held by a specified owner.
	 * @param owner Owner of the lock, must implement equals() method.
	 * @param lockable The object that is tested, must implement equals() and hashcode() methods.
	 * @return {@link LockMode} The mode in which lock is held, or {@link LockMode#NONE} if lock is not held.
	 */
	LockMode findLock(Object owner, Object lockable);	
}
