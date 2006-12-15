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
package org.simpledbm.rss.api.tx;

import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockMode;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.wal.Lsn;

/**
 * Client interface for managing transactions. 
 * <p>
 * <strong>IMPORTANT:</strong> <em>A Transaction object must not
 * be concurrently used by more than one thread.</em> 
 * 
 * @see TransactionManager#begin()
 * @author Dibyendu Majumdar
 * @since 23-Aug-2005
 */
public interface Transaction {
	
	/**
	 * Returns the LSN of the most recent log record generated
	 * for this transaction.
	 */
	public Lsn getLastLsn();
	
	/**
	 * Generates a transaction related log record. The new log record is linked to the
	 * transaction's list of log records.
	 * <p>
	 * Transaction related log records must always be associated with specific disk pages.
	 */
	public Lsn logInsert(Page page, Loggable logrec) throws TransactionException;

	/**
	 * Acquires a lock in specified mode, and adds it to the transaction's list of locks.
	 * If the lock is not available, waits until the lock is granted or the lock
	 * timeout expires. 
	 * <p>
	 * If the Lock Manager does not support deadlock detection, the lock timeout is
	 * used to detect possible deadlocks.
	 * @see TransactionManager#setLockWaitTimeout(int)
	 * @see LockManager#acquire(Object, Object, LockMode, LockDuration, int)
	 */		
	public void acquireLock(Lockable lockable, LockMode mode, LockDuration duration) throws TransactionException;	

	/**
	 * Same as {@link #acquireLock(Object, LockMode, LockDuration) acquireLock} but does not wait
	 * for lock to be available.
	 * @see #acquireLock(Object, LockMode, LockDuration) 
	 */		
	public void acquireLockNowait(Lockable lockable, LockMode mode, LockDuration duration) throws TransactionException;	
	
	/**
	 * Attempts to release a lock, and if this succeeds, the lock is removed from
	 * the transaction's list of locks. Each lock has a reference count which indicates 
	 * how many times the lock was acquired by a transaction. This method will
	 * decrement the reference count, and the lock will be released only when
	 * reference count is 0.
	 * <p>
	 * Only SHARED or UPDATE mode locks may be released early. It is an error
	 * to try to release lock held in any other mode.
	 * @see org.simpledbm.rss.api.locking.LockHandle#release(boolean)
	 */
	public boolean releaseLock(Lockable lockable) throws TransactionException;
    
    /**
     * Checks if the specified object is locked by this transaction.
     * @return Current Lockmode or {@link LockMode#NONE} if lock is not held by this transaction.
     */
    public LockMode hasLock(Lockable lockable);
	
    /**
     * Downgrades a currently held lock to specified mode. If the downgrade
     * request is invalid, an Exception will be thrown.
     *  
     * @param lockable Object to be locked
     * @param downgradeTo The target lock mode
     */
    public void downgradeLock(Lockable lockable, LockMode downgradeTo) throws TransactionException;
    
	/**
	 * Schedules a post commit action. Post commit actions are used to deal with
	 * operations that should be deferred until it is certain that the transaction 
	 * is definitely committing. For example, dropping a Container. 
	 * <p>
	 * PostCommitActions are recorded in the transaction's prepare log record.
	 * <p>
	 * Once the transaction is committed, the pending post commit actions are
	 * performed. It is the responsibility of the Transactional Modul to
	 * ensure that each post commit action is logged appropriately. The
	 * Transaction Manager must ensure that these actions are performed despite
	 * system failures. 
	 */
	public void schedulePostCommitAction(PostCommitAction action);
	
	/**
	 * Creates a transaction savepoint.
	 */
	public Savepoint createSavepoint();

	/**
	 * Commits the transaction. All locks held by the
	 * transaction are released.
	 */
	public void commit() throws TransactionException;	

	/**
	 * Rolls back a transaction upto a savepoint. Locks acquired
	 * since the Savepoint are released. PostCommitActions queued
	 * after the Savepoint was created are discarded.
	 */
	public void rollback(Savepoint sp) throws TransactionException;	

	/**
	 * Aborts the transaction, undoing all changes and releasing locks.
	 */
	public void abort() throws TransactionException;	

	/**
	 * Starts a Nested Top Action. Will throw an exception if a nested top action is already in scope.
	 */
	public void startNestedTopAction() throws TransactionException;

	/**
	 * Completes a nested top action. Will throw an exception if there isn't a nested top action in scope.
	 */
	public void completeNestedTopAction() throws TransactionException;

	/**
	 * Abandons a nested top action.
	 */
	public void resetNestedTopAction();
	
	/**
	 * Returns the isolation mode set for the transaction.
	 */
	public IsolationMode getIsolationMode();
}
