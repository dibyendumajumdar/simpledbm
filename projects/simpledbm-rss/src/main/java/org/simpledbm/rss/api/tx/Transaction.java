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
package org.simpledbm.rss.api.tx;

import org.simpledbm.common.api.locking.LockMode;
import org.simpledbm.common.api.tx.IsolationMode;
import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockManager;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.wal.Lsn;

/**
 * Client interface for managing transactions.
 * <p>
 * <strong>IMPORTANT:</strong> <em>A Transaction object must not
 * be concurrently used by more than one thread.</em>
 * 
 * @see TransactionManager#begin(IsolationMode)
 * @author Dibyendu Majumdar
 * @since 23-Aug-2005
 */
public interface Transaction {

    /**
     * Returns the LSN of the most recent log record generated for this
     * transaction.
     */
    public Lsn getLastLsn();

    /**
     * Generates a transaction related log record. The new log record is linked
     * to the transaction's list of log records.
     * <p>
     * Transaction related log records must always be associated with specific
     * disk pages.
     */
    public Lsn logInsert(Page page, Loggable logrec);

    /**
     * Acquires a lock in specified mode, and adds it to the transaction's list
     * of locks. If the lock is not available, waits until the lock is granted
     * or the lock timeout expires.
     * <p>
     * If the Lock Manager does not support deadlock detection, the lock timeout
     * is used to detect possible deadlocks.
     * 
     * @see TransactionManager#setLockWaitTimeout(int)
     * @see LockManager#acquire(Object, Object, LockMode, LockDuration, int,
     *      org.simpledbm.rss.api.locking.LockInfo)
     */
    public void acquireLock(Lockable lockable, LockMode mode,
            LockDuration duration);

    /**
     * Same as {@link #acquireLock(Lockable, LockMode, LockDuration)} but does
     * not wait for lock to be available.
     * 
     * @see #acquireLock(Lockable, LockMode, LockDuration)
     */
    public void acquireLockNowait(Lockable lockable, LockMode mode,
            LockDuration duration);

    /**
     * Attempts to release a lock, and if this succeeds, the lock is removed
     * from the transaction's list of locks. Each lock has a reference count
     * which indicates how many times the lock was acquired by a transaction.
     * This method will decrement the reference count, and the lock will be
     * released only when reference count is 0.
     * <p>
     * Only SHARED or UPDATE mode locks may be released early. It is an error to
     * try to release lock held in any other mode.
     */
    public boolean releaseLock(Lockable lockable);

    /**
     * Checks if the specified object is locked by this transaction.
     * 
     * @return Current Lockmode or {@link LockMode#NONE} if lock is not held by
     *         this transaction.
     */
    public LockMode hasLock(Lockable lockable);

    /**
     * Downgrades a currently held lock to specified mode. If the downgrade
     * request is invalid, an Exception will be thrown.
     * 
     * @param lockable Object to be locked
     * @param downgradeTo The target lock mode
     */
    public void downgradeLock(Lockable lockable, LockMode downgradeTo);

    /**
     * Schedules a post commit action. Post commit actions are used to deal with
     * operations that should be deferred until it is certain that the
     * transaction is definitely committing. For example, dropping a Container.
     * <p>
     * PostCommitActions are recorded in the transaction's prepare log record.
     * <p>
     * Once the transaction is committed, the pending post commit actions are
     * performed. It is the responsibility of the Transactional Modul to ensure
     * that each post commit action is logged appropriately. The Transaction
     * Manager must ensure that these actions are performed despite system
     * failures.
     */
    public void schedulePostCommitAction(PostCommitAction action);

    /**
     * Creates a transaction savepoint.
     */
    public Savepoint createSavepoint(boolean saveCursors);

    /**
     * Commits the transaction. All locks held by the transaction are released.
     */
    public void commit();

    /**
     * Rolls back a transaction upto a savepoint. Locks acquired since the
     * Savepoint are released. PostCommitActions queued after the Savepoint was
     * created are discarded.
     */
    public void rollback(Savepoint sp);

    /**
     * Aborts the transaction, undoing all changes and releasing locks.
     */
    public void abort();

    /**
     * Starts a Nested Top Action. Will throw an exception if a nested top
     * action is already in scope.
     */
    public void startNestedTopAction();

    /**
     * Completes a nested top action. Will throw an exception if there isn't a
     * nested top action in scope.
     */
    public void completeNestedTopAction();

    /**
     * Abandons a nested top action.
     */
    public void resetNestedTopAction();

    /**
     * Returns the isolation mode set for the transaction.
     */
    public IsolationMode getIsolationMode();

    /**
     * Sets the lock timeout value.
     */
    public void setLockTimeout(int seconds);

    /**
     * Informs the transaction about a transactional cursor that needs to
     * participate in rollbacks.
     */
    public void registerTransactionalCursor(TransactionalCursor cursor);

    /**
     * Removes a transactional cursor from this transactions set.
     */
    public void unregisterTransactionalCursor(TransactionalCursor cursor);
}
