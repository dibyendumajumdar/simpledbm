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
package org.simpledbm.rss.impl.tx;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.simpledbm.rss.api.bm.BufferAccessBlock;
import org.simpledbm.rss.api.bm.BufferManager;
import org.simpledbm.rss.api.bm.DirtyPageInfo;
import org.simpledbm.rss.api.exception.ExceptionHandler;
import org.simpledbm.rss.api.latch.Latch;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockInfo;
import org.simpledbm.rss.api.locking.LockManager;
import org.simpledbm.rss.api.locking.LockMode;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.registry.ObjectFactory;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.registry.Storable;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageContainerInfo;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.api.tx.BaseLoggable;
import org.simpledbm.rss.api.tx.Compensation;
import org.simpledbm.rss.api.tx.ContainerDeleteOperation;
import org.simpledbm.rss.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.Lockable;
import org.simpledbm.rss.api.tx.Loggable;
import org.simpledbm.rss.api.tx.LoggableFactory;
import org.simpledbm.rss.api.tx.LogicalUndo;
import org.simpledbm.rss.api.tx.MultiPageRedo;
import org.simpledbm.rss.api.tx.NonTransactionRelatedOperation;
import org.simpledbm.rss.api.tx.PageFormatOperation;
import org.simpledbm.rss.api.tx.PostCommitAction;
import org.simpledbm.rss.api.tx.Redoable;
import org.simpledbm.rss.api.tx.Savepoint;
import org.simpledbm.rss.api.tx.SinglePageLogicalUndo;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.api.tx.TransactionException;
import org.simpledbm.rss.api.tx.TransactionId;
import org.simpledbm.rss.api.tx.TransactionManager;
import org.simpledbm.rss.api.tx.TransactionalCursor;
import org.simpledbm.rss.api.tx.TransactionalModule;
import org.simpledbm.rss.api.tx.TransactionalModuleRegistry;
import org.simpledbm.rss.api.tx.Undoable;
import org.simpledbm.rss.api.wal.LogManager;
import org.simpledbm.rss.api.wal.LogReader;
import org.simpledbm.rss.api.wal.LogRecord;
import org.simpledbm.rss.api.wal.Lsn;
import org.simpledbm.rss.util.ByteString;
import org.simpledbm.rss.util.Dumpable;
import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;

/**
 * <p>
 * This Transaction Manager implements the ARIES algorithm, as described by C.Mohan and others in:
 * <cite><a href="http://www.almaden.ibm.com/u/mohan/RJ6649Rev.pdf">ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking and Partial Rollbacks 
 * Using Write-Ahead Logging</a></cite>.
 * </p>
 * <p>
 * The implementation is fairly faithful to the published algorithms, except for differences noted below: 
 * </p>
 * <h3>Differences from ARIES</h3>
 * <p>
 * ARIES expects the Buffer Manager to maintain a dirty_pages table. We do not insist on this,
 * but assume that the Buffer Manager can generate such a list when required.
 * </p>
 * <p>
 * We support multi-page redo log records; such redo log records are applied to
 * a set of pages. This allows changes to multiple pages be logged atomically - a requirement
 * for some implementations of BTree.
 * </p>
 * <p>
 * The ARIES paper mentions post commit actions but does not show how to implement them. We provide
 * an implementation of post commit actions. Post commit actions are required to implement
 * dropping of containers.
 * </p>
 * <p>
 * We also support non-transaction related log records. For instance, opening of
 * a container can be logged as a non-transaction related operation. Non-transaction related
 * records are applied unconditionally at system restart.
 * </p>
 * <p>We log a list of open containers with Checkpoint so that these can be re-opened at system restart.
 * </p>
 * <h3>Limitations in current implementation</h3>
 * <ol>
 * <li>
 * It is assumed that only one thread may be executing a particular transaction. This assumption
 * leads to reduction in latching because there is no need to synchronise access to the
 * transaction control block (trx).
 * </li>
 * <li>
 * We cannot handle more than one Buffer Manager at present.
 * </li>
 * <li>
 * We do not deal with distributed transactions. This means that we do not need to remember locks
 * and re-acquire them during recovery.
 * </li>
 * </ol>
 * 
 * <h3>Main areas of concern</h3>
 * <ol>
 * <li>
 * Correctness of the algorithm - I made several mistakes when implementing the algorithm in C++.
 * Basically, it is vital to not miss out something - each step in the ARIES algorithm has been
 * carefully thought out. 
 * </li>
 * <li>
 * Latch contention and deadlock. The initial implementation suffered from deadlocks in at least
 * three situations. 
 * </li>
 * </ol>
 * 
 * @author Dibyendu Majumdar
 * @since 23-Aug-2005
 */
public final class TransactionManagerImpl implements TransactionManager {

    static final Logger log = Logger.getLogger(TransactionManagerImpl.class
        .getPackage()
        .getName());
    static final ExceptionHandler exceptionHandler = ExceptionHandler.getExceptionHandler(log);
    static final MessageCatalog mcat = new MessageCatalog();

    private static final short MODULE_ID = 1;

    private static final short TYPE_BASE = 0;
    private static final short TYPE_TRXPREPARE = TYPE_BASE + 1;
    private static final short TYPE_TRXABORT = TYPE_BASE + 2;
    private static final short TYPE_TRXEND = TYPE_BASE + 3;
    private static final short TYPE_CHECKPOINTBEGIN = TYPE_BASE + 4;
    private static final short TYPE_CHECKPOINTEND = TYPE_BASE + 5;
    private static final short TYPE_DUMMYCLR = TYPE_BASE + 6;

    static enum TrxState {
        TRX_UNPREPARED, TRX_PREPARED, TRX_DEAD
    }

    /**
     * Holds the list of dirty pages; used during restart recovery only.
     */
    private ArrayList<DirtyPageInfo> dirtyPages;

    /**
     * Active transactions table. Note that to reduce latching,
     * we mark dead transactions as TRX_DEAD but leave them in the
     * list for future garbage collection. This enables us to avoid
     * latching the list exclusively when transactions
     * are completed. 
     */
    private LinkedList<TransactionImpl> trxTable = new LinkedList<TransactionImpl>();

    /**
     * Protects modifications to the transaction table.
     * Must be latched exclusively when the transaction table needs to be modified.
     */
    final Latch latch;

    /**
     * The LSN from where restart redo will start. Used only during restart recovery.
     */
    private Lsn redoLsn;

    /**
     * The LSN upto which it is known that transactions have committed; in other words
     * the LSN of the oldest known committed transaction.
     */
    // Lsn commitLsn;

    final LogManager logmgr;

    final BufferManager bufmgr;

    final LockManager lockmgr;

    final LoggableFactory loggableFactory;
    
    final ObjectRegistry objectRegistry;

    final TransactionalModuleRegistry moduleRegistry;

    private final StorageManager storageManager;

    private final StorageContainerFactory storageFactory;

    /**
     * The default checkpoint interval in milliseconds.
     */
    static final int DEFAULT_CHECKPOINT_INTERVAL = 15000;
    static final String PROPERTY_CHECKPOINT_INTERVAL = "transaction.ckpt.interval";

    /**
     * The time interval between each checkpoint.
     */
    int checkpointInterval = DEFAULT_CHECKPOINT_INTERVAL;

    /**
     * The next transaction id. Transaction Ids must be monotonically increasing
     * values. The current value of the transaction id is saved during Checkpoint. At restart
     * the value is adjusted in case it is not be up-to-date.
     */
    private final AtomicLong trid = new AtomicLong(-1);

    /**
     * List of pending post commit actions, gathered during system restart.
     * Transactions save their post commit actions in the prepare record.
     * At restart we make a list of all actions that were logged in 
     * prepare records of active transactions, and then ensure that these
     * are executed when the transactions commit. 
     */
    private LinkedList<PostCommitAction> restartPendingActions;

    /**
     * List of active containers is logged in the CheckpointBegin record.
     * This list is used at system restart to re-open the containers.
     */
    private ActiveContainerInfo[] activeContainers;

    /**
     * This flag is used to signal to the CheckpointWriter thread
     * that it should shutdown.
     */
    volatile boolean stop = false;

    /**
     * CheckpointWriter thread is responsible for taking checkpoints at
     * periodic intervals.
     */
    Thread checkpointWriter;

    /**
     * This sync object is used for signalling the CheckpointWriter 
     * thread.
     */
    // Object checkpointWriterSync = new Object();

    /**
     * This flag is used to indicate that there has been an error
     * condition.
     */
    volatile boolean errored = false;

    /**
     * When lock requests are made, we specify a timeout value of 
     * 60 seconds. 
     */
    static final int DEFAULT_LOCK_TIMEOUT = 60;
    static final String PROPERTY_LOCK_TIMEOUT = "transaction.lock.timeout";
    
    /**
     * The lock timeout value can be configured to a different value.
     */
    int lockWaitTimeout = DEFAULT_LOCK_TIMEOUT;

    private int getNumericProperty(Properties props, String name,
            int defaultValue) {
        String value = props.getProperty(name);
        if (value != null) {
            try {
                int returnValue = Integer.parseInt(value);
                return returnValue;
            } catch (NumberFormatException e) {
                // Ignore and return default value
            }
        }
        return defaultValue;
    }
    
    public TransactionManagerImpl(LogManager logmgr,
            StorageContainerFactory storageFactory,
            StorageManager storageManager, BufferManager bufmgr,
            LockManager lockmgr, LoggableFactory loggableFactory,
            LatchFactory latchFactory, ObjectRegistry objectFactory,
            TransactionalModuleRegistry moduleRegistry,
            Properties p) {
    	
        this.logmgr = logmgr;
        this.storageFactory = storageFactory;
        this.storageManager = storageManager;
        this.bufmgr = bufmgr;
        this.lockmgr = lockmgr;
        this.objectRegistry = objectFactory;
        this.loggableFactory = loggableFactory;
        this.moduleRegistry = moduleRegistry;
        this.latch = latchFactory.newReadWriteLatch();

        objectFactory.registerObjectFactory(
            TYPE_TRXPREPARE,
            new TransactionManagerImpl.TrxPrepare.TrxPrepareFactory(loggableFactory));
        objectFactory.registerObjectFactory(
            TYPE_TRXABORT,
            new TransactionManagerImpl.TrxAbort.TrxAbortFactory());
        objectFactory.registerObjectFactory(
            TYPE_TRXEND,
            new TransactionManagerImpl.TrxEnd.TrxEndFactory());
        objectFactory.registerObjectFactory(
            TYPE_CHECKPOINTBEGIN,
            new TransactionManagerImpl.CheckpointBegin.CheckpointBeginFactory());
        objectFactory.registerObjectFactory(
            TYPE_CHECKPOINTEND,
            new TransactionManagerImpl.CheckpointEnd.CheckpointEndFactory(this));
        objectFactory.registerObjectFactory(
            TYPE_DUMMYCLR,
            new TransactionManagerImpl.DummyCLR.DummyCLRFactory());

        this.lockWaitTimeout = getNumericProperty(p, PROPERTY_LOCK_TIMEOUT, DEFAULT_LOCK_TIMEOUT);
        this.checkpointInterval = getNumericProperty(p, PROPERTY_CHECKPOINT_INTERVAL, DEFAULT_CHECKPOINT_INTERVAL);
        
        log.info(getClass().getName(), "ctor", mcat.getMessage("IX0019", PROPERTY_LOCK_TIMEOUT, lockWaitTimeout));
        log.info(getClass().getName(), "ctor", mcat.getMessage("IX0019", PROPERTY_CHECKPOINT_INTERVAL, checkpointInterval));
    }

    /**
     * Allocate the next transaction Id. Transaction Ids must be monotonically increasing
     * values. The current value of the transaction id is saved during Checkpoint. At restart
     * the value is adjusted in case it is not be up-to-date.
     * <p>
     * Latching issues:<br>
     * TrxMgr is latched exlusively when creating a new transaction, so no
     * further latching required here.
     */
    private TransactionId allocateNextTrxId() {
        return new TransactionId(trid.incrementAndGet());
    }

    /**
     * Update the system transaction Id counter.
     * This function is called during recovery to update the master transaction ID. If a 
     * transaction read from the Log has an ID that is greater than the current master ID,
     * then, the master transaction ID is set to the Log's trid.
     * <p>
     * Latching issues:<br>
     * The trxmgr is latched exclusively during recovery - hence no further latching required.
     */
    private void resetTrxId(TransactionId trxId) {
        if (trid.get() < trxId.longValue()) {
            trid.set(trxId.longValue());
        }
    }

    public final int getCheckpointInterval() {
        latch.sharedLock();
        try {
            return checkpointInterval;
        } finally {
            latch.unlockShared();
        }
    }

    public final void setCheckpointInterval(int checkpointInterval) {
        latch.exclusiveLock();
        try {
            this.checkpointInterval = checkpointInterval;
        } finally {
            latch.unlockExclusive();
        }
    }

    public final int getLockWaitTimeout() {
        latch.sharedLock();
        try {
            return lockWaitTimeout;
        } finally {
            latch.unlockShared();
        }
    }

    public final void setLockWaitTimeout(int lockWaitTimeout) {
        latch.exclusiveLock();
        try {
            this.lockWaitTimeout = lockWaitTimeout;
        } finally {
            latch.unlockExclusive();
        }
    }

    /**
     * Allocate a new transaction. This function is also called by the 
     * Transaction Manager when reading a new transaction from the LOG.
     * When called during recovery, a trid is supplied. Otherwise, trid is
     * generated by incrementing the master trid. 
     * <p>
     * Latching issues:<br>
     * The trxmgr must be exclusively latched prior to calling this routine.
     * This is to avoid conflicts with the checkpoint function.
     */
    private TransactionImpl newTransaction(TransactionId trxId) {

        TransactionImpl trx = null;
        if (trxId == null) {
            /* New transaction - allocate a new transaction Id */
            trx = new TransactionImpl(this, allocateNextTrxId());
        } else {
            /* Transaction read from the LOG. Hence, we need to check whether the
             * trid is greater than the current trid.
             */
            resetTrxId(trxId);
            trx = new TransactionImpl(this, trxId);
        }
        trxTable.add(trx);
        if (log.isDebugEnabled()) {
            log.debug(
                this.getClass().getName(),
                "newTransaction",
                "SIMPLEDBM-DEBUG: Starting new transaction " + trx.trxId);
        }
        return trx;
    }

    /** 
     * Remove a transaction from the list of active transactions. We avoid removing the trx from the
     * transaction table so that there is no need to latch trxmgr.
     * <p>
     * Latching issues:<br>
     * No latches required because there is no change to the transaction table.
     */
    void deleteTransaction(TransactionImpl trx) {
        assert trx.state != TrxState.TRX_DEAD;
        if (log.isDebugEnabled()) {
            log.debug(
                this.getClass().getName(),
                "deleteTransaction",
                "SIMPLEDBM-DEBUG: Deleting transaction " + trx);
        }
        trx.releaseLocks(null);
        trx.state = TrxState.TRX_DEAD;
    }

    /** 
     * Begin a new transaction. There is no need to log this because the first log record
     * generated by a transaction is enough to indicate the start of a transaction. 
     * <p>
     * Latching issues:<br>
     * We acquire an exclusive latch because we are about to modify the transaction table.
     */
    public final Transaction begin(IsolationMode isolationMode) {
        latch.exclusiveLock();
        try {
            TransactionImpl trx = newTransaction(null);
            trx.isolationMode = isolationMode;
            return trx;
        } finally {
            latch.unlockExclusive();
        }
    }

    /**
     * Validate Loggable hierarchy to help catch potential bugs in client code.
     */
    void validateLoggableHierarchy(Loggable loggable) {
        String errCode = null;
        if (loggable instanceof Redoable) {
            if (loggable instanceof NonTransactionRelatedOperation) {
                errCode = "EX0002";
            } else if (loggable instanceof PostCommitAction) {
                errCode = "EX0003";
            } else if (loggable instanceof Checkpoint) {
                errCode = "EX0004";
            } else if (loggable instanceof TrxControl) {
                errCode = "EX0005";
            }
        } else if (loggable instanceof Undoable) {
            if (loggable instanceof MultiPageRedo) {
                errCode = "EX0006";
            } else if (loggable instanceof Compensation) {
                errCode = "EX0007";
            } else if (loggable instanceof ContainerDeleteOperation) {
                errCode = "EX0008";
            }
        }
        if (null != errCode) {
            exceptionHandler.errorThrow(
                this.getClass().getName(),
                "validateLoggableHierarchy",
                new TransactionException(mcat.getMessage(errCode, loggable)));
        }
    }

    /**
     * Inserts a new log record. 
     */
    Lsn doLogInsert(Loggable logrec) {
        validateLoggableHierarchy(logrec);
        byte[] data = new byte[logrec.getStoredLength()];
        ByteBuffer bb = ByteBuffer.wrap(data);
        logrec.store(bb);
        Lsn lsn = logmgr.insert(data, data.length);
        logrec.setLsn(lsn);
        if (log.isTraceEnabled()) {
            log.trace(
                this.getClass().getName(),
                "doLogInsert",
                "SIMPLEDBM-TRACE: Inserted log record " + logrec);
        }
        return lsn;
    }

    /**
     * Inserts non-transaction related log record.
     * No latching needed.
     */
    public final Lsn logNonTransactionRelatedOperation(Loggable operation) {
        if (operation instanceof NonTransactionRelatedOperation) {
            return doLogInsert(operation);
        } else {
            exceptionHandler.errorThrow(
                this.getClass().getName(),
                "logNonTransactionRelatedOperation",
                new TransactionException(mcat.getMessage("EX0009", 
                	operation.getClass().getName(), 
                	NonTransactionRelatedOperation.class.getName())));
            return null;
        }
    }

    /**
     * Generates a Checkpoint. The trxmgr is exclusively latched
     * to prevent modification of the transaction table. Implication is
     * that new transactions cannot start while a checkpoint is being taken.
     * @see #writeCheckpoint()
     */
    public final void checkpoint() {
        latch.exclusiveLock();
        try {
            writeCheckpoint();
        } finally {
            latch.unlockExclusive();
        }
    }

    /**
     * Remove dead transactions from the transaction table.
     */
    private void removeDeadTransactions() {
        Iterator<TransactionImpl> iter = trxTable.iterator();
        while (iter.hasNext()) {
            TransactionImpl trx = iter.next();
            if (trx.state == TrxState.TRX_DEAD) {
                iter.remove();
            }
        }
    }

    /**
     * Generates a Checkpoint. Checkpoint is generated using two records - a begin and
     * an end record.
     * <p>CheckpointEnd record contains a list of dirty pages in the Buffer Manager, and a list of all 
     * currently active transactions. The list of open containers is logged in the
     * checkpoint begin record.</p>
     * <p>Checkpoint records can potentially be large.</p> 
     * <p>Limitation: A checkpoint record must fit into a single log record.</p>
     * <p>Restrictions: The ContainerDeleteOperation must not occur during checkpoint. This
     * is ensured by acquiring exclusive latch on trxmgr during checkpoints,
     * which prevents commits and rollbacks from executing concurrently with
     * checkpoints. Since the ContainerDeleteOperation is performed during commit,
     * it cannot occur concurrently with a checkpoint.</p>
     * <p>Latching: Caller must obtain exclusive latch on trxmgr prior to
     * calling this method. TODO: I think that we could reduce the duration of latching 
     * by releasing the latch after constructing the records in memory. However, still
     * need to ensure that ContainerDeleteOperation cannot occur.</p>
     */
    private void writeCheckpoint() {

        if (log.isDebugEnabled()) {
            log.debug(
                this.getClass().getName(),
                "writeCheckPoint",
                "SIMPLEDBM-DEBUG: Creating a checkpoint");
        }

        /* 
         * Write checkpoint BEGIN record. Note that the LSN of the BEGIN record will be saved as
         * the CheckpointLsn. Although we could combine the begin, body and end records
         * into one, keeping them separate helps with handling of large checkpoints that need 
         * to be broken into chunks.
         * 
         * The importance of starting the redo scan from the Checkpoint Begin record is
         * explained in Mohan's paper. Basically, it allows the checkpoint data to be broken
         * into chunks, and also allows the dirty pages list to change even while the checkpoint
         * is in operation.  
         */
        CheckpointBegin checkpointBegin = new CheckpointBegin(0, TransactionManagerImpl.TYPE_CHECKPOINTBEGIN);
        
        checkpointBegin.setActiveContainers(storageManager
            .getActiveContainers());
        Lsn checkpointLsn = doLogInsert(checkpointBegin);

        /*
         * Since we have an exclusive latch on trxmgr, let's clean up the
         * transaction table:
         */
        removeDeadTransactions();

        /*
         * Write the contents of checkpoint end record. Ideally this should be broken down into smaller
         * records. 
         */
        CheckpointEnd checkpointEnd = new CheckpointEnd(0, TransactionManagerImpl.TYPE_CHECKPOINTEND, this);
        checkpointEnd.setTransactionTable(trxTable);
        /*
         * Calculate the oldest interesting LSN. This can be used by the
         * Log Manager to reclaim unwanted archived log files.
         */
        DirtyPageInfo[] dirtyPageArray = bufmgr.getDirtyPages();
        Lsn undoLWM = getOldestInterestingLsn();
        for (DirtyPageInfo dp : dirtyPageArray) {
            if (undoLWM.isNull()
                    || undoLWM.compareTo(dp.getRealRecoveryLsn()) > 0) {
                undoLWM = dp.getRealRecoveryLsn();
            }
        }
        if (undoLWM.isNull() || undoLWM.compareTo(checkpointLsn) > 0) {
            undoLWM = checkpointLsn;
        }
        checkpointEnd.setDirtyPageList(dirtyPageArray);
        checkpointEnd.setTrxId(new TransactionId(trid.get()));
        Lsn flushLsn = doLogInsert(checkpointEnd);

        if (log.isDebugEnabled()) {
            log.debug(
                this.getClass().getName(),
                "writeCheckpoint",
                "SIMPLEDBM-DEBUG: CheckpointLSN = " + checkpointLsn
                        + ", oldestInterestingLSN = " + undoLWM);
        }

        /*
         * Note that the checkpointLsn is updated after the checkpoint logs have been
         * generated but before the log flush. Since the checkpointLsn is part of the
         * Log Anchor, it will be written after the log records have been flushed. Therefore,
         * the Log Anchor will always point to a valid checkpoint record. If the system
         * fails after writing the log records but before writing the Log Anchor, then the
         * Log Anchor will retain its existing checkpointLsn.
         */
        logmgr.setCheckpointLsn(checkpointLsn, undoLWM);
        /*
         * According to Mohan the log flush is not needed. But because setting checkpointLsn 
         * will result in the log Anchor being updated at next flush, we do the flush
         * now. 
         * TODO: Explore alternatives.
         */
        logmgr.flush(flushLsn);
    }

    /**
     * Reopens containers that were active at the time of last checkpoint.
     * Caller must have initialised activeContainers list.
     */
    private void reopenActiveContainers() {
        if (getActiveContainers() == null) {
            return;
        }
        for (int i = 0; i < getActiveContainers().length; i++) {
            if (getActiveContainers()[i] == null) {
                continue;
            }
            if (log.isDebugEnabled()) {
                log.debug(
                    this.getClass().getName(),
                    "reopenActiveContainers",
                    "SIMPLEDBM-DEBUG: Reopening container "
                            + getActiveContainers()[i]);
            }
            StorageContainer sc = storageManager
                .getInstance(getActiveContainers()[i].containerId);
            if (sc == null) {
                sc = storageFactory.open(getActiveContainers()[i].name
                    .toString());
                storageManager.register(
                    getActiveContainers()[i].containerId,
                    sc);
            } else {
                if (!sc.getName().equals(
                    getActiveContainers()[i].name.toString())) {
                    exceptionHandler.errorThrow(
                        this.getClass().getName(),
                        "reopenActiveContainers",
                        new TransactionException(mcat.getMessage(
                        "EX0010",
                        getActiveContainers()[i].containerId,
                        sc.getName(),
                        getActiveContainers()[i].name.toString())));
                }
            }
        }
    }

    /**
     * Reads a checkpoint record.
     * <p>
     * Latching issues:<br>
     * We do not acquire any latches because trxmgr is exclusively latched during recovery anyway.
     */
    private LogReader readCheckpoint() {
        Lsn scanLsn = logmgr.getCheckpointLsn();
        if (log.isDebugEnabled()) {
            log.debug(
                this.getClass().getName(),
                "readCheckpoint",
                "SIMPLEDBM-DEBUG: Reading checkpoint record at " + scanLsn);
        }
        LogReader reader = logmgr.getForwardScanningReader(scanLsn);
        LogRecord logrec = reader.getNext();
        if (logrec == null) {
            exceptionHandler.errorThrow(this.getClass().getName(), "readCheckpoint", 
            		new TransactionException(mcat.getMessage("EX0011")));
        }
        CheckpointBegin checkpointBegin = (CheckpointBegin) loggableFactory
            .getInstance(logrec);
        setActiveContainers(checkpointBegin.getActiveContainers());

        logrec = reader.getNext();
        if (logrec == null) {
            exceptionHandler.errorThrow(this.getClass().getName(), "readCheckpoint", 
            		new TransactionException(mcat.getMessage("EX0011")));
        }
        CheckpointEnd checkpointBody = (CheckpointEnd) loggableFactory
            .getInstance(logrec);
        trxTable = checkpointBody.trxTable;
        dirtyPages = checkpointBody.newDirtyPages;
        trid.set(checkpointBody.getTrxId().longValue());

        return reader;
    }

    /**
     * Add a page to the list of dirty pages if it not already present. Used
     * during restart recovery.
     * <p>
     * Latching issues:<br>
     * None because trxmgr is already exclusively latched.
     */
    private void addToDirtyPages(PageId pageId, Lsn recoveryLsn) {
        for (DirtyPageInfo dp : dirtyPages) {
            if (dp.getPageId().equals(pageId)) {
                return;
            }
        }
        DirtyPageInfo dp = new DirtyPageInfo(pageId, recoveryLsn, recoveryLsn);
        dirtyPages.add(dp);
    }

    /**
     * Delete transactions that are unprepared and that have not generated any undoable 
     * log records. Used during restart recovery.
     * <p>
     * Latching issues:<br>
     * None because trxmgr is already latched.
     */
    private void deleteUnpreparedNullTransactions() {
        for (TransactionImpl trx : trxTable) {
            /* delete transactions where state == UNPREPARED and undo_next_lsn == NULL */
            if (trx.state == TrxState.TRX_UNPREPARED
                    && trx.undoNextLsn.isNull()) {
                /* Generate the transaction END log record */
                trx.endTransaction();
                deleteTransaction(trx);
            }
        }
    }

    /**
     * Get the minimum recovery_lsn amongst all recovery_lsn values in the dirty_pages list.
     * <p>
     * Latching issues:<br>
     * None because trxmgr is already latched.
     */
    private Lsn getMinimumRecoveryLsn(ArrayList<DirtyPageInfo> dirtyPages) {
        Lsn lsn = new Lsn();
        for (DirtyPageInfo dp : dirtyPages) {
            if (lsn.isNull() || dp.getRealRecoveryLsn().compareTo(lsn) < 0) {
                lsn = dp.getRealRecoveryLsn();
            }
        }
        return lsn;
    }

    /**
     * Find a page in the dirty page list.
     * <p>
     * Latching issues:<br>
     * None because trxmgr is already latched.
     */
    private DirtyPageInfo getDirtyPage(PageId pageId) {
        for (DirtyPageInfo dp : dirtyPages) {
            if (dp.getPageId().equals(pageId)) {
                return dp;
            }
        }
        return null;
    }

    /**
     * Remove all dirty pages that belong to the container that has been
     * deleted. Also remove the container from the list of active containers.
     */
    private void processContainerDelete(Loggable loggable) {
        ContainerDeleteOperation containerDelete = (ContainerDeleteOperation) loggable;
        if (log.isDebugEnabled()) {
            log
                .debug(
                    this.getClass().getName(),
                    "processContainerDelete",
                    "SIMPLEDBM-DEBUG: Removing pages of deleted container "
                            + containerDelete.getContainerId()
                            + " from Buffer Manager, and removing container from list of open containers");
        }
        Iterator<DirtyPageInfo> iter = dirtyPages.iterator();
        while (iter.hasNext()) {
            DirtyPageInfo dp = iter.next();
            if (dp.getPageId().getContainerId() == containerDelete
                .getContainerId()) {
                iter.remove();
            }
        }
        if (getActiveContainers() != null) {
            for (int i = 0; i < getActiveContainers().length; i++) {
                if (getActiveContainers()[i].containerId == containerDelete
                    .getContainerId()) {
                    getActiveContainers()[i] = null;
                }
            }
        }
    }

    /**
     * Remove a deleted container from the Storage Manager
     * module. Unfortunately we cannot physically remove the container due to the
     * fact that at restart the open container event may be replayed, and this
     * event requires the container to be available.
     */
    private void removeDeletedContainer(Loggable loggable) {
        ContainerDeleteOperation containerDelete = (ContainerDeleteOperation) loggable;
        if (log.isDebugEnabled()) {
            log.debug(
                this.getClass().getName(),
                "removeContainerDelete",
                "SIMPLEDBM-DEBUG: Removing container "
                        + containerDelete.getContainerId()
                        + " from Storage Manager");
        }
        if (storageManager.getInstance(containerDelete.getContainerId()) != null) {
            storageManager.remove(containerDelete.getContainerId());
            // FIXME: Must remove the container physically. One option is to
            // ignore errors when opening container. 
        }
    }

    /**
     * Find a Transaction record.
     * <p>
     * Latching issues:<br>
     * None because trxmgr is already latched.
     */
    private TransactionImpl findTransaction(TransactionId trxId) {
        for (TransactionImpl trx : trxTable) {
            if (trx.trxId.equals(trxId)) {
                return trx;
            }
        }
        return null;
    }

    /**
     * Removes the specified action from the list of pending actions.
     */
    private void removePostCommitAction(PostCommitAction deleteaction) {
        Iterator<PostCommitAction> iter = restartPendingActions.iterator();
        while (iter.hasNext()) {
            PostCommitAction action = iter.next();
            if (action.getTrxId().equals(deleteaction.getTrxId())
                    && action.getActionId() == deleteaction.getActionId()) {
                if (log.isDebugEnabled()) {
                    log.debug(
                        this.getClass().getName(),
                        "removePostCommitAction",
                        "SIMPLEDBM-DEBUG: Discarding Post Commit Action "
                                + action);
                }
                iter.remove();
                break;
            }
        }
    }

    /**
     * Discards all pending actions for specified transaction.
     */
    private void discardPostCommitActions(TransactionId trxid) {
        Iterator<PostCommitAction> iter = restartPendingActions.iterator();
        while (iter.hasNext()) {
            PostCommitAction action = iter.next();
            if (action.getTrxId().equals(trxid)) {
                if (log.isDebugEnabled()) {
                    log.debug(
                        this.getClass().getName(),
                        "discardPostCommitActions",
                        "SIMPLEDBM-DEBUG: Discarding Post Commit Action "
                                + action);
                }
                iter.remove();
                break;
            }
        }
    }

    /**
     * Obtains the oldest LSN that may be required for undo recovery. 
     * This is the start LSN of the oldest transaction
     * currently active.
     * <p>
     * The Transaction Manager must be latched exclusively prior
     * to calling this method.
     */
    private Lsn getOldestInterestingLsn() {
        Lsn lsn = new Lsn();
        for (TransactionImpl trx : trxTable) {
            if (trx.state == TrxState.TRX_DEAD) {
                continue;
            }
            if (trx.firstLsn.isNull()) {
                continue;
            }
            if (lsn.isNull() || trx.firstLsn.compareTo(lsn) < 0) {
                lsn = new Lsn(trx.firstLsn);
            }
        }
        return lsn;
    }

    /**
     * Mohan:
     * The first pass of the log that is made during restart recovery is the analysis pass.
     * The result from this pass are the transaction table, which contains the list of
     * transactions which were in unprepared state at the time of system failure; the 
     * dirty_pages table, which contains the list of pages that were potentially dirty
     * in the buffers when the system failed, and the RedoLSN, which is the location from 
     * which the redo pass must start processing the log. The only log records that may
     * be written by this routine are end records for transactions that had totally
     * rolled back before system failure, but for whom end records are missing.
     * During this pass, if a log record is encountered for a page whose identity does
     * not already appear in the dirty_pages table, then an entry is made in the table
     * with the current log record's LSN as the page's recovery_lsn. The transaction
     * table is modified to track the state changes of transactions and also to note the
     * LSN of the most recent log record that would need to be undone if it were
     * determined ultimately that the transaction had to be rolled back. The RedoLSN
     * is the minimum of recovery_lsn from the dirty pages table at the end of the
     * analysis pass.
     * <p>
     * Latching issues:<br>
     * None because trxmgr is already latched.
     */
    private void restartAnalysis() {

        if (log.isDebugEnabled()) {
            log.debug(
                this.getClass().getName(),
                "restartAnalysis",
                "SIMPLEDBM-DEBUG: Starting restart analysis");
        }

        /* Open logscan at Begin checkpint record */
        LogReader reader = readCheckpoint();
        /* durableLsn marks the end of log */
        Lsn durableLsn = logmgr.getDurableLsn();

        LogRecord logrec = reader.getNext();
        while (logrec != null && logrec.getLsn().compareTo(durableLsn) <= 0) {

            TransactionImpl trx = null;
            Loggable loggable = loggableFactory.getInstance(logrec);

            /*
             * There are following type of log records:
             * Redoable (including Undoable and Compensation)
             * TrxControl 
             * Checkpoint
             * NonTransactionRelatedOperation (such as opening a container)
             *   PostCommitAction (these are also NonTransactionRelatedOperations)
             * ContainerDeleteOperation (can also be compensation/redoable if it is the result of an undo
             *   of create container operation)
             * 
             * In the ARIES paper, we are supposed to check transaction related records -
             * excluding checkpoint and OSfile_return (ContainerDeleteOperation). Although 
             * PostCommitActions are associated with a transaction, they are NOT linked to
             * the transactions list of records - hence they must not be included below.  
             */
            if (loggable instanceof Redoable || loggable instanceof TrxControl) {
                /*
                 * Infer the start of a new transaction from the first transaction related log record.
                 */
                assert !loggable.getTrxId().isNull();
                trx = findTransaction(loggable.getTrxId());
                if (trx == null) {
                    trx = newTransaction(loggable.getTrxId());
                }
                trx.registerLsn(loggable, loggable.getLsn());
            }

            if (loggable instanceof Redoable) {
                /*
                 * Redoable records are always page oriented, except for DummyCLRs.
                 * Compensation log records are redoable.
                 * DummyCLRs need to be skipped. This is easy because a DummyCLR's pageId is null.
                 */
                if (!loggable.getPageId().isNull()) {
                    if (loggable instanceof MultiPageRedo) {
                        PageId[] pageIds = ((MultiPageRedo) loggable)
                            .getPageIds();
                        for (PageId pageId : pageIds) {
                            addToDirtyPages(pageId, loggable.getLsn());
                        }
                    } else {
                        addToDirtyPages(loggable.getPageId(), loggable.getLsn());
                    }
                }
            }

            else if (loggable instanceof Checkpoint) {
                /* Skip incomplete checkpoint data */
            }

            else if (loggable instanceof TrxPrepare) {
                assert trx.state == TrxState.TRX_UNPREPARED;
                trx.state = TrxState.TRX_PREPARED;
                TrxPrepare prepareLog = (TrxPrepare) loggable;
                /*
                 * The prepare log contains a list of PostCommitActions.
                 * We add these to a global list of pending actions.
                 */
                restartPendingActions.addAll(prepareLog.getPostCommitActions());
            }

            else if (loggable instanceof TrxAbort) {
                trx.state = TrxState.TRX_UNPREPARED;
                /*
                 * No need to perform the PostCommitActions associated with 
                 * this transaction.
                 */
                discardPostCommitActions(loggable.getTrxId());
            }

            else if (loggable instanceof TrxEnd) {
                deleteTransaction(trx);
            }

            if (loggable instanceof ContainerDeleteOperation) {
                /*
                 * ARIES: Here we are supposed to to do following:
                 * WHEN('OSfile_return') delete from Dirty_pages all pages of returned file.
                 * We need this because otherwise we may end up applying
                 * changes to non-existent pages during redo. If the pages subsequently become part of
                 * another container, then it will cause chaos. 
                 */
                processContainerDelete(loggable);
            }

            if (loggable instanceof PostCommitAction) {
                /*
                 * This means that the PostCommitAction has been performed. Therefore
                 * we can remove it from the list of global pending actions.
                 */
                removePostCommitAction((PostCommitAction) loggable);
            }

            /*
             * Although durableLsn may have changed we do not need to refresh it because
             * the new log records are all end transaction records. These do not affect
             * recovery.
             */
            if (logrec.getLsn().compareTo(durableLsn) < 0) {
                logrec = reader.getNext();
            } else {
                break;
            }
        }
        deleteUnpreparedNullTransactions();
        redoLsn = getMinimumRecoveryLsn(dirtyPages);
        if (log.isDebugEnabled()) {
            log.debug(
                this.getClass().getName(),
                "restartAnalysis",
                "SIMPLEDBM-DEBUG: Redo processing will start from log record "
                        + redoLsn);
        }
    }

    /**
     * Performs Post Commit actions for a specified transaction. 
     * <p>
     * Latching issues: all latching handled by caller.
     */
    private void doPostCommitActions(LinkedList<PostCommitAction> actions,
            TransactionId trxId) {
        Iterator<PostCommitAction> iter = actions.iterator();
        while (iter.hasNext()) {
            PostCommitAction action = iter.next();
            if (action.getTrxId().equals(trxId)) {
                TransactionalModule module = moduleRegistry.getModule(action
                    .getModuleId());
                moduleRedo(module, action);
                logNonTransactionRelatedOperation(action);
                iter.remove();
            }
        }
    }

    /**
     * Mohan:
     * The second pass of the log that is made during restart recovery is the redo pass.
     * No logs are written in this pass. The redo pass starts scanning the log records from
     * the trxmgr->redo_lsn point. When a redoable log record is encountered, a check is
     * made to see if the referenced page appears in the dirty_pages table. If it does and if 
     * the log record's LSN is greater than or equal to the recovery_lsn of the page in
     * the table, then it is suspected that the page state might be such that the log 
     * record's update might have to be redone. To resolve this suspicion, the page is 
     * accessed. If the page's LSN is found to be less than the log record's LSN,
     * then the update is redone.
     * <p>
     * Latching issues:<br>
     * None because trxmgr is already latched.
     */
    private void restartRedo() {

        if (redoLsn.isNull()) {
            if (log.isDebugEnabled()) {
                log
                    .debug(
                        this.getClass().getName(),
                        "restartRedo",
                        "SIMPLEDBM-DEBUG: Skipping restart redo phase as there are no logs to process");
            }
            return;
        } else {
            if (log.isDebugEnabled()) {
                log.debug(
                    this.getClass().getName(),
                    "restartRedo",
                    "SIMPLEDBM-DEBUG: Starting restart redo phase");
            }
        }

        LogReader reader = logmgr.getForwardScanningReader(redoLsn);
        Lsn durableLsn = logmgr.getDurableLsn();
        LogRecord logrec = reader.getNext();

        while (logrec != null && logrec.getLsn().compareTo(durableLsn) <= 0) {
            Loggable loggable = loggableFactory.getInstance(logrec);

            if (log.isTraceEnabled()) {
                log.trace(
                    this.getClass().getName(),
                    "restartRedo",
                    "SIMPLEDBM-TRACE: Processing log record " + loggable);
            }

            if (loggable instanceof ContainerDeleteOperation) {
                /*
                 * This is a kludge to ensure that the open container action for a 
                 * container that is deleted as a result of aborting create container is
                 * undone.
                 */
                removeDeletedContainer(loggable);
            }

            if (loggable instanceof Redoable) {

                PageId[] pageIds;
                if (loggable instanceof MultiPageRedo) {
                    pageIds = ((MultiPageRedo) loggable).getPageIds();
                } else {
                    pageIds = new PageId[] { loggable.getPageId() };
                }

                for (PageId pageid : pageIds) {
                    DirtyPageInfo dp = getDirtyPage(pageid);
                    if (dp != null
                            && logrec.getLsn().compareTo(dp.getRecoveryLsn()) >= 0) {

                        BufferAccessBlock bab;
                        /* 
                         * This can fail if the page wasn't written properly.
                         * The log record contains the page type information so that we
                         * can create the type of page that we need if the read fails.
                         */
                        if (loggable instanceof PageFormatOperation) {
                            /*
                             * We assume that the page wasn't written properly
                             */
                            bab = bufmgr.fixExclusive(pageid, true, loggable
                                .getPageType(), 0);
                        } else {
                            bab = bufmgr.fixExclusive(pageid, false, -1, 0);
                        }
                        try {
                            Page page = bab.getPage();
                            if (page.getPageLsn().compareTo(logrec.getLsn()) < 0) {
                                if (log.isTraceEnabled()) {
                                    log.trace(
                                        this.getClass().getName(),
                                        "restartRedo",
                                        "SIMPLEDBM-TRACE: Redoing the effects of Log record "
                                                + loggable);
                                }
                                TransactionalModule module = moduleRegistry
                                    .getModule(loggable.getModuleId());
                                moduleRedo(module, page, (Redoable) loggable);
                                bab.setDirty(logrec.getLsn());
                            } else {
                                if (log.isTraceEnabled()) {
                                    log.trace(
                                        this.getClass().getName(),
                                        "restartRedo",
                                        "SIMPLEDBM-TRACE: Skipping redo of log record ["
                                                + loggable + "] on page ["
                                                + pageid
                                                + "] as page LSN >= log LSN");
                                }
                                /* TODO: how to increment an LSN so that we get
                                 * a valid LSN as the result ? Following code will
                                 * work as long as we do not attempt to access
                                 * a log record using the recovery_lsn
                                 */
                                /*
                                 * 8-May-2004 
                                 * Hit above problem !!
                                 * Following makes recovery_lsn invalid, so we
                                 * cannot use this value for real recovery_lsn of the page.
                                 * hence we note the real recovery lsn above.
                                 */
                                dp.setRealRecoveryLsn(page.getPageLsn());
                                dp.setRecoveryLsn(new Lsn(
                                    page.getPageLsn().getIndex(),
                                    page.getPageLsn().getOffset() + 1));
                            }
                        } finally {
                            bab.unfix();
                        }
                    }

                    else {
                        if (log.isTraceEnabled()) {
                            log.trace(
                                this.getClass().getName(),
                                "restartRedo",
                                "SIMPLEDBM-TRACE: Skipping redo of log record ["
                                        + loggable + "] on page [" + pageid
                                        + "] as page is not dirty");
                        }
                    }
                }
            }

            else if (loggable instanceof NonTransactionRelatedOperation
                    || loggable instanceof PostCommitAction) {

                /*
                 * This is not specified in ARIES but we need a way of redoing
                 * operations that are not related to pages. For example, creating
                 * a new container, or deleting a container, or extending a 
                 * container. 
                 */
                TransactionalModule module = moduleRegistry.getModule(loggable
                    .getModuleId());
                try {
                    moduleRedo(module, loggable);
                } catch (TransactionException e) {
                    /*
                     * 05-Dec-05
                     * We ignore exceptions raised by NonTransactionRelatedOperations as
                     * a workaround for the OpenContainer problem. If a container creation 
                     * operation is aborted, then the OpenContainer operation will fail 
                     * at restart.
                     */
                    log.warn(this.getClass().getName(), "restartRedo", mcat
                        .getMessage("WX0012", loggable), e);
                }
            }

            else if (loggable instanceof TrxEnd) {

                TrxEnd trxEnd = (TrxEnd) loggable;
                /* perform any pending actions */
                if (log.isTraceEnabled()) {
                    log.trace(
                        this.getClass().getName(),
                        "restartRedo",
                        "SIMPLEDBM-TRACE: Examining post commit actions of log record "
                                + loggable);
                }
                doPostCommitActions(restartPendingActions, trxEnd.getTrxId());
            }

            if (logrec.getLsn().compareTo(durableLsn) < 0) {
                logrec = reader.getNext();
            } else {
                break;
            }
        }
    }

    /** 
     * Get the maximum recovery_lsn amongst all recovery_lsn values in the dirty_pages list.
     * <p>
     * Latching issues:<br>
     * None because trxmgr is already latched.
     */
    private Lsn getMaximumUnpreparedUndoLsn() {
        Lsn lsn = new Lsn();

        for (TransactionImpl trx : trxTable) {
            if (trx.state == TrxState.TRX_UNPREPARED) {
                if (lsn.compareTo(trx.undoNextLsn) < 0) {
                    lsn = trx.undoNextLsn;
                }
            }
        }
        return lsn;
    }

    /**
     * Mohan:
     * This is the third pass in the recovery process. In this pass, loser transactions are
     * rolled back in reverse chronological order, in a single sweep of the log. This is
     * done by continually taking the maximum of the LSNs of the next log record to be 
     * processed for each of the yet-to-be-completely-undone loser transactions, until
     * no loser transaction remains to be undone. 
     * Note that during this pass the dirty_pages table is not consulted. Also, since
     * history is repeated before the undo pass is initiated, the LSN on the page is
     * not consulted to determine whether an undo operation should be performed or not.
     * <p>
     * Latching issues:<br>
     * None because trxmgr is already latched.
     */
    private void restartUndo() {

        if (log.isDebugEnabled()) {
            log.debug(
                this.getClass().getName(),
                "restartUndo",
                "SIMPLEDBM-DEBUG: Starting undo phase");
        }

        Lsn undoLsn = getMaximumUnpreparedUndoLsn();
        while (!undoLsn.isNull()) {
            LogRecord logrec = logmgr.read(undoLsn);
            Loggable loggable = loggableFactory.getInstance(logrec);
            TransactionImpl trx = findTransaction(loggable.getTrxId());

            if (loggable instanceof Compensation) {
                trx.undoNextLsn = ((Compensation) loggable).getUndoNextLsn();
            }

            else if (loggable instanceof TrxAbort
                    || loggable instanceof TrxPrepare) {
                trx.undoNextLsn = loggable.getPrevTrxLsn();
            }

            else if (loggable instanceof Redoable /* or undoable */) {

                if (loggable instanceof Undoable) {
                    if (log.isTraceEnabled()) {
                        log.trace(
                            this.getClass().getName(),
                            "restartUndo",
                            "SIMPLEDBM-TRACE: Undoing effects of log record "
                                    + loggable);
                    }
                    TransactionalModule module = moduleRegistry
                        .getModule(loggable.getModuleId());
                    if (loggable instanceof LogicalUndo) {
                        trx.performLogicalUndo(module, (Undoable) loggable);
                    } else {
                        trx.performPhysicalUndo(module, (Undoable) loggable);
                    }
                }
                trx.undoNextLsn = loggable.getPrevTrxLsn();

                if (loggable.getPrevTrxLsn().isNull()) {
                    trx.endTransaction();
                    deleteTransaction(trx);
                }
            }

            undoLsn = getMaximumUnpreparedUndoLsn();
        }
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.tm.TrxMgr#start()
     */
    public final void start() {
        doRestart();
        checkpointWriter = new Thread(
            new CheckpointWriter(this),
            "CheckpointWriter");
        checkpointWriter.start();
        log.info(this.getClass().getName(), "start", mcat.getMessage("IX0013"));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.simpledbm.rss.tm.TrxMgr#shutdown()
     */
    public final void shutdown() {
        stop = true;
        /*
         * By writing out the buffers prior to checkpoint we can cust down restart recovery time.
         */
        bufmgr.writeBuffers();
        while (checkpointWriter.isAlive()) {
            LockSupport.unpark(checkpointWriter);
            try {
                checkpointWriter.join();
            } catch (InterruptedException e) {
                log.error(this.getClass().getName(), "shutdown", mcat
                    .getMessage("EX0014"), e);
            }
        }
        log.info(this.getClass().getName(), "shutdown", mcat
            .getMessage("IX0015"));
    }

    /**
     * Orchestrate the Transaction Manager restart processing.
     * <p>
     * Latching issues: Not latched as it causes deadlock due to lack of reentrancy in current
     * implementation of Latch. When this is fixed, it would be desirable to latch exclusively.
     */
    private final void doRestart() {

        Lsn checkpointLsn = logmgr.getCheckpointLsn();
        if (checkpointLsn.isNull()) {
            if (log.isDebugEnabled()) {
                log
                    .debug(
                        this.getClass().getName(),
                        "restart",
                        "SIMPLEDBM-DEBUG: Restart processing skipped because there are no log records to process");
            }
            writeCheckpoint();
            return;
        }

        if (log.isDebugEnabled()) {
            log.debug(
                this.getClass().getName(),
                "restart",
                "SIMPLEDBM-DEBUG: Starting restart processing");
        }

        restartPendingActions = new LinkedList<PostCommitAction>();

        restartAnalysis();
        reopenActiveContainers();
        restartRedo();
        /* 
         * ARIES: At this point we are supposed to do following:
         * buffer pool Dirty_pages table := Dirty_Pages
         * remove entries for non-buffer-resident pages from the buffer pool Dirty_Pages table
         */
        bufmgr.updateRecoveryLsns(dirtyPages.toArray(new DirtyPageInfo[0]));
        dirtyPages = null;

        restartUndo();

        /*
         * TODO:
         * ARIES: re-acquire locks for prepared transactions (only if supporting distributed trx)
         */
        writeCheckpoint();

        /*
         * Since we do not support distributed transactions.
         */
        trxTable.clear();

        restartPendingActions.clear();
        restartPendingActions = null;
    }

    /**
     * Implements the Transaction interface. Transactions are meant to be
     * accessed by a single thread only; if multiple threads access the same
     * transaction object, results are undefined.
     */
    public static final class TransactionImpl implements Transaction, Storable,
            Dumpable {

		private TransactionManagerImpl trxmgr;

        public static final int SIZE = Lsn.SIZE * 3 + TransactionId.SIZE
                + TypeSize.BYTE * 2;

        /**
         * transaction id
         */
        TransactionId trxId;

        /**
         * status of the transaction (unprepared, prepared) 
         */
        TrxState state = TrxState.TRX_UNPREPARED;

        /**
         * lsn of first log record written by this transaction
         */
        Lsn firstLsn = new Lsn();

        /**
         * lsn of most recent log record written by transaction
         */
        Lsn lastLsn = new Lsn();

        /**
         * lsn of the next log record that should be processed 
         * during rollback.
         */
        Lsn undoNextLsn = new Lsn();

        /**
         * List of locks held by the transaction. Locks are always added
         * at the top of the list to facilitate releasing in reverse order.
         */
        final ArrayList<Lockable> locks = new ArrayList<Lockable>();

        /**
         * Each lock is assigned a number - this is used to determine which
         * locks can be released during rollbacks to savepoints.
         */
        int lockPos = -1;

        /**
         * List of post commit actions.
         */
        final LinkedList<PostCommitAction> postCommitActions = new LinkedList<PostCommitAction>();

        /**
         * Each post commit action is given a number - this helps to identify which
         * actions have been completed. It also can be used to identify the actions that
         * should be discarded upon rollback to a savepoint.
         */
        int actionId = 1;

        /**
         * Dummy CLR used for nested top actions.
         */
        DummyCLR dummyCLR;

        /**
         * Our isolation mode; for information only. It is client's responsibility
         * to handle differences in locking strategy based upon isolation mode.
         */
        IsolationMode isolationMode = IsolationMode.READ_COMMITTED;

        /**
         * Lock wait timeout in seconds
         */
        int lockTimeout = 60;

        /**
         * Reference to transaction manager's latch - used to coordinate
         * actions with the transaction manager.
         */
        LatchHelper trxMgrLatchRef;

        /**
         * Set of cursors associated with this transaction. Used to reset cusror
         * state after partial rollbacks.
         */
        HashSet<TransactionalCursor> cursorSet = new HashSet<TransactionalCursor>();

        public synchronized IsolationMode getIsolationMode() {
            return isolationMode;
        }

        /**
         * Record the most recent LSN for the transaction, and return the
         * previous transaction LSN. This function sets undo_next_lsn as well.
         * undo_next_lsn points to the next log record that should be undone.
         * For regular undo records, it is set to the record's lsn, but for CLRs,
         * it is set the lsn of the predecessor of the log record being compensated.
         * <p>
         * Latching issues:<br>
         * We do not acquire any latches because only current thread can modify
         * the trx structure.
         */
        private final Lsn registerLsn(Loggable logrec, Lsn newLsn) {
            logrec.setLsn(newLsn);
            lastLsn = newLsn;
            if (firstLsn.isNull()) {
                firstLsn = newLsn;
            }
            if (logrec instanceof Compensation) {
                this.undoNextLsn = ((Compensation) logrec).getUndoNextLsn();
            } else if (logrec instanceof Undoable) {
                this.undoNextLsn = newLsn;
            }
            if (log.isTraceEnabled()) {
                log.trace(
                    this.getClass().getName(),
                    "registerLsn",
                    "SIMPLEDBM-TRACE: Added log record " + logrec.getLsn()
                            + " to transaction " + this);
            }

            return logrec.getPrevTrxLsn();
        }

        /**
         * Schedule a post commit action. Post commit actions are used to deal with
         * actions that should be deferred until it is certain that the transaction 
         * is definitely committing. For example, dropping a Container. 
         * <p>
         * PostCommitActions need to be recorded in the Prepare log record.
         * <p>
         * Once the transaction is committed by writing the TrxEnd record, the
         * post commit actions must be performed. For each action which involves
         * deleting a Storage Container, appropriate redo-only logs must be generated.
         * These logs should not be associated with any particular transaction,
         * and care should be taken to ensure that a checkpoint is not in progress.
         */
        public synchronized final void schedulePostCommitAction(
                PostCommitAction action) {
            action.setActionId(actionId++);
            action.setTrxId(trxId);
            assert action.getModuleId() != -1;
            if (log.isTraceEnabled()) {
                log.trace(
                    this.getClass().getName(),
                    "schedulePostCommitAction",
                    "SIMPLEDBM-TRACE: Scheduling post commit action " + action
                            + " for transaction " + this);
            }
            postCommitActions.add(action);
        }

        /**
         * Insert a log record, and wrap any exceptions in TrxException.
         */
        public synchronized final Lsn logInsert(Page page, Loggable logrec) {
            return doLogInsert(page, logrec);
        }

        /**
         * Generate a transaction related log record. This function is meant to
         * be used by resource managers who need to generate logs. It takes care
         * of linking the new log record to the transaction.
         * <p>
         * Latching issues:</br> Since this function modifies the transaction's
         * persistent state, we need to latch the trxmgr so that we can avoid
         * conflicts with checkpoints.
         */
        private final Lsn doLogInsert(Page page, Loggable logrec) {
            if (logrec instanceof Redoable) {
                assert logrec.getModuleId() != -1;
            }
            logrec.setTrxId(trxId);
            logrec.setPrevTrxLsn(lastLsn);
            if (!(logrec instanceof DummyCLR)) {
                logrec.setPageId(page.getType(), page.getPageId());
            }
            trxMgrLatchRef.sharedLock();
            Lsn lsn = null;
            try {
                lsn = getTrxmgr().doLogInsert(logrec);
                registerLsn(logrec, lsn);
            } finally {
                trxMgrLatchRef.unlockShared();
            }
            return lsn;
        }

        /**
         * Creates a new Transaction object.
         */
        TransactionImpl(TransactionManagerImpl trxmgr, TransactionId trxId) {
            this.trxmgr = trxmgr;
            this.trxMgrLatchRef = new LatchHelper(trxmgr.latch);
            this.trxId = trxId;
            this.lockTimeout = trxmgr.getLockWaitTimeout();
        }

        /**
         * Creates a new Transaction object. Only to be used when
         * reading from a container.
         */
        TransactionImpl(TransactionManagerImpl trxmgr, ByteBuffer bb) {
        	this.trxmgr = trxmgr;
            this.trxMgrLatchRef = new LatchHelper(trxmgr.latch);
            this.lockTimeout = trxmgr.getLockWaitTimeout();
            trxId = new TransactionId(bb);
            firstLsn = new Lsn(bb);
            lastLsn = new Lsn(bb);
            undoNextLsn = new Lsn(bb);
            int ordinal = bb.get();
            if (TrxState.TRX_PREPARED.ordinal() == ordinal) {
                state = TrxState.TRX_PREPARED;
            } else if (TrxState.TRX_UNPREPARED.ordinal() == ordinal) {
                state = TrxState.TRX_UNPREPARED;
            } else {
                state = TrxState.TRX_DEAD;
            }
            ordinal = bb.get();
            if (IsolationMode.CURSOR_STABILITY.ordinal() == ordinal) {
                isolationMode = IsolationMode.CURSOR_STABILITY;
            } else if (IsolationMode.READ_COMMITTED.ordinal() == ordinal) {
                isolationMode = IsolationMode.READ_COMMITTED;
            } else {
                isolationMode = IsolationMode.SERIALIZABLE;
            }
        }

        public synchronized int getLockTimeout() {
            return lockTimeout;
        }

        public synchronized void setLockTimeout(int seconds) {
            this.lockTimeout = seconds;
        }

        public synchronized void registerTransactionalCursor(
                TransactionalCursor cursor) {
            synchronized (cursorSet) {
                cursorSet.add(cursor);
            }
        }

        public synchronized void unregisterTransactionalCursor(
                TransactionalCursor cursor) {
            synchronized (cursorSet) {
                cursorSet.remove(cursor);
            }
        }

        static class MyLockInfo implements LockInfo {

            LockMode mode;

            public boolean setHeldByOthers(boolean value) {
                return false;
            }

            public void setPreviousMode(LockMode mode) {
                this.mode = mode;
            }

            public LockMode getPreviousMode() {
                return mode;
            }

        }

        /**
         * Acquires a lock in requested mode on behalf of the transaction, 
         * and adds it to the transaction's list of locks. Note that the lock is added
         * only if it does not already exist within the list. This is because a second request
         * for the same lock is a conversion request.
         * <p>
         * When adding a new lock request, we assign a new lockPos value to the lock.
         * This is used when performing partial rollbacks, such as to a savepoint.
         * All locks that have an lockPos greater or equal to the Savepoint can be
         * released.
         * <p>
         * TODO: Detect deadlocks.
         * <p>
         * Latching issues:<br>
         * No latches acquired because there is no change to the persistent state of the 
         * transaction.
         */
        private void doAcquireLock(Lockable lockable, LockMode mode,
                LockDuration duration, int timeout) {
            MyLockInfo lockInfo = new MyLockInfo();
            getTrxmgr().lockmgr.acquire(
                this,
                lockable,
                mode,
                duration,
                timeout,
                lockInfo);
            if (log.isTraceEnabled()) {
                log.trace(
                    this.getClass().getName(),
                    "acquireLock",
                    "SIMPLEDBM-TRACE: Acquired lock on " + lockable
                            + " in mode " + mode + " for duration " + duration);
            }
            if (duration == LockDuration.INSTANT_DURATION) {
                return;
            }
            if (lockInfo.getPreviousMode() != null) {
                /*
                 * We held this lock previously so no need to add to our list.
                 */
                return;
            }
            /*
             * Each lock is given a unique position that can be used
             * to determine whether the lock should released during
             * rollback to a savepoint.
             * Lock conversions do not change the position of the lock in this table.
             */
            locks.add(++lockPos, lockable);
        }

        /**
         * @see #doAcquireLock(Lockable, LockMode, LockDuration, int)
         */
        public synchronized final void acquireLock(Lockable lockable,
                LockMode mode, LockDuration duration) {
            doAcquireLock(lockable, mode, duration, lockTimeout);
        }

        /**
         * Attempts to acquire a lock without waiting; if the lock is not
         * available, an exception will be thrown.
         * 
         * @see #doAcquireLock(Lockable, LockMode, LockDuration, int)
         */
        public synchronized final void acquireLockNowait(Lockable lockable,
                LockMode mode, LockDuration duration) {
            doAcquireLock(lockable, mode, duration, 0);
        }

        /**
         * Decrements a lock's reference count and if the reference count drops
         * to 0, the lock is released. This is meant to be used for situations
         * where a lock must be released prior to the commit, for example, in
         * cursor stability or read committed isolation modes.
         */
        public synchronized final boolean releaseLock(Lockable lockable) {
            return doReleaseLock(lockable);
        }

        /**
         * Decrements a lock's reference count and if the reference count drops
         * to 0, the lock is released. This is meant to be used for situations
         * where a lock must be released prior to the commit, for example, in
         * cursor stability or read committed isolation modes.
         * <p>
         * If the lock is released it is removed from the transaction's list of
         * locks.
         * <p>
         * Latching issues:<br>
         * No latches acquired because there is no change to the persistent
         * state of the transaction.
         * 
         */
        private final boolean doReleaseLock(Object lockable) {
            return trxmgr.lockmgr.release(this, lockable, false);
        }

        /* (non-Javadoc)
         * @see org.simpledbm.rss.tm.Transaction#downgradeLock(java.lang.Object, org.simpledbm.rss.locking.LockMode)
         */
        public synchronized final void downgradeLock(Lockable lockable,
                LockMode downgradeTo) {
            doDowngradeLock(lockable, downgradeTo);
        }

        /**
         * Downgrades a lock to specified mode. The typical use case for this is
         * when a cursor has placed an UPDATE lock on a record, and this needs
         * to be downgraded to SHARED lock.
         */
        public synchronized final void doDowngradeLock(Object lockable,
                LockMode downgradeTo) {
            trxmgr.lockmgr.downgrade(this, lockable, downgradeTo);
        }

        /* (non-Javadoc)
         * @see org.simpledbm.rss.tm.Transaction#hasLock(java.lang.Object)
         */
        public synchronized LockMode hasLock(Lockable lockable) {
            return trxmgr.lockmgr.findLock(this, lockable);
        }

        /**
         * Releases locks held by the transaction.
         * If a Savepoint is supplied, only those locks are released that were acquired subsequent to the
         * Savepoint. 
         * <p>
         * Latching issues:<br>
         * No latches required because there is no change to the persistent state of the transaction.
         */
        private final void releaseLocks(SavepointImpl sp) {
            if (log.isDebugEnabled()) {
                log.debug(
                    this.getClass().getName(),
                    "releaseLocks",
                    "SIMPLEDBM-DEBUG: Releasing locks for transaction " + this
                            + " upto savepoint " + sp);
            }
            for (int i = locks.size() - 1; i >= 0; i--) {
                /*
                 * If the lock has a lockPos greater than the Savepoint then it
                 * can be released.
                 */
                if (sp == null || i > sp.lockPos) {
                    Lockable lockable = locks.set(i, null);
                    if (lockable != null) {
                        if (log.isTraceEnabled()) {
                            log.trace(
                                this.getClass().getName(),
                                "releaseLock",
                                "SIMPLEBM-TRACE: Releasing lock " + lockable);
                        }
                        /*
                         * Even if rolling back to a savepoint we forcibly
                         * release locks because if lock was acquired after the
                         * savepoint, it is not required any more.
                         */
                        trxmgr.lockmgr.release(this, lockable, true);
                    }
                }
                if (sp != null && i <= sp.lockPos) {
                    break;
                }
            }
        }

        public synchronized final int countLocks() {
            int count = 0;
            for (int i = locks.size() - 1; i >= 0; i--) {
                Lockable lockable = locks.get(i);
                if (lockable == null) {
                    continue;
                }
                LockMode mode = hasLock(lockable);
                if (mode != LockMode.NONE) {
                    count++;
                }
            }
            return count;
        }

        /**
         * Discards PostCommitActions that were scheduled after
         * specified Savepoint.
         */
        private final void discardCommitActions(SavepointImpl sp) {
            if (log.isDebugEnabled()) {
                log.debug(
                    this.getClass().getName(),
                    "discardCommitActions",
                    "SIMPLEDBM-DEBUG: Discarding commit actions upto savepoint "
                            + sp);
            }
            Iterator<PostCommitAction> iter = postCommitActions.iterator();
            while (iter.hasNext()) {
                PostCommitAction action = iter.next();
                /*
                 * If the PostCommitAction has an actionId >=
                 * the Savepoint, then it can be discarded.
                 */
                if (action.getActionId() >= sp.actionId) {
                    if (log.isDebugEnabled()) {
                        log.debug(
                            this.getClass().getName(),
                            "discardCommitActions",
                            "SIMPLEDBM-DEBUG: Discarding commit action "
                                    + action);
                    }
                    iter.remove();
                }
            }
        }

        private void saveCursorStates(Savepoint sp) {
            synchronized (cursorSet) {
                for (TransactionalCursor cursor : cursorSet) {
                    cursor.saveState(sp);
                }
            }
        }

        private void restoreCursorStates(Savepoint sp) {
            synchronized (cursorSet) {
                for (TransactionalCursor cursor : cursorSet) {
                    cursor.restoreState(this, sp);
                }
            }
        }

        /**
         * Create a transaction savepoint.
         * <p>Latching issues:<br>
         * No latches required because only current thread may modify last_lsn.
         */
        public synchronized final SavepointImpl createSavepoint(
                boolean saveCursors) {
            SavepointImpl sp = new SavepointImpl(
                lastLsn,
                lockPos,
                actionId,
                saveCursors);
            if (saveCursors) {
                saveCursorStates(sp);
            }
            if (log.isDebugEnabled()) {
                log.debug(
                    this.getClass().getName(),
                    "createSavepoint",
                    "SIMPLEDBM-DEBUG: Created savepoint " + sp);
            }
            return sp;
        }

        /**
         * Prepare a transaction for commit. Note that since we do not support distributed transactions,
         * we do not record locks in the prepare record. We do however log any PostCommitActions
         * that have been scheduled.
         * <p>
         * Latching issues:<br>
         * Caller must S latch trxmgr in order to avoid conflicts with checkpoints.
         */
        private final void prepare() {
            if (log.isDebugEnabled()) {
                log.debug(
                    this.getClass().getName(),
                    "prepare",
                    "SIMPLEDBM-DEBUG: Preparing to commit transaction " + this);
            }
            assert state == TrxState.TRX_UNPREPARED;

            /*
             * Optimise for readonly transaction.
             * A readonly transaction would not have generated log records
             */
            if (!lastLsn.isNull() || !postCommitActions.isEmpty()) {
                TrxPrepare prepareLogRec = new TrxPrepare(0, TransactionManagerImpl.TYPE_TRXPREPARE);
                prepareLogRec.setTrxId(trxId);
                prepareLogRec.setPrevTrxLsn(lastLsn);
                prepareLogRec.setPostCommitActions(postCommitActions);
                /*
                 * FIXME Log all exclusive locks in the prepare record so that
                 * we can reacquire locks after recovery - only needed for
                 * distributed transactions.
                 */
                Lsn myLsn = getTrxmgr().doLogInsert(prepareLogRec);
                registerLsn(prepareLogRec, myLsn);
                getTrxmgr().logmgr.flush(myLsn);
            }
            state = TrxState.TRX_PREPARED;
            /*
             * At this point read locks can be released.
             */
        }

        /**
         * Log the end of a transaction.
         */
        private final void endTransaction() {
            /*
             * Optimise for readonly transaction.
             * A readonly transaction would not have generated log records
             */
            if (lastLsn.isNull() && postCommitActions.isEmpty()) {
                // No need to generate an end record
                return;
            }
            TrxEnd commitLogRec = new TrxEnd(0, TransactionManagerImpl.TYPE_TRXEND);
            commitLogRec.setTrxId(trxId);
            commitLogRec.setPrevTrxLsn(lastLsn);
            Lsn myLsn = getTrxmgr().doLogInsert(commitLogRec);
            registerLsn(commitLogRec, myLsn);
            getTrxmgr().logmgr.flush(myLsn);
        }

        /* (non-Javadoc)
         * @see org.simpledbm.rss.tm.Transaction#commit()
         */
        public synchronized final void commit() {
            doCommit();
        }

        /**
         * Commits a transaction. Commit processing involves following steps:
         * <ol>
         * <li>Prepare the transaction; log any PostCommitActions in the
         * prepare log record.</li>
         * <li>Write an end transaction record.</li>
         * <li>Perform PostCommitActions.</li>
         * <li>Release locks acquired by the transaction and mark transaction
         * as dead.</li>
         * </ol>
         * <p>
         * Latching issues: we S latch trxmgr so as to avoid conflicts with
         * checkpointing.
         */
        private final void doCommit() {

            if (log.isDebugEnabled()) {
                log.debug(
                    this.getClass().getName(),
                    "prepare",
                    "SIMPLEDBM-DEBUG: Committing transaction " + this);
            }
            trxMgrLatchRef.sharedLock();
            try {
                prepare();
                endTransaction();
                getTrxmgr().doPostCommitActions(postCommitActions, trxId);
                getTrxmgr().deleteTransaction(this);
            } finally {
                trxMgrLatchRef.unlockShared();
            }
        }

        /* (non-Javadoc)
         * @see org.simpledbm.rss.tm.Transaction#rollback(org.simpledbm.rss.tm.Savepoint)
         */
        public synchronized final void rollback(Savepoint savepoint) {
            doRollback(savepoint);
        }

        /**
         * Rollback a transaction upto a savepoint.
         * <p>
         * Latching issues:<br>
         * We acquire a shared latch on trxmgr to avoid conflicts with
         * checkpoints. Note that we cannot acquire exclusive latch here because
         * it would lead to a deadlock with the Buffer Manager. This is because,
         * normal WAL protocol requires FIX-DO-LOG-UNFIX sequence.
         */
        private final void doRollback(Savepoint savepoint) {
            assert savepoint != null;
            trxMgrLatchRef.sharedLock();
            try {
                doRollbackInternal(savepoint);
            } finally {
                trxMgrLatchRef.unlockShared();
            }
        }

        /**
         * Starts a Nested Top Action. Will throw an exception if a nested top action is already in scope.
         */
        public synchronized final void startNestedTopAction() {
            if (dummyCLR != null) {
                exceptionHandler.errorThrow(
                    this.getClass().getName(),
                    "startNestedTopAction",
                    new TransactionException(mcat.getMessage("EX0016")));
            }
            dummyCLR = new DummyCLR(
                    TransactionManagerImpl.MODULE_ID,
                    TransactionManagerImpl.TYPE_DUMMYCLR);
            dummyCLR.setUndoNextLsn(getLastLsn());
        }

        /**
         * Completes a nested top action. Will throw an exception if there isn't a nested top action in scope.
         */
        public synchronized final void completeNestedTopAction() {
            if (dummyCLR == null) {
                exceptionHandler.errorThrow(
                    this.getClass().getName(),
                    "startNestedTopAction",
                    new TransactionException(mcat.getMessage("EX0017")));
            }
            logInsert(null, dummyCLR);
            resetNestedTopAction();
        }

        /**
         * Abandons a nested top action.
         */
        public synchronized final void resetNestedTopAction() {
            dummyCLR = null;
        }

        /**
         * Performs page oriented physical undo, ie, the undo is performed on the
         * same page that was modified during normal operation.
         */
        private final void performPhysicalUndo(TransactionalModule module,
                Undoable undoable) {
            PageId pageId = undoable.getPageId();
            BufferAccessBlock bab = getTrxmgr().bufmgr.fixExclusive(
                pageId,
                false,
                -1,
                0);
            try {
                Page page = bab.getPage();
                Compensation clr = moduleGenerateCompensation(module, undoable);
                clr.setUndoNextLsn(undoable.getPrevTrxLsn());
                Lsn lsn = logInsert(page, clr);
                moduleRedo(module, page, clr);
                bab.setDirty(lsn);
            } finally {
                bab.unfix();
            }

        }

        /**
         * Performs logical undo. If it is known that the logical undo is page
         * orientated, ie, will affect only one page, then we follow steps similar to
         * physical undos, except that we allow the module to search for the page.
         * If this is not case, and the module may need to modify multiple pages
         * as part of undo, then we let the module handle all the logic.
         * <p>
         * An example of a single page logical undo would be key insert and delete
         * operations in a btree that uses logical key deletes. Because in such a
         * case, it is known that the undo of an delete will not cause a page split,
         * as the key will simply be be reinstated by removing the deleted mark.
         * <p>
         * An example of multiple page logic undo is a btree that does not use
         * logical key deletes.
         */
        private final void performLogicalUndo(TransactionalModule module,
                Undoable undoable) {
            if (undoable instanceof SinglePageLogicalUndo) {
                BufferAccessBlock bab = moduleFindAndFixPageForUndo(
                    module,
                    undoable);
                try {
                    Page page = bab.getPage();
                    Compensation clr = moduleGenerateCompensation(
                        module,
                        undoable);
                    clr.setUndoNextLsn(undoable.getPrevTrxLsn());
                    Lsn lsn = logInsert(page, clr);
                    moduleRedo(module, page, clr);
                    bab.setDirty(lsn);
                } finally {
                    bab.unfix();
                }
            } else {
                moduleUndo(module, this, undoable);
            }
        }

        /**
         * Rollback a transaction completely or upto a savepoint. To rollback completely supply null
         * as parameter.
         * <p>
         * Rollback starts at undoNextLsn. When a change is to be undone, we invoke the Module
         * that created the original log record. We also expect the Module to record "redo" 
         * information in a Compensation Log Record.
         * <p>
         * Latching issues:<br>
         * Caller must latch in shared mode.<br>
         * @see #doAbort()
         * @see #rollback(Savepoint)
         */
        private final void doRollbackInternal(Savepoint savepoint) {

            SavepointImpl sp = (SavepointImpl) savepoint;
            boolean delete = (sp == null);
            Lsn rollbackUpto = delete ? new Lsn() : sp.lsn;
            if (log.isDebugEnabled()) {
                if (delete) {
                    log.debug(
                        this.getClass().getName(),
                        "rollback",
                        "SIMPLEDBM-DEBUG: Rolling back transaction " + this
                                + " completely");
                } else {
                    log.debug(
                        this.getClass().getName(),
                        "rollback",
                        "SIMPLEDBM-DEBUG: Rolling back transaction " + this
                                + " upto " + sp);
                }
            }

            /* Undo starts at undoNextLsn */
            Lsn undoNext = undoNextLsn;
            while (undoNext.compareTo(rollbackUpto) > 0) {

                LogRecord logrec = getTrxmgr().logmgr.read(undoNext);
                Loggable loggable = getTrxmgr().loggableFactory
                    .getInstance(logrec);

                if (loggable instanceof Undoable) {

                    if (log.isDebugEnabled()) {
                        log.debug(
                            this.getClass().getName(),
                            "rollback",
                            "Undoing effects of log record " + loggable
                                    + " to transaction " + this);
                    }

                    /*
                     * Get the Module responsible for creating this log record, and
                     * ask it to undo the changes. The Module must generate appropriate Compensation
                     * Log records.
                     */
                    TransactionalModule module = getTrxmgr().moduleRegistry
                        .getModule(loggable.getModuleId());
                    if (loggable instanceof LogicalUndo) {
                        performLogicalUndo(module, (Undoable) loggable);
                    } else {
                        performPhysicalUndo(module, (Undoable) loggable);
                    }
                    undoNext = loggable.getPrevTrxLsn();
                }

                /* Check Compensation first as it is derived from Redoable */
                else if (loggable instanceof Compensation) {
                    /* a CLR - nothing to undo */
                    undoNext = ((Compensation) loggable).getUndoNextLsn();
                }

                else {
                    /* Skip record and go to previous one. */
                    undoNext = loggable.getPrevTrxLsn();
                }
            }

            /* Update the transaction */
            undoNextLsn = undoNext;

            if (!delete) {
                /*
                 * Optimise for readonly transactions.
                 */
                if (!lastLsn.isNull()) {
                    getTrxmgr().logmgr.flush(lastLsn);
                }
                releaseLocks(sp);
                /*
                 * Discard PostCommitActions that were scheduled after the
                 * Savepoint.
                 */
                discardCommitActions(sp);

                /*
                 * Restore cursors to their position at the time of savepoint
                 */
                if (sp.cursorsSaved()) {
                    restoreCursorStates(sp);
                }
            }
        }

        /* (non-Javadoc)
         * @see org.simpledbm.rss.tm.Transaction#abort()
         */
        public synchronized final void abort() {
            doAbort();
        }

        /**
         * Aborts the transaction. The intention to abort is recorded first via
         * the TrxAbort log record, changes are then rolled back, the
         * transaction end record written, the locks released, and finally the
         * transaction is marked dead. All the undo work is done in
         * {@link #doRollback(Savepoint)}.
         * <p>
         * Latching issues:<br>
         * We acquire a shared latch on trxmgr to avoid conflicts with
         * checkpoints. Note that we cannot acquire exclusive latch here because
         * it would lead to a deadlock with the Buffer Manager. This is because,
         * normal WAL protocol requires FIX-DO-LOG-UNFIX sequence.
         */
        private final void doAbort() {
            trxMgrLatchRef.sharedLock();
            try {
                if (log.isDebugEnabled()) {
                    log.debug(
                        this.getClass().getName(),
                        "abort",
                        "SIMPLEDBM-DEBUG: Aborting transaction " + this);
                }
                /*
                 * Optimise for readonly transactions
                 */
                if (!lastLsn.isNull() || !postCommitActions.isEmpty()) {
                    /*
                     * First write the Abort log record.
                     */
                    TrxAbort abortLogRec = new TrxAbort(0, TransactionManagerImpl.TYPE_TRXABORT);
                    
                    abortLogRec.setTrxId(trxId);
                    abortLogRec.setPrevTrxLsn(lastLsn);
                    Lsn myLsn = getTrxmgr().doLogInsert(abortLogRec);
                    registerLsn(abortLogRec, myLsn);
                    getTrxmgr().logmgr.flush(myLsn);
                }
                state = TrxState.TRX_UNPREPARED;
                /*
                 * Undo changes.
                 */
                doRollbackInternal(null);
                /*
                 * Write end transaction record.
                 */
                endTransaction();
                /*
                 * Release locks and mark the transaction dead.
                 */
                getTrxmgr().deleteTransaction(this);
            } finally {
                trxMgrLatchRef.unlockShared();
            }
        }

        /* (non-Javadoc)
         * @see org.simpledbm.rss.io.Storable#store(java.nio.ByteBuffer)
         */
        public final void store(ByteBuffer bb) {
            trxId.store(bb);
            firstLsn.store(bb);
            lastLsn.store(bb);
            undoNextLsn.store(bb);
            bb.put((byte) state.ordinal());
            bb.put((byte) isolationMode.ordinal());
        }

        /* (non-Javadoc)
         * @see org.simpledbm.rss.io.Storable#getStoredLength()
         */
        public final int getStoredLength() {
            return SIZE;
        }

        /* (non-Javadoc)
         * @see org.simpledbm.rss.tm.Transaction#getLastLsn()
         */
        public synchronized final Lsn getLastLsn() {
            return new Lsn(lastLsn);
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("Transaction(trxId=");
            trxId.appendTo(sb).append(", firstLsn");
            firstLsn.appendTo(sb).append(", lastLsn=");
            lastLsn.appendTo(sb).append(", undoNextLsn=");
            undoNextLsn.appendTo(sb).append(", state=").append(state).append(
                ", isolationMode=").append(isolationMode).append(")");
            return sb;
        }

        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        private TransactionManagerImpl getTrxmgr() {
            return trxmgr;
        }

        @Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (!super.equals(obj))
				return false;
			if (getClass() != obj.getClass())
				return false;
			final TransactionImpl other = (TransactionImpl) obj;
			if (trxId == null) {
				if (other.trxId != null)
					return false;
			} else if (!trxId.equals(other.trxId))
				return false;
			return true;
		}

		@Override
		public int hashCode() {
			final int PRIME = 31;
			int result = super.hashCode();
			result = PRIME * result + ((trxId == null) ? 0 : trxId.hashCode());
			return result;
		}
    }

    /**
     * The SavepointImpl records the state of the transaction at the
     * time it is created. It records three things:
     * <ol>
     * <li>LSN - this marks the point beyond which changes can be
     * undone.</li>
     * <li>lockPos - marks the position in the lock table 
     * beynd which locks can be released.</li>
     * <li>actionId - marks the position of the PostCommitActions 
     * table beyond which actions can be discarded.</li>
     * </ol>
     */
    static final class SavepointImpl implements Savepoint, Dumpable {
        final Lsn lsn;
        final int lockPos;
        final int actionId;
        final boolean saveCursors;

        final Hashtable<Object, Object> savedValues = new Hashtable<Object, Object>();

        SavepointImpl(Lsn lsn, int lockPos, int actionId, boolean saveCursors) {
            this.lsn = lsn;
            this.lockPos = lockPos;
            this.actionId = actionId;
            this.saveCursors = saveCursors;
        }

        public Object getValue(Object key) {
            return savedValues.get(key);
        }

        public void saveValue(Object key, Object value) {
            if (!saveCursors) {
                return;
            }
            savedValues.put(key, value);
        }

        public boolean cursorsSaved() {
            return saveCursors;
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("Savepoint(lsn=");
            lsn.appendTo(sb).append(", lockPos=").append(lockPos).append(
                ", actionId=").append(actionId);
            sb.append(", saveCursors=").append(saveCursors).append(")");
            return sb;
        }

        @Override
        public final String toString() {
            return appendTo(new StringBuilder()).toString();
        }
    }

    /**
     * Information regarding an open container that will be logged in the
     * transaction's prepare record.
     * @see TrxPrepare
     */
    static final class ActiveContainerInfo implements Storable {

        private final ByteString name;

        private final int containerId;

        ActiveContainerInfo(String name, int containerId) {
            this.name = new ByteString(name);
            this.containerId = containerId;
        }

        public ActiveContainerInfo(ByteBuffer bb) {
        	name = new ByteString(bb);
            containerId = bb.getInt();
        }

        public void store(ByteBuffer bb) {
            assert name != null;
            name.store(bb);
            bb.putInt(containerId);
        }

        public int getStoredLength() {
            return name.getStoredLength() + TypeSize.INTEGER;
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb
                .append("ActiveContainerInfo(name=")
                .append(name.toString())
                .append(", id=")
                .append(containerId)
                .append(")");
            return sb;
        }

        @Override
        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
    }

    static abstract class Checkpoint extends BaseLoggable {

		protected Checkpoint(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		protected Checkpoint(ByteBuffer bb) {
			super(bb);
		}
    }

    /**
     * CheckpointBegin log record marks the start of a checkpoint.
     * A list of open containers is recorded in this record.
     */
    static public final class CheckpointBegin extends Checkpoint {

        ActiveContainerInfo[] activeContainers = new ActiveContainerInfo[0];

		public CheckpointBegin(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public CheckpointBegin(ByteBuffer bb) {
			super(bb);
            short n = bb.getShort();
            activeContainers = new ActiveContainerInfo[n];
            for (int i = 0; i < n; i++) {
              activeContainers[i] = new ActiveContainerInfo(bb);
            }
		}

		public void setActiveContainers(StorageContainerInfo[] storageContainers) {
            this.activeContainers = new ActiveContainerInfo[storageContainers.length];
            for (int i = 0; i < storageContainers.length; i++) {
                this.activeContainers[i] = new ActiveContainerInfo(
                    storageContainers[i].getName(),
                    storageContainers[i].getContainerId());
            }
        }

        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            bb.putShort((short) activeContainers.length);
            for (ActiveContainerInfo aci : activeContainers) {
                aci.store(bb);
            }
        }

        @Override
        public int getStoredLength() {

            int retValue;

            retValue = super.getStoredLength();
            retValue += TypeSize.SHORT;
            for (ActiveContainerInfo aci : activeContainers) {
                retValue += aci.getStoredLength();
            }
            return retValue;
        }

        public ActiveContainerInfo[] getActiveContainers() {
            return activeContainers;
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("CheckpointBegin(");
            super.appendTo(sb);
            sb.append(", activeContainers={");
            for (ActiveContainerInfo aci : activeContainers) {
                aci.appendTo(sb).append(",");
            }
            sb.append("})");
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class CheckpointBeginFactory implements ObjectFactory {

			public Class<?> getType() {
				return CheckpointBegin.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new CheckpointBegin(buf);
			}
        	
        }
        
    }

    /**
     * CheckpointEnd record contains a list of dirty pages and the
     * transaction table.
     * 
     * @author Dibyendu Majumdar
     * @since 25-Aug-2005
     */
    static public final class CheckpointEnd extends Checkpoint {

        DirtyPageInfo[] dirtyPages;
        ArrayList<DirtyPageInfo> newDirtyPages;
        LinkedList<TransactionImpl> trxTable;
        int n_trx = 0;
        TransactionManagerImpl trxmgr;

        public CheckpointEnd(int moduleId, int typeCode, TransactionManagerImpl trxmgr) {
			super(moduleId, typeCode);
			this.trxmgr = trxmgr;
		}

		public CheckpointEnd(TransactionManagerImpl trxmgr, ByteBuffer bb) {
			super(bb);
			this.trxmgr = trxmgr;
            int n_trx = bb.getInt();
            trxTable = new LinkedList<TransactionImpl>();
            for (int i = 0; i < n_trx; i++) {
                TransactionImpl trx = new TransactionImpl(trxmgr, bb);
                trxTable.add(trx);
            }
            int n_dp = bb.getInt();
            newDirtyPages = new ArrayList<DirtyPageInfo>(n_dp);
            for (int i = 0; i < n_dp; i++) {
                DirtyPageInfo dp = new DirtyPageInfo(bb);
                newDirtyPages.add(dp);
            }
		}

        final void setDirtyPageList(DirtyPageInfo[] dirtyPages) {
            this.dirtyPages = dirtyPages;
        }

        final void setTransactionTable(LinkedList<TransactionImpl> trxTable) {
            this.trxTable = trxTable;
        }

        @Override
        public final int getStoredLength() {
            int size = 0;
            for (TransactionImpl trx : trxTable) {
                if (trx.state == TrxState.TRX_DEAD) {
                    continue;
                }
                n_trx++;
                size += trx.getStoredLength();
            }

            for (DirtyPageInfo dp : dirtyPages) {
                size += dp.getStoredLength();
            }

            size += TypeSize.INTEGER * 2;
            size += super.getStoredLength();
            return size;
        }


        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            int n_trx = 0;
            for (TransactionImpl trx : trxTable) {
                if (trx.state == TrxState.TRX_DEAD) {
                    continue;
                }
                n_trx++;
            }
            bb.putInt(n_trx);
            for (TransactionImpl trx : trxTable) {
                if (trx.state == TrxState.TRX_DEAD) {
                    continue;
                }
                trx.store(bb);
            }
            bb.putInt(dirtyPages.length);
            for (DirtyPageInfo dp : dirtyPages) {
                dp.store(bb);
            }
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("CheckpointEnd(");
            super.appendTo(sb);
            sb.append(", trxTable={");
            for (TransactionImpl trx : trxTable) {
                if (trx.state == TrxState.TRX_DEAD) {
                    continue;
                }
                trx.appendTo(sb).append(',');
            }
            sb.append("}, dirtyPages={");
            for (DirtyPageInfo dp : dirtyPages) {
                sb.append(dp).append(',');
            }
            sb.append("})");
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
        static final class CheckpointEndFactory implements ObjectFactory {
        	private final TransactionManagerImpl trxmgr;
        	CheckpointEndFactory(TransactionManagerImpl trxmgr) {
        		this.trxmgr = trxmgr;
        	}
			public Class<?> getType() {
				return CheckpointEnd.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new CheckpointEnd(trxmgr, buf);
			}
        }
    }

    static abstract class TrxControl extends BaseLoggable {
		protected TrxControl(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}
		protected TrxControl(ByteBuffer bb) {
			super(bb);
		}
    }

    /**
     * TrxPrepare record is used to log that a Transaction has reached
     * prepared state. PostCommitActions are recorded in the prepare log record.
     * If distributed transactions are to be supported, then the lock table should
     * also be recorded here.
     */
    static public final class TrxPrepare extends TrxControl {

        LinkedList<PostCommitAction> postCommitActions = new LinkedList<PostCommitAction>();

          public TrxPrepare(int moduleId, int typeCode) {
        	super(moduleId, typeCode);
        }
        
        public TrxPrepare(LoggableFactory loggableFactory, ByteBuffer bb) {
        	super(bb);
            int n = bb.getInt();
            postCommitActions = new LinkedList<PostCommitAction>();
            for (int i = 0; i < n; i++) {
                PostCommitAction action = (PostCommitAction) loggableFactory
                    .getInstance(bb);
                postCommitActions.add(action);
            }
        }
        
        public final LinkedList<PostCommitAction> getPostCommitActions() {
            return postCommitActions;
        }

        public final void setPostCommitActions(
                LinkedList<PostCommitAction> postCommitActions) {
            this.postCommitActions = postCommitActions;
        }

        @Override
        public final int getStoredLength() {
            int size = super.getStoredLength();
            for (PostCommitAction action : postCommitActions) {
                size += action.getStoredLength();
            }
            size += TypeSize.INTEGER;
            return size;
        }

        @Override
        public final void store(ByteBuffer bb) {
            super.store(bb);
            int n = postCommitActions.size();
            bb.putInt(n);
            for (PostCommitAction action : postCommitActions) {
                action.store(bb);
            }
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("TrxPrepare(");
            super.appendTo(sb);
            sb.append(", postCommitActions={");
            for (PostCommitAction action : postCommitActions) {
                if (action instanceof Dumpable) {
                    ((Dumpable) action).appendTo(sb);
                } else {
                    sb.append(action);
                }
                sb.append(',');
            }
            sb.append("})");
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class TrxPrepareFactory implements ObjectFactory {

        	private final LoggableFactory loggableFactory;
        	TrxPrepareFactory(LoggableFactory loggableFactory) {
        		this.loggableFactory = loggableFactory;
        	}
        	
			public Class<?> getType() {
				return TrxPrepare.class;
			}

			public Object newInstance(ByteBuffer buf) {
				return new TrxPrepare(loggableFactory, buf);
			}
        	
        }
        
    }

    static public final class TrxAbort extends TrxControl {

    	public TrxAbort(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public TrxAbort(ByteBuffer bb) {
			super(bb);
		}

		public StringBuilder appendTo(StringBuilder sb) {
            sb.append("TrxAbort(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class TrxAbortFactory implements ObjectFactory {
			public Class<?> getType() {
				return TrxAbort.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new TrxAbort(buf);
			}
        }
    }

    static public final class TrxEnd extends TrxControl {

        public TrxEnd(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public TrxEnd(ByteBuffer bb) {
			super(bb);
		}

		public StringBuilder appendTo(StringBuilder sb) {
            sb.append("TrxEnd(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }

        static final class TrxEndFactory implements ObjectFactory {
			public Class<?> getType() {
				return TrxEnd.class;
			}
			public Object newInstance(ByteBuffer buf) {
				return new TrxEnd(buf);
			}
        }
    }

    /**
     * DummyCLRs are used to create Nested Top Actions. There are times when
     * certain updates of a transaction should be committed, irrespective of whether
     * the transaction subsequently commits or aborts. However, these actions must
     * still be atomic. A Nested Top Action supports this requirement without
     * having to initiate independent transactions to perform such actions. A nested
     * top action is created by the following steps: 
     * <ol>
     * <li>Save the transaction's last log LSN as savedLSN.</li>
     * <li>Fix exclusively the page that is to be updated.</li> 
     * <li>Generate the redo/undo log record for the page update.</li>
     * <li>Set pageLsn = LSN of the log record above.</li>
     * <li>If this is a new page, then externalize changes to the page affected.</li>
     * <li>Unfix the page.</li>
     * <li>Repeat above steps for any other pages affected.</li>
     * <li>Initialize a DummyCLR record, and set its undoNextLsn to savedLSN.</li>
     * <li>Log the dummyCLR.</li>
     * </ol>
     * <p>
     * Using the nested top action approach, if the enclosing transaction were to rollback after
     * the completion of the nested top action, then the dummy CLR will ensure that
     * the updates performed as part of the nested top action are not undone. However,
     * if a system failure were to occur before the dummy CLR is written, then the 
     * incomplete nested top-action will be undone since the nested top action's log
     * records are written as undo-redo as opposed to redo only. This provides the
     * desired atomic property of nested top actions.
     * <p>
     * Of course, if the nested top action can be expressed as a single redo only 
     * log record, then that is preferable. Nested top actions are useful when the
     * action requires more than one log record, but must retain its atomic property.
     * <p>IMPORTANT: Note that the pageId of a DummyCLR must be NULL - this is
     * true by default, so don't change the pageId for a DummyCLR! 
     * 
     * @author Dibyendu Majumdar
     * @since 27-Aug-2005
     */
    public static final class DummyCLR extends BaseLoggable implements
            Compensation {

        public DummyCLR(int moduleId, int typeCode) {
			super(moduleId, typeCode);
		}

		public DummyCLR(ByteBuffer bb) {
			super(bb);
		}

		public StringBuilder appendTo(StringBuilder sb) {
            sb.append("DummyCLR(");
            super.appendTo(sb);
            sb.append(")");
            return sb;
        }

        public String toString() {
            return appendTo(new StringBuilder()).toString();
        }
        
        static final class DummyCLRFactory implements ObjectFactory {
			public Class<?> getType() {
				return DummyCLR.class;
			}
//			public Object newInstance() {
//				return new DummyCLR();
//			}
			public Object newInstance(ByteBuffer buf) {
				return new DummyCLR(buf);
			}
        }
    }

    static void moduleRedo(TransactionalModule module, Loggable loggable) {
        module.redo(loggable);
    }

    static void moduleUndo(TransactionalModule module, Transaction trx,
            Undoable loggable) {
        module.undo(trx, loggable);
    }

    static void moduleRedo(TransactionalModule module, Page page,
            Redoable loggable) {
        module.redo(page, loggable);
    }

    static Compensation moduleGenerateCompensation(TransactionalModule module,
            Undoable loggable) {
        return module.generateCompensation(loggable);
    }

    static BufferAccessBlock moduleFindAndFixPageForUndo(
            TransactionalModule module, Undoable loggable) {
        return module.findAndFixPageForUndo(loggable);
    }

    /**
     * @param activeContainers The activeContainers to set.
     */
    private void setActiveContainers(ActiveContainerInfo[] activeContainers) {
        this.activeContainers = activeContainers;
    }

    /**
     * @return Returns the activeContainers.
     */
    private ActiveContainerInfo[] getActiveContainers() {
        return activeContainers;
    }

    /**
     * Implements the background task for generating Checkpoints
     * at regular intervals.
     */
    public static class CheckpointWriter implements Runnable {

        TransactionManagerImpl trxmgr;

        public CheckpointWriter(TransactionManagerImpl trxmgr) {
            this.trxmgr = trxmgr;
        }

        public void run() {

            for (;;) {
                long then = System.nanoTime();
                long timeout = TimeUnit.NANOSECONDS.convert(
                    trxmgr.checkpointInterval,
                    TimeUnit.MILLISECONDS);
                while (timeout > 0) {
                    LockSupport.parkNanos(timeout);
                    long now = System.nanoTime();
                    timeout -= (now - then);
                    then = now;
                    if (timeout <= 0 || trxmgr.stop) {
                        break;
                    }
                }
                try {
                    trxmgr.checkpoint();
                } catch (TransactionException e) {
                    log.error(CheckpointWriter.class.getName(), "run", mcat
                        .getMessage("EX0018"), e);
                    trxmgr.errored = true;
                    break;
                }
                if (trxmgr.stop) {
                    break;
                }
            }
        }
    }

}
