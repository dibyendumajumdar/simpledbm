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

package org.simpledbm.rss.impl.wal;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.simpledbm.rss.api.st.Storable;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageException;
import org.simpledbm.rss.api.wal.LogException;
import org.simpledbm.rss.api.wal.LogManager;
import org.simpledbm.rss.api.wal.LogReader;
import org.simpledbm.rss.api.wal.LogRecord;
import org.simpledbm.rss.api.wal.Lsn;
import org.simpledbm.rss.util.ByteString;
import org.simpledbm.rss.util.ChecksumCalculator;
import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;

/**
 * The default LogMgr implementation.
 * <p>
 * This implementation stores control information about the Log separately from
 * log files. For safety, multiple copies of control information are stored
 * (though at present, only the first control file is used when opening the
 * Log).
 * <p>
 * Logically, the Log is organized as a never ending sequence of log records.
 * Physically, the Log is split up into log files. There is a fixed set of
 * <em>online</em> log files, and a dynamic set of <em>archived</em> log
 * files. The set of online log files is called a Log Group.
 * <p>
 * Each Log Group consists of a set of pre-allocated log files of the same size.
 * The maximum number of groups possible is defined in {@link #MAX_LOG_GROUPS},
 * and the maximum number of log files within a group is defined in
 * {@link #MAX_LOG_FILES}. Note that each group is a complete set in itself -
 * the Log is recoverable if any one of the groups is available, and if the
 * archived log files are available. If more than one group is created, it is
 * expected that each group will reside on a different disk sub-system.
 * <p>
 * The Log Groups are allocated when the Log is initially created. The log files
 * within a group are also pre-allocated. However, the content of the online log
 * files changes over time.
 * <p>
 * Logically, just as the Log can be viewed as a sequence of Log Records, it can
 * also be thought of as a sequence of Log Files. The Log Files are numbered in
 * sequence, starting from 1. The Log File sequence number is called
 * <em>LogIndex</em>. At any point in time, the physical set of online log
 * files will contain a set of logical log files. For example, if there are 3
 * physical files in a Log Group, then at startup, the set of logical log files
 * would be 1, 2 and 3. After some time, the log file 1 would get archived, and
 * in its place a new logical log file 4 would be created. The set now would now
 * consist of logical log files 2, 3 and 4.
 * <p>
 * When a log record is written to disk, it is written out to an online log
 * file. If there is more than one group, then the log record is written to each
 * of the groups. The writes happen in sequence to ensure that if there is a
 * write failure, damage is restricted to one Log Group. Note that due to the
 * way this works, having more than 1 group will slow down log writes. It is
 * preferable to use hardware based disk mirroring of log files as opposed to
 * using multiple log groups.
 * <p>
 * When new log records are created, they are initially stored in the log
 * buffers. Log records are written out to log files either because of a client
 * request to flush the log, or because of the periodic flush event.
 * <p>
 * During a flush, the system determines which log file to use. There is the
 * notion of <em>Current</em> log file, which is where writes are expected to
 * occur. If the current log file is full, it is put into a queue for archiving,
 * and the log file is <em>switched</em>. Until an online log file has been
 * archived, its physical file cannot be reused. A separate archive thread
 * monitors archive requests and archives log files in the background.
 * <p>
 * Only one flush is permitted to execute at any point in time. Similarly, only
 * one archive is permitted to execute at any point in time. However, multiple
 * clients are allowed to concurrently insert and read log records, even while
 * flushing and archiving is going on, except under following circumstances.
 * <ol>
 * <li>Log inserts cannot proceed if the system has used up more memory than it
 * should. In that case, it must wait for some memory to be freed up. To ensure
 * maximum concurrency, the memory calculation is only approximate.</li>
 * <li>A Log flush cannot proceed if all the online log files are full. In this
 * situation, the flush must wait for at least one file to be archived. </li>
 * <li>When reading a log record, if the online log file containing the record
 * is being archived, the reader may have to wait for the status of the log file
 * to change, before proceeding with the read. Conversely, if a read is active,
 * the archive thread must wait for the read to be over before changing the
 * status of the log file.</li>
 * </ol>
 * <p>
 * If archive mode is ON, log files are archived before being re-used.
 * Otherwise, they can be reused if the file is no longer needed - however this
 * is currently not implemented. By default archive mode is ON.
 * <h3>Limitations of current design</h3>
 * <ol>
 * <li>A Log record cannot span log files, and it must fit within a single log
 * buffer. Thus the size of a log record is limited by the size of a log buffer
 * and by the size of a log file. As a workaround to this limitation, clients
 * can split the data into multiple log records, but in that case, clients are
 * responsible for merging the data back when reading from the Log. </li>
 * </ol>
 * <h3>Known issues</h3>
 * <p>The StorageContainerFactory instance needs to be part of the 
 * constructor interface.
 * </p>
 * 
 * @author Dibyendu Majumdar
 * @since 14 June 2005
 * @see LogAnchor
 * @see #readLogAnchor
 * @see LogFileHeader
 * @see #LOGREC_HEADER_SIZE
 * @see #MAX_CTL_FILES
 * @see #MAX_LOG_GROUPS
 * @see #MAX_LOG_FILES
 * 
 */
public final class LogManagerImpl implements LogManager {

    private static final String LOG_CLASS_NAME = LogManagerImpl.class.getName();

    static final Logger logger = Logger.getLogger(LogManagerImpl.class.getPackage().getName());

    static final String DEFAULT_GROUP_PATH = ".";

    static final String DEFAULT_ARCHIVE_PATH = ".";

    static final int DEFAULT_LOG_FILES = 2;

    static final int DEFAULT_LOG_GROUPS = 1;

    static final int DEFAULT_CTL_FILES = 2;

    /**
     * A valid Log Group has status set to {@value}.
     */
    private static final int LOG_GROUP_OK = 1;

    /**
     * An invalid Log Group has its status set to {@value}.
     */
    static final int LOG_GROUP_INVALID = 2;

    /**
     * Initially a Log File is marked as unused, and its status is set to
     * {@value}. A Log File also goes into this status once it has been
     * archived.
     */
    private static final int LOG_FILE_UNUSED = 0;

    /**
     * The Log File currently being flushed has its status set to {@value}.
     */
    private static final int LOG_FILE_CURRENT = 1;

    /**
     * Once the log file is full, it status changes to {@value}, and it becomes
     * ready for archiving. An archive request is sent to the archive thread.
     */
    private static final int LOG_FILE_FULL = 2;

    /**
     * If a Log File becomes corrupt, its status is set to {@value}.
     */
    static final int LOG_FILE_INVALID = -1;

    /**
     * The maximum number of control files allowed is {@value}.
     */
    static final int MAX_CTL_FILES = 3;

    /**
     * The maximum number of Log Groups allowed is {@value}.
     * 
     * @see #LOG_GROUP_IDS
     */
    static final int MAX_LOG_GROUPS = 3;

    /**
     * The maximum number of log files in a group is {@value}.
     */
    private static final int MAX_LOG_FILES = 8;

    static final int MAX_LOG_BUFFERS = 2;

    static final int DEFAULT_LOG_BUFFER_SIZE = 2 * 1024; // * 1024;

    static final int DEFAULT_LOG_FILE_SIZE = 2 * 1024; // * 1024;

    /**
     * Each Log Group has a single character ID. The ID stored in the file
     * header of all log files belonging to a group.
     * 
     * @see LogFileHeader#id
     */
    private static final char[] LOG_GROUP_IDS = { 'a', 'b', 'c' };

    /**
     * The first Log File number is 1, because 0 is reserved for identifying
     * Null value; and the first offset in a file is immediately after the log
     * file header record.
     */
    static final Lsn FIRST_LSN = new Lsn(1, LogFileHeader.SIZE);

    /**
     * A LogRecord contains a header portion, followed by the data supplied by
     * the user, followed by a trailer. The structure of the header portion is
     * as follows:
     * <ol>
     * <li> length - 4 byte length of the log record </li>
     * <li> lsn - 8 bytes containing lsn of the log record </li>
     * <li> prevLsn - 8 bytes containing lsn of previous log record </li>
     * </ol>
     * The header is followed by the data. Note that EOF records do not have any
     * data.
     * <ol>
     * <li> data - byte[length - (length of header fields)] </li>
     * </ol>
     * Data is followed by a trailer containing a checksum. Checksum is
     * calculated on header and data fields.
     * <ol>
     * <li> checksum - 8 bytes </li>
     * </ol>
     * The minimum length of a Log Record is {@value}.
     */
    private static final int LOGREC_HEADER_SIZE = TypeSize.INTEGER + // length
            Lsn.SIZE + // lsn
            Lsn.SIZE + // prevLsn
            TypeSize.LONG; // checksum

    /**
     * An EOF Record is simply one that has no data. EOF Records are used to
     * mark the logical end of a Log file. They are skipped during log scans.
     */
    private static final int EOF_LOGREC_SIZE = LOGREC_HEADER_SIZE;

    /**
     * Currently active buffer, access to this is protected via
     * {@link #bufferLock}.
     */
    private LogBuffer currentBuffer;

    /**
     * List of log buffers; the active buffer is always at the tail of this
     * list. Access to this is protected via {@link #bufferLock}.
     */
    private final LinkedList<LogBuffer> logBuffers;

    /**
     * Holds control information about the Log. Access protected via
     * {@link #anchorLock} and {@link #anchorWriteLock}.
     */
    LogAnchor anchor;

    /**
     * Handles to open log files.
     */
    private final StorageContainer[][] files;

    /**
     * Handles to open control files.
     */
    private final StorageContainer[] ctlFiles;

    /**
     * Flag to indicate that the log is open.
     */
    private volatile boolean started;

    /**
     * Flag to indicate that the log has encountered an error and needs to be
     * closed.
     */
    private volatile boolean errored;

    /**
     * LogAnchor is normally written out only during logSwitches, or log
     * archives. If a client wants the log anchor to be written out for some
     * other reason, such as when the checkpointLsn is updated, then this flag
     * should be set to true. This will cause the LogAnchor to be written out at
     * the next flush event.
     */
    private volatile boolean anchorDirty;

    /**
     * The StorageFactory that will be used to create/open log files and control
     * files.
     */
    final StorageContainerFactory storageFactory;

    /**
     * Used as a method of communication between the flush thread and the
     * archive thread. Flush thread acquires the semaphore before it starts
     * writing to a new log file, the archive thread releases it when a log file
     * is archived. In other words, everytime the status of a log File changes
     * to {@link #LOG_FILE_FULL}, the semaphore is acquired, and everytime the
     * status changes to {@link #LOG_FILE_UNUSED}, the semaphore is released.
     * This enables a strategy for initializing the semaphore when opening the
     * log - the occurences of LOG_FILE_FULL status is counted and the semaphore
     * acquired as many times.
     * 
     * @see #setupBackgroundThreads()
     * @see #handleFlushRequest_(FlushRequest)
     * @see #handleNextArchiveRequest_(ArchiveRequest)
     */
    private Semaphore logFilesSemaphore;

    /**
     * Used to manage periodic flushes of the Log.
     * 
     * @see LogWriter
     * @see #setupBackgroundThreads()
     */
    private ScheduledExecutorService flushService;

    /**
     * A Single Threaded Executor service is used to handle archive log file
     * requests.
     * 
     * @see ArchiveRequestHandler
     * @see #setupBackgroundThreads()
     */
    private ExecutorService archiveService;

    /*
     * Note on multi-threading issues. The goals of the design are to ensure
     * that:
     * 
     * a) Clients who want to insert log records do not block because a log
     * flush is taking place. The only situation where the clients will block is
     * if there is not enough memory left to allocate buffers for the new log
     * record.
     * 
     * b) Log flush should be performed by either a dedicated thread or by the
     * calling thread. Either way, only one log flush is allowed to be active at
     * any time.
     * 
     * c) Clients who request a log flush will block until the desired flush is
     * completed.
     * 
     * d) A separate thread should handle log archiving. Log archiving should
     * not interfere with a log flush, however, a log flush may have to wait for
     * archive to complete if there aren't any available log files.
     * 
     * e) Log archive requests are generated by the log flush event whenever a
     * log file is full.
     * 
     * The goal is maximise concurrency.
     * 
     * The order of locking is specified in the comments against each lock. To
     * avoid deadlocks, locks are always acquired in a particular order, and
     * released as early as possible. If a lock has to be acquired contrary to
     * defined order then the attempt must be conditional.
     */

    /**
     * Used to ensure that only one archive is active at any point in time. Must
     * be acquired before any other lock, hence incompatible with
     * {@link #flushLock}. Must be acquired before any other lock.
     */
    private final ReentrantLock archiveLock;

    /**
     * Used to ensure that only one flush is active at any point in time. Must
     * be acquired before any other lock, hence incompatible with
     * {@link #archiveLock}. Must be acquired before any other lock.
     */
    private final ReentrantLock flushLock;

    /**
     * Protects access to the LogAnchor object. Must be acquired after
     * {@link #bufferLock} if both are needed. Must be held when
     * {@link #anchorWriteLock} is acquired.
     */
    private final ReentrantLock anchorLock;

    /**
     * Protects access to {@link #currentBuffer} and {@link #logBuffers}. Must
     * be acquired before {@link #anchorLock}.
     */
    private final ReentrantLock bufferLock;

    /**
     * When the system exceeds the number of allowed Log Buffers, the inserter
     * must wait for some Log Buffers to be freed. This cndition is used by the
     * inserter to wait; the flush daemon signals this condition when buffers
     * are available.
     */
    private final Condition buffersAvailable;

    /**
     * Ensures that only one thread writes the anchor out at any point in time.
     * Can be acquired only if {@link #anchorLock} is held. Once this lock is
     * acquired, anchorLock can be released.
     */
    private final ReentrantLock anchorWriteLock;

    /**
     * These are used to synchronize between log reads and attempts to reuse log
     * files. Must be acquired before {@link #anchorLock}.
     */
    private final ReentrantLock[] readLocks;

    /**
     * Exceptions encountered by background threads are recorded here.
     */
    private final List<Exception> exceptions = Collections
        .synchronizedList(new LinkedList<Exception>());

    /**
     * If set, disables log flushes when explicitly requested by the buffer manager or transactions.
     * Log flushes still happen during log switches or when there is a checkpoint.
     * This option can improve performance at the expense of lost transactions after recovery.
     */
    private boolean disableExplicitFlushRequests = false;

    MessageCatalog mcat = new MessageCatalog();

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.wal.LogManager#insert(byte[], int)
     */
    public final Lsn insert(byte[] data, int length) {
        assertIsOpen();
        int reclen = calculateLogRecordSize(length);
        if (reclen > getMaxLogRecSize()) {
            logger.error(LOG_CLASS_NAME, "insert", mcat.getMessage(
                "EW0001",
                reclen,
                getMaxLogRecSize()));
            throw new LogException(mcat.getMessage(
                "EW0001",
                reclen,
                getMaxLogRecSize()));
        }
        bufferLock.lock();
        /*
         * Following code has the problem that if we run out of
         * log buffers it will wait until the log flush kicks in.
         * This can be a long wait depending upon how often the
         * log writer flushes the log.
         * An alternative is to force the log flush here.
         */
        /*
        if (logBuffers.size() > anchor.maxBuffers) {
            try {
                buffersAvailable.await();
            } catch (InterruptedException e) {
                logger.error(LOG_CLASS_NAME, "insert", mcat
                    .getMessage("EW0002"), e);
                throw new LogException(mcat.getMessage("EW0002"), e);
            }
        }
         */
        /*
         * FIXME Following is a temporary workaround for this issue.
         * Problem is that this is not optimum for performance,
         * as we don't have to wait for a full flush to complete
         * before resuming the insert. Ideally, we want to trigger 
         * the Log Writer here and start waiting on buffersAvailable.
         */
        if (logBuffers.size() > anchor.maxBuffers) {
            bufferLock.unlock();
            flush();
            bufferLock.lock();
        }
        
        try {
            anchorLock.lock();
            try {
                Lsn nextLsn = advanceToNextRecord(anchor.currentLsn, reclen);
                if (nextLsn.getOffset() > getEofPos()) {
                    // Add EOF record
                    // System.err.println("ADDING EOF AT " + anchor.currentLsn);
                    addToBuffer(
                        anchor.currentLsn,
                        new byte[1],
                        0,
                        anchor.maxLsn);
                    anchor.maxLsn = anchor.currentLsn;
                    anchor.currentLsn = advanceToNextFile(anchor.currentLsn);
                    nextLsn = advanceToNextRecord(anchor.currentLsn, reclen);
                }
                addToBuffer(anchor.currentLsn, data, length, anchor.maxLsn);
                anchor.maxLsn = anchor.currentLsn;
                anchor.currentLsn = nextLsn;
                return anchor.maxLsn;
            } finally {
                anchorLock.unlock();
            }
        } finally {
            bufferLock.unlock();
        }
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.wal.LogManager#flush(org.simpledbm.rss.api.wal.Lsn)
     */
    public final void flush(Lsn upto) {
        assertIsOpen();
        if (!getDisableExplicitFlushRequests() || anchorDirty || upto == null) {
            handleFlushRequest(new FlushRequest(upto));
        }
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.wal.LogManager#flush()
     */
    public final void flush() {
        assertIsOpen();
        flush(null);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.wal.LogManager#getForwardScanningReader(org.simpledbm.rss.api.wal.Lsn)
     */
    public final LogReader getForwardScanningReader(Lsn startLsn) {
        assertIsOpen();
        return new LogForwardReaderImpl(
            this,
            startLsn == null ? LogManagerImpl.FIRST_LSN : startLsn);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.wal.LogManager#getBackwardScanningReader(org.simpledbm.rss.api.wal.Lsn)
     */
    public final LogReader getBackwardScanningReader(Lsn startLsn) {
        assertIsOpen();
        return new LogBackwardReaderImpl(this, startLsn == null ? getMaxLsn()
                : startLsn);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.wal.LogManager#start()
     */
    public final synchronized void start() {
        /*
         * Opens the Log. After opening the control files, and the log files, the
         * log is scanned in order to locate the End of Log. See {@link #scanToEof}
         * for details of why this is necessary.
         */
        if (started || errored) {
            logger.error(LOG_CLASS_NAME, "start", mcat.getMessage("EW0003"));
            throw new LogException(mcat.getMessage("EW0003"));
        }
        openCtlFiles();
        openLogFiles();
        scanToEof();
        setupBackgroundThreads();
        errored = false;
        started = true;
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.wal.LogManager#shutdown()
     */
    public final synchronized void shutdown() {
        if (started) {
            /*
             * We shutdown the flush service before attempting to flush the Log -
             * to avoid unnecessary conflicts.
             */
            flushService.shutdown();
            try {
                flushService.awaitTermination(60, TimeUnit.SECONDS);
            } catch (InterruptedException e1) {
                logger.error(LOG_CLASS_NAME, "shutdown", mcat
                    .getMessage("EW0004"), e1);
            }
            logger.info(this.getClass().getName(), "shutdown", mcat
                .getMessage("IW0030"));
            if (!errored) {
                try {
                    flush();
                } catch (Exception e) {
                    logger.error(LOG_CLASS_NAME, "shutdown", mcat
                        .getMessage("EW0004"), e);
                }
            }
            archiveService.shutdown();
            try {
                archiveService.awaitTermination(60, TimeUnit.SECONDS);
            } catch (InterruptedException e1) {
                logger.error(LOG_CLASS_NAME, "shutdown", mcat
                    .getMessage("EW0004"), e1);
            }
            logger.info(this.getClass().getName(), "shutdown", mcat
                .getMessage("IW0031"));
            closeLogFiles();
            closeCtlFiles();
            logger.info(this.getClass().getName(), "shutdown", mcat
                .getMessage("IW0032"));
            /*
             * TODO // move this to the beginning Let us first set the flag so
             * that further client requests will not be entertained.
             * FIXME Is it legal to invoke start() after shutdown()?
             */
            started = false;
        }
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.wal.LogManager#getCheckpointLsn()
     */
    public final Lsn getCheckpointLsn() {
        anchorLock.lock();
        Lsn lsn = null;
		try {
			lsn = new Lsn(anchor.checkpointLsn);
		} finally {
			anchorLock.unlock();
		}
		return lsn;
    }

    /*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.rss.api.wal.LogManager#getOldestInterestingLsn()
	 */
    public final Lsn getOldestInterestingLsn() {
		anchorLock.lock();
		Lsn lsn = null;
		try {
			lsn = new Lsn(anchor.oldestInterestingLsn);
		} finally {
			anchorLock.unlock();
		}
		return lsn;
	}

    final void setCheckpointLsn(Lsn lsn) {
        anchorLock.lock();
        try {
            anchorWriteLock.lock();
            try {
                anchor.checkpointLsn = new Lsn(lsn);
                anchorDirty = true;
            } finally {
                anchorWriteLock.unlock();
            }
        } finally {
            anchorLock.unlock();
        }
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.wal.LogManager#setCheckpointLsn(org.simpledbm.rss.api.wal.Lsn, org.simpledbm.rss.api.wal.Lsn)
     */
    public final void setCheckpointLsn(Lsn lsn, Lsn oldestInterestingLsn) {
        anchorLock.lock();
        try {
            anchorWriteLock.lock();
            try {
                anchor.checkpointLsn = new Lsn(lsn);
                anchor.oldestInterestingLsn = new Lsn(oldestInterestingLsn);
                anchorDirty = true;
            } finally {
                anchorWriteLock.unlock();
            }
        } finally {
            anchorLock.unlock();
        }
    }

    /**
     * Initializes background threads and sets up any synchronisation primitives
     * that will be used to coordinate actions between the background threads.
     * 
     * @see #logFilesSemaphore
     * @see #flushService
     * @see #archiveService
     */
    private void setupBackgroundThreads() {
        logFilesSemaphore = new Semaphore(anchor.n_LogFiles - 1);
        for (int i = 0; i < anchor.n_LogFiles; i++) {
            if (anchor.fileStatus[i] == LOG_FILE_FULL) {
                try {
                    logFilesSemaphore.acquire();
                } catch (InterruptedException e) {
                    logger.error(LOG_CLASS_NAME, "setupBackgroundThreads", mcat
                        .getMessage("EW0005"), e);
                    throw new LogException(mcat.getMessage("EW0005"), e);
                }
            }
        }
        flushService = Executors.newSingleThreadScheduledExecutor();
        archiveService = Executors.newSingleThreadExecutor();

        flushService.scheduleWithFixedDelay(
            new LogWriter(this),
            anchor.logFlushInterval,
            anchor.logFlushInterval,
            TimeUnit.SECONDS);
        logger.info(this.getClass().getName(), "setupBackgroundThreads", mcat
            .getMessage("IW0028"));
        flushService.scheduleWithFixedDelay(
            new ArchiveCleaner(this),
            anchor.logFlushInterval,
            anchor.logFlushInterval,
            TimeUnit.SECONDS);
        logger.info(this.getClass().getName(), "setupBackgroundThreads", mcat
            .getMessage("IW0029"));
    }

    /**
     * Determines the LSN of the first log record in the next log file.
     * 
     * @param lsn
     *            Current Lsn
     * @return New value of Lsn
     */
    Lsn advanceToNextFile(Lsn lsn) {
        return new Lsn(lsn.getIndex() + 1, FIRST_LSN.getOffset());
    }

    /**
     * Determines LSN of next log record in the same log file.
     * 
     * @param lsn
     *            Current Lsn
     * @param length
     *            Length of the Log Record
     * @return New value of Lsn
     */
    Lsn advanceToNextRecord(Lsn lsn, int length) {
        return new Lsn(lsn.getIndex(), lsn.getOffset() + length);
    }

    /**
     * Calculates the size of a log record including any header information.
     * 
     * @param dataLength
     *            Length of the data
     * @return Length of the LogRecord
     */
    static int calculateLogRecordSize(int dataLength) {
        return dataLength + LOGREC_HEADER_SIZE;
    }

    /**
     * Returns the last usable position in the log file, allowing for an EOF
     * record.
     * 
     * @return Position of the last usable position in the log file.
     */
    private int getEofPos() {
        return anchor.logFileSize - EOF_LOGREC_SIZE;
    }

    /**
     * Returns the maximum usable space in a log file, allowing for the log file
     * header and the EOF record.
     */
    private int getUsableLogSpace() {
        return anchor.logFileSize - LOGREC_HEADER_SIZE - LogFileHeader.SIZE;
    }

    /**
     * Determines the size of the largest log record that can be accomodated. It
     * must fit into the log buffer as well as a single log file.
     */
    private int getMaxLogRecSize() {
        int n1 = getUsableLogSpace();
        int n2 = anchor.logBufferSize;

        return n1 < n2 ? n1 : n2;
    }

    /**
     * Sets the names of the control files. The maximum number of Control Files
     * that can be used is defined in {@link #MAX_CTL_FILES}.
     * 
     * @param files
     */
    final void setCtlFiles(String files[]) {
        if (files.length > MAX_CTL_FILES) {
            logger.error(LOG_CLASS_NAME, "setCtlFiles", mcat.getMessage(
                "EW0006",
                files.length,
                MAX_CTL_FILES));
            throw new LogException(mcat.getMessage(
                "EW0006",
                files.length,
                MAX_CTL_FILES));
        }
        for (int i = 0; i < files.length; i++) {
            anchor.ctlFiles[i] = new ByteString(files[i]);
        }
    }

    /**
     * Sets the paths for the Log Groups and the number of log files in each
     * group. A maximum of {@link #MAX_LOG_GROUPS} groups, and
     * {@link #MAX_LOG_FILES} files may be specified.
     * 
     * @param groupPaths
     * @param n_LogFiles
     */
    final void setLogFiles(String groupPaths[], short n_LogFiles) {
        int n_LogGroups = groupPaths.length;
        if (n_LogGroups > MAX_LOG_GROUPS) {
            logger.error(LOG_CLASS_NAME, "setLogFiles", mcat.getMessage(
                "EW0007",
                n_LogGroups,
                MAX_LOG_GROUPS));
            throw new LogException(mcat.getMessage(
                "EW0007",
                n_LogGroups,
                MAX_LOG_GROUPS));
        }
        if (n_LogFiles > MAX_LOG_FILES) {
            logger.error(LOG_CLASS_NAME, "setLogFiles", mcat.getMessage(
                "EW0008",
                n_LogFiles,
                MAX_LOG_FILES));
            throw new LogException(mcat.getMessage(
                "EW0008",
                n_LogFiles,
                MAX_LOG_FILES));
        }
        anchor.n_LogGroups = (short) n_LogGroups;
        anchor.n_LogFiles = n_LogFiles;
        for (int i = 0; i < anchor.n_LogGroups; i++) {
            anchor.groups[i] = new LogGroup(
                LOG_GROUP_IDS[i],
                groupPaths[i],
                LOG_GROUP_OK,
                anchor.n_LogFiles);
        }
    }

    final void setLogFlushInterval(int interval) {
        anchor.logFlushInterval = interval;
    }

    final void setMaxBuffers(int maxBuffers) {
        anchor.maxBuffers = maxBuffers;
    }

    /**
     * Creates a LogAnchor and initializes it to default values. Defaults are to
     * use {@link #DEFAULT_LOG_GROUPS} groups containing
     * {@link #DEFAULT_LOG_FILES} log files, and {@link #DEFAULT_CTL_FILES}
     * mirrored control files.
     */
    private LogAnchor createDefaultLogAnchor() {
        int i;

        LogAnchor anchor = new LogAnchor();
        anchor.n_CtlFiles = DEFAULT_CTL_FILES;
        anchor.n_LogGroups = DEFAULT_LOG_GROUPS;
        anchor.n_LogFiles = DEFAULT_LOG_FILES;
        anchor.ctlFiles = new ByteString[MAX_CTL_FILES];
        for (i = 0; i < MAX_CTL_FILES; i++) {
            anchor.ctlFiles[i] = new ByteString("ctl." + Integer.toString(i));
        }
        anchor.groups = new LogGroup[MAX_LOG_GROUPS];
        for (i = 0; i < MAX_LOG_GROUPS; i++) {
            anchor.groups[i] = new LogGroup(
                LOG_GROUP_IDS[i],
                DEFAULT_GROUP_PATH,
                LOG_GROUP_OK,
                anchor.n_LogFiles);
        }
        anchor.archivePath = new ByteString(DEFAULT_ARCHIVE_PATH);
        anchor.archiveMode = true;
        anchor.currentLogFile = 0;
        anchor.currentLogIndex = 1;
        anchor.archivedLogIndex = 0;
        anchor.currentLsn = FIRST_LSN;
        anchor.maxLsn = new Lsn();
        anchor.durableLsn = new Lsn();
        anchor.durableCurrentLsn = FIRST_LSN;
        anchor.fileStatus = new short[MAX_LOG_FILES];
        anchor.logIndexes = new int[MAX_LOG_FILES];
        for (i = 0; i < MAX_LOG_FILES; i++) {
            if (i != anchor.currentLogFile) {
                anchor.fileStatus[i] = LOG_FILE_UNUSED;
                anchor.logIndexes[i] = 0;
            } else {
                anchor.fileStatus[i] = LOG_FILE_CURRENT;
                anchor.logIndexes[i] = anchor.currentLogIndex;
            }
        }
        anchor.logBufferSize = DEFAULT_LOG_BUFFER_SIZE;
        anchor.logFileSize = DEFAULT_LOG_FILE_SIZE;
        // anchor.nextTrxId = 0;
        anchor.checkpointLsn = new Lsn();
        anchor.oldestInterestingLsn = new Lsn();
        anchor.logFlushInterval = 6;
        anchor.maxBuffers = 10 * anchor.n_LogFiles;

        return anchor;
    }

    /**
     * Creates a default LogImpl.
     */
    public LogManagerImpl(StorageContainerFactory storageFactory) {
        archiveLock = new ReentrantLock();
        flushLock = new ReentrantLock();
        anchorLock = new ReentrantLock();
        bufferLock = new ReentrantLock();
        buffersAvailable = bufferLock.newCondition();
        anchorWriteLock = new ReentrantLock();
        readLocks = new ReentrantLock[MAX_LOG_FILES];
        for (int i = 0; i < MAX_LOG_FILES; i++) {
            readLocks[i] = new ReentrantLock();
        }

        anchor = createDefaultLogAnchor();

        files = new StorageContainer[MAX_LOG_GROUPS][MAX_LOG_FILES];
        ctlFiles = new StorageContainer[MAX_CTL_FILES];
        logBuffers = new LinkedList<LogBuffer>();
        currentBuffer = new LogBuffer(anchor.logBufferSize);
        logBuffers.add(currentBuffer);

        this.storageFactory = storageFactory;

        errored = false;
        started = false;
        anchorDirty = false;
    }

    /**
     * Reads a LogAnchor from permanent storage. The format of a LogAnchor is as
     * follows:
     * 
     * <pre>
     *   int - length
     *   long - checksum
     *   byte[length] - data
     * </pre>
     * 
     * <p>
     * The checksum is validated to ensure that the LogAnchor is valid and has
     * not been corrupted.
     * 
     * @param container
     * @return
     */
    private LogAnchor readLogAnchor(StorageContainer container) {
        int n;
        long checksum;
        byte bufh[] = new byte[TypeSize.INTEGER + TypeSize.LONG];
        long position = 0;
        if (container.read(position, bufh, 0, bufh.length) != bufh.length) {
            logger.error(LOG_CLASS_NAME, "readLogAnchor", mcat
                .getMessage("EW0009"));
            throw new LogException(mcat.getMessage("EW0009"));
        }
        position += bufh.length;
        ByteBuffer bh = ByteBuffer.wrap(bufh);
        n = bh.getInt();
        checksum = bh.getLong();
        byte bufb[] = new byte[n];
        if (container.read(position, bufb, 0, n) != n) {
            logger.error(LOG_CLASS_NAME, "readLogAnchor", mcat
                .getMessage("EW0010"));
            throw new LogException(mcat.getMessage("EW0010"));
        }
        long newChecksum = ChecksumCalculator.compute(bufb, 0, n);
        if (newChecksum != checksum) {
            logger.error(LOG_CLASS_NAME, "readLogAnchor", mcat
                .getMessage("EW0011"));
            throw new LogException(mcat.getMessage("EW0011"));
        }
        ByteBuffer bb = ByteBuffer.wrap(bufb);
        LogAnchor anchor = new LogAnchor();
        anchor.retrieve(bb);
        return anchor;
    }

    /**
     * Creates a new Log Control file.
     * 
     * @param filename
     */
    private void createLogAnchor(String filename) {
        if (logger.isDebugEnabled()) {
            logger.debug(
                LOG_CLASS_NAME,
                "createLogAnchor",
                "SIMPLEDBM-DEBUG: Creating log control file " + filename);
        }
        StorageContainer file = null;
        try {
            file = storageFactory.create(filename);
            int n = anchor.getStoredLength();
            ByteBuffer bb = ByteBuffer.allocate(n);
            anchor.store(bb);
            bb.flip();
            updateLogAnchor(file, bb);
        } finally {
            if (file != null) {
                file.close();
            }
        }
    }

    /**
     * Create all the Log Control files.
     */
    private void createLogAnchors() {
        int i;
        for (i = 0; i < anchor.n_CtlFiles; i++) {
            createLogAnchor(anchor.ctlFiles[i].toString());
        }
    }

    /**
     * Write a Log Control block to permanent storage. See
     * {@link #readLogAnchor} for information about file format of Log Control
     * file.
     * 
     * @param container
     * @param bb
     * @see #readLogAnchor(StorageContainer)
     */
    private void updateLogAnchor(StorageContainer container, ByteBuffer bb) {
        long checksum = ChecksumCalculator.compute(bb.array(), 0, bb.limit());
        ByteBuffer bh = ByteBuffer.allocate(TypeSize.INTEGER + TypeSize.LONG);
        bh.putInt(bb.limit());
        bh.putLong(checksum);
        bh.flip();
        long position = 0;
        container.write(position, bh.array(), 0, bh.limit());
        position += bh.limit();
        container.write(position, bb.array(), 0, bb.limit());
    }

    /**
     * Update all copies of the LogAnchor.
     * 
     * @param bb
     */
    private void updateLogAnchors(ByteBuffer bb) {
        int i;
        for (i = 0; i < anchor.n_CtlFiles; i++) {
            updateLogAnchor(ctlFiles[i], bb);
        }
        //System.err.println("UPDATED LOG ANCHOR");
        anchorDirty = false;
    }

    /**
     * Creates a Log File, autoextending it to its maximum length. The file
     * header is initialized, and the rest of the file is set to Null bytes.
     * 
     * @param filename
     * @param header
     */
    private void createLogFile(String filename, LogFileHeader header) {

        if (logger.isDebugEnabled()) {
            logger.debug(
                LOG_CLASS_NAME,
                "createLogFile",
                "SIMPLEDBM-DEBUG: Creating log file " + filename);
        }

        StorageContainer file = null;
        int buflen = 8192;
        int len = anchor.logBufferSize;
        if (len < buflen) {
            buflen = len;
        }
        byte buf[] = new byte[buflen];

        try {
            file = storageFactory.create(filename);

            int written = 0;
            boolean needToWriteHeader = true;

            while (written < len) {
                int left = len - written;
                int n;

                if (needToWriteHeader) {
                    ByteBuffer bb = ByteBuffer.wrap(buf, 0, LogFileHeader.SIZE);
                    header.store(bb);
                }

                if (left > buf.length)
                    n = buf.length;
                else
                    n = left;
                file.write(written, buf, 0, n);
                written += n;
                if (needToWriteHeader) {
                    needToWriteHeader = false;
                    buf = new byte[buflen];
                }
            }
            file.flush();
        } finally {
            if (file != null) {
                file.close();
            }
        }
    }

    /**
     * Creates all the log files.
     * 
     * @see #createLogFile(String, LogFileHeader)
     */
    private void createLogFiles() {
        int i, j;
        for (i = 0; i < anchor.n_LogGroups; i++) {
            for (j = 0; j < anchor.n_LogFiles; j++) {
                LogFileHeader header = new LogFileHeader(
                    LOG_GROUP_IDS[i],
                    anchor.logIndexes[j]);
                createLogFile(anchor.groups[i].files[j].toString(), header);
            }
        }
    }

    /**
     * Resets a log file by initializing the header record.
     * 
     * @param logfile
     */
    private void resetLogFiles(int logfile) {
        LogFileHeader header = new LogFileHeader();
        ByteBuffer bb = ByteBuffer.allocate(LogFileHeader.SIZE);

        for (int i = 0; i < anchor.n_LogGroups; i++) {
            header.index = anchor.logIndexes[logfile];
            header.id = anchor.groups[i].id;
            bb.clear();
            header.store(bb);
            bb.flip();
            files[i][logfile].write(0, bb.array(), 0, bb.limit());
        }
    }

    /**
     * Creates and initializes a Log. Existing Log Files will be over-written.
     */
    final synchronized void create() {
        createLogFiles();
        createLogAnchors();
    }

    /**
     * Opens an already existing Log File.
     * 
     * @param groupno
     * @param fileno
     */
    private void openLogFile(int groupno, int fileno) {
        LogFileHeader fh = new LogFileHeader();
        byte[] bufh = new byte[fh.getStoredLength()];
        files[groupno][fileno] = storageFactory
            .open(anchor.groups[groupno].files[fileno].toString());
        if (anchor.fileStatus[fileno] != LOG_FILE_UNUSED) {
            if (files[groupno][fileno].read(0, bufh, 0, bufh.length) != bufh.length) {
                logger.error(LOG_CLASS_NAME, "openLogFile", mcat.getMessage(
                    "EW0012",
                    anchor.groups[groupno].files[fileno].toString()));
                throw new LogException(mcat.getMessage(
                    "EW0012",
                    anchor.groups[groupno].files[fileno].toString()));
            }
            ByteBuffer bh = ByteBuffer.wrap(bufh);
            fh.retrieve(bh);
            if (fh.id != LOG_GROUP_IDS[groupno]
                    || fh.index != anchor.logIndexes[fileno]) {
                logger.error(LOG_CLASS_NAME, "openLogFile", mcat.getMessage(
                    "EW0013",
                    anchor.groups[groupno].files[fileno].toString()));
                throw new LogException(mcat.getMessage(
                    "EW0013",
                    anchor.groups[groupno].files[fileno].toString()));
            }
        }
    }

    /**
     * Opens all the log files.
     */
    private void openLogFiles() {
        int i, j;
        for (i = 0; i < anchor.n_LogGroups; i++) {
            for (j = 0; j < anchor.n_LogFiles; j++) {
                openLogFile(i, j);
            }
        }
    }

    /**
     * Closes all the Log files.
     */
    private void closeLogFiles() {
        int i, j;
        for (i = 0; i < anchor.n_LogGroups; i++) {
            for (j = 0; j < anchor.n_LogFiles; j++) {
                if (files[i][j] != null) {
                    try {
                        files[i][j].close();
                    } catch (Exception e) {
                        logger.error(LOG_CLASS_NAME, "closeLogFiles", mcat
                            .getMessage("EW0014"), e);
                    }
                    files[i][j] = null;
                }
            }
        }
    }

    /**
     * Opens all the Control files.
     * <p>
     * TODO: validate the contents of each control file.
     */
    private void openCtlFiles() {
        for (int i = 0; i < anchor.n_CtlFiles; i++) {
            ctlFiles[i] = storageFactory.open(anchor.ctlFiles[i].toString());
        }
        anchor = readLogAnchor(ctlFiles[0]);
        anchor.maxLsn = anchor.durableLsn;
        anchor.currentLsn = anchor.durableCurrentLsn;
    }

    /**
     * Close all the Control files.
     */
    private void closeCtlFiles() {
        for (int i = 0; i < anchor.n_CtlFiles; i++) {
            if (ctlFiles[i] != null) {
                try {
                    ctlFiles[i].close();
                } catch (Exception e) {
                    logger.error(LOG_CLASS_NAME, "closeCtlFiles", mcat
                        .getMessage("EW0015"), e);
                }
                ctlFiles[i] = null;
            }
        }
    }

    /**
     * @return Returns the bufferSize.
     */
    public final int getLogBufferSize() {
        return anchor.logBufferSize;
    }

    /**
     * @param bufferSize
     *            The bufferSize to set.
     */
    public final void setLogBufferSize(int bufferSize) {
        anchor.logBufferSize = bufferSize;
    }

    public final void setLogFileSize(int fileSize) {
        anchor.logFileSize = fileSize;
    }

    public final void setArchivePath(String path) {
        anchor.archivePath = new ByteString(path);
    }

    public final void setArchiveMode(boolean mode) {
        anchor.archiveMode = mode;
    }

    public final Lsn getMaxLsn() {
        return anchor.maxLsn;
    }

    public final Lsn getDurableLsn() {
        return anchor.durableLsn;
    }

    private void assertIsOpen() {
        if (!started || errored) {
            logger.error(LOG_CLASS_NAME, "assertIsOpen", mcat
                .getMessage("EW0016"));
            throw new LogException(mcat.getMessage("EW0016"));
        }
    }

    /**
     * Inserts a log record into the log buffers. If there is not enough space
     * in the current log buffer, a new buffer is allocated.
     * 
     * @param lsn
     * @param data
     * @param length
     * @param prevLsn
     */
    private void addToBuffer(Lsn lsn, byte[] data, int length, Lsn prevLsn) {
        int reclen = calculateLogRecordSize(length);
        if (currentBuffer.getRemaining() < reclen) {
            // System.err.println("CURRENT BUFFER " + currentBuffer + " IS FULL");
            currentBuffer = new LogBuffer(anchor.logBufferSize);
            logBuffers.add(currentBuffer);
        }
        currentBuffer.insert(lsn, data, length, prevLsn);
    }

    /**
     * Enqueues a request for archiving a log file. The request is passed on to
     * the archive thread.
     * 
     * @param req
     * @see #archiveService
     * @see ArchiveRequestHandler
     */
    private void submitArchiveRequest(ArchiveRequest req) {
        archiveService.submit(new ArchiveRequestHandler(this, req));
    }

    /**
     * Switches the current log file. At any point in time, one of the log files
     * within a group is marked as active. Log writes occur to the active log
     * file. When an active log file becomes full, its status is changed to
     * {@link #LOG_FILE_FULL} and the next available log file is marked as
     * current. If there isn't an available log file, then the caller must wait
     * until the archive thread has freed up a log file.
     */
    private void logSwitch() {
        short next_log_file;
        int i;

        // First flush current log file contents.
        for (i = 0; i < anchor.n_LogGroups; i++) {
            files[i][anchor.currentLogFile].flush();
        }

        ArchiveRequest arec = new ArchiveRequest();
        anchorLock.lock();
        try {
            if (anchor.fileStatus[anchor.currentLogFile] != LOG_FILE_CURRENT) {
                logger.error(LOG_CLASS_NAME, "logSwitch", mcat.getMessage(
                    "EW0017",
                    anchor.currentLogFile,
                    anchor.fileStatus[anchor.currentLogFile]));
                throw new LogException(mcat.getMessage(
                    "EW0017",
                    anchor.currentLogFile,
                    anchor.fileStatus[anchor.currentLogFile]));
            }

            // System.err.println(Thread.currentThread().getName() + ": LogSwitch: LOG FILE STATUS OF " + anchor.currentLogFile + " CHANGED TO FULL");
            anchor.fileStatus[anchor.currentLogFile] = LOG_FILE_FULL;

            // Generate an archive request
            arec.fileno = anchor.currentLogFile;
            arec.logIndex = anchor.currentLogIndex;
            // System.err.println(Thread.currentThread().getName() + ": LogSwitch: Submitting archive request log index " + arec.logIndex + " log file " + arec.fileno);
            submitArchiveRequest(arec);
        } finally {
            anchorLock.unlock();
        }

        // Wait for a log file to become available
        try {
            logFilesSemaphore.acquire();
        } catch (InterruptedException e) {
            logger.error(
                LOG_CLASS_NAME,
                "logSwitch",
                mcat.getMessage("EW0018"),
                e);
            throw new LogException(mcat.getMessage("EW0018"), e);
        }

        ByteBuffer bb = null;

        // We need to determine the next log file to use
        // Note that we hold the flushLock, so we are the only active flush
        anchorLock.lock();
        try {
            try {
                // next_log_file = (short) (anchor.currentLogFile + 1);
                next_log_file = anchor.currentLogFile;
                while (anchor.fileStatus[next_log_file] != LOG_FILE_UNUSED) {
                    // System.err.println("SKIPPING LOG FILE THAT IS STILL IN USE");
                    next_log_file++;
                    if (next_log_file == anchor.n_LogFiles) {
                        next_log_file = 0;
                    }
                    if (next_log_file == anchor.currentLogFile) {
                        break;
                    }
                }

                if (anchor.fileStatus[next_log_file] != LOG_FILE_UNUSED) {
                    //System.err.println("Saved Log File=" + savedLogFile);
                    //System.err.println("Saved Log Index=" + savedLogIndex);
                    //System.err.println("Current Log File=" + anchor.currentLogFile);
                    //System.err.println("Current Log Index=" + anchor.currentLogIndex);
                    //for (int j = 0; j < anchor.n_LogFiles; j++) {
                    //	System.err.println("FileStatus[" + j + "]=" + anchor.fileStatus[j]);
                    //}
                    logger.error(LOG_CLASS_NAME, "logSwitch", mcat
                        .getMessage("EW0018"));
                    throw new LogException(mcat.getMessage("EW0018"));
                } else {
                    anchor.currentLogIndex++;
                    //System.err.println(Thread.currentThread().getName() + ": LogSwitch: LOG FILE STATUS OF " + next_log_file + " CHANGED TO CURRENT");
                    anchor.fileStatus[next_log_file] = LOG_FILE_CURRENT;
                    anchor.logIndexes[next_log_file] = anchor.currentLogIndex;
                    anchor.currentLogFile = next_log_file;
                }
                int n = anchor.getStoredLength();
                bb = ByteBuffer.allocate(n);
                anchor.store(bb);
                bb.flip();
                anchorWriteLock.lock();
            } finally {
                anchorLock.unlock();
            }
            updateLogAnchors(bb);
        } finally {
            if (anchorWriteLock.isHeldByCurrentThread()) {
                anchorWriteLock.unlock();
            }
        }
        resetLogFiles(anchor.currentLogFile);
    }

    /**
     * Update the Log Control files.
     */
    private void writeLogAnchor() {
        int n = anchor.getStoredLength();
        ByteBuffer bb = ByteBuffer.allocate(n);
        anchor.store(bb);
        bb.flip();
        updateLogAnchors(bb);
    }

    /**
     * Performs a log write. For efficiency, a single log write will write out
     * as many log records as possible. If the log file is not the current one,
     * then a log switch is performed.
     * 
     * @param req
     */
    private void doLogWrite(LogWriteRequest req) {

        if (req.logIndex != anchor.currentLogIndex) {
            logSwitch();
        }

        assert anchor.currentLogIndex == req.logIndex;

        int f = anchor.currentLogFile;
        for (int g = 0; g < anchor.n_LogGroups; g++) {
            files[g][f].write(
                req.offset,
                req.buffer.buffer,
                req.startPosition,
                req.length);
        }
    }

    /**
     * Process a log flush request. Ensures that only one flush can be active at
     * any time. Most of the work is done in
     * {@link #handleFlushRequest_(FlushRequest)}.
     * 
     * @param req
     * @see #handleFlushRequest_(FlushRequest)
     */
    private void handleFlushRequest(FlushRequest req) {
        flushLock.lock();
        try {
            handleFlushRequest_(req);
        } finally {
            flushLock.unlock();
        }
    }

    /**
     * Handles log flush requests. The process starts with the oldest log
     * buffer, and then moves progressively forward until the stop condition is
     * satisfied. To make log writes efficient, the log records in a buffer are
     * examined and grouped by log files. A single flush request per file is
     * created. The writes are then handed over to {@link #doLogWrite}. The
     * grouping does not span log buffers.
     * <p>
     * If the log buffer being processed is not the current one, then it is
     * freed after all records contained in it have been written out.
     * <p>
     * After all the log records have been written out, the {@link #anchorDirty}
     * flag is checked. If this has been set, then the Log Control files are
     * updated. This means that a log flush does not always update the control
     * files. Hence, if there is a system crash, the control files may not
     * correctly point to the real end of log. To overcome this problem, at
     * restart the Log is scanned from the point recorded in the control file,
     * and the real end of the Log is located. This is a good compromise because
     * it removes the need to update the control files at every flush.
     * 
     * @param req
     * @see #doLogWrite(LogWriteRequest)
     * @see #handleFlushRequest(FlushRequest)
     * @see #scanToEof()
     */
    private void handleFlushRequest_(FlushRequest req) {

        LinkedList<LogWriteRequest> iorequests = new LinkedList<LogWriteRequest>();
        boolean done = false;
        int totalFlushCount = 0;

        while (!done) {

            int flushCount = 0;
            boolean deleteBuffer = false;
            LogBuffer buf = null;
            Lsn durableLsn = null;
            LogWriteRequest currentRequest = null;

            bufferLock.lock();
            try {
                anchorLock.lock();
                try {
                    // Get the oldest log buffer
                    buf = logBuffers.getFirst();
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            LOG_CLASS_NAME,
                            "handleFlushRequest_",
                            "SIMPLEDBM-DEBUG: Flushing Log Buffer " + buf);
                    }
                    // Did we flush all available records?
                    boolean flushedAllRecs = true;
                    for (LogRecordBuffer rec : buf.records.values()) {
                        if (rec.getLsn().compareTo(anchor.durableLsn) <= 0) {
                            // Log record is already on disk
                            // System.err.println("SKIPPING ALREADY FLUSHED LSN=" + rec.getLsn());
                            continue;
                        }
                        if (req.upto != null
                                && rec.getLsn().compareTo(req.upto) > 0) {
                            // System.err.println("BREAKING FLUSH AS REACHED TARGET LSN=" + req.upto);
                            // we haven't finished with the buffer yet!!
                            flushedAllRecs = false;
                            done = true;
                            break;
                        }
                        // System.err.println("FLUSHING LSN=" + rec.getLsn());
                        if (currentRequest == null
                                || currentRequest.logIndex != rec
                                    .getLsn()
                                    .getIndex()) {
                            // Either this is the first log record or the log
                            // file has changed
                            currentRequest = createFlushIORequest(buf, rec);
                            iorequests.add(currentRequest);
                        } else {
                            // Same log file
                            currentRequest.length += rec.getLength();
                            if (rec.getDataLength() == 0) {
                                // This is an EOF record, so this log is now
                                // full
                                currentRequest.logfull = true;
                            }
                        }
                        durableLsn = rec.getLsn();
                        flushCount++;
                    }
                    if (flushedAllRecs) {
                        if (buf != currentBuffer) {
                            // This buffer can be deleted after we are done with it.
                            deleteBuffer = true;
                        }
                    }
                    if (buf == currentBuffer) {
                        // we have no more buffers to flush
                        done = true;
                    }
                } finally {
                    anchorLock.unlock();
                }
            } finally {
                bufferLock.unlock();
            }

            if (flushCount > 0 || deleteBuffer) {

                if (flushCount > 0) {
                    totalFlushCount += flushCount;

                    // Perform the flush actions
                    for (LogWriteRequest ioreq : iorequests) {
                        doLogWrite(ioreq);
                    }
                }

                bufferLock.lock();
                try {
                    if (durableLsn != null) {
                        anchorLock.lock();
                        try {
                            anchor.durableCurrentLsn = durableLsn;
                            anchor.durableLsn = durableLsn;
                        } finally {
                            anchorLock.unlock();
                        }
                    }
                    if (deleteBuffer) {
                        // assert buf.records.getLast().getLsn().compareTo(anchor.durableLsn) <= 0;
                        assert buf.records.lastKey().compareTo(
                            anchor.durableLsn) <= 0;
                        logBuffers.remove(buf);
                        // Inform inserters that they can proceed to acquire new
                        // buffer
                        buffersAvailable.signalAll();
                    }
                } finally {
                    bufferLock.unlock();
                }
            }

            iorequests.clear();
        }

        if (totalFlushCount > 0 || anchorDirty) {
            ByteBuffer bb = null;
            try {
                anchorLock.lock();
                try {
                    if (anchorDirty) {
                        int n = anchor.getStoredLength();
                        bb = ByteBuffer.allocate(n);
                        anchor.store(bb);
                        bb.flip();
                        /*
                         * Since we do not want to hold on to the anchorlock
                         * while the anchor is being written, we obtain the
                         * anchor write lock which prevents multiple writes to
                         * the anchor concurrently.
                         */
                        anchorWriteLock.lock();
                    }
                } finally {
                    anchorLock.unlock();
                }
            } finally {
                if (bb != null) {
                    updateLogAnchors(bb);
                }
                if (anchorWriteLock.isHeldByCurrentThread()) {
                    anchorWriteLock.unlock();
                }
            }
        }
    }

    /**
     * Creates a new flush request, the start position of the request is set to
     * the start position of the log record.
     * 
     * @param buf
     * @param rec
     */
    private LogWriteRequest createFlushIORequest(LogBuffer buf,
            LogRecordBuffer rec) {
        LogWriteRequest currentRequest = new LogWriteRequest();
        currentRequest.buffer = buf;
        currentRequest.length = rec.getLength();
        currentRequest.startPosition = rec.getPosition();
        currentRequest.logIndex = rec.getLsn().getIndex();
        currentRequest.offset = rec.getLsn().getOffset();
        if (rec.getDataLength() == 0) {
            // This is an EOF record, so this log is now full
            currentRequest.logfull = true;
        }
        return currentRequest;
    }

    /**
     * Archives a specified log file, by copying it to a new archive log file.
     * The archive log file is named in a way that allows it to be located by
     * the logIndex. The archive file is created in the archive path.
     * 
     * @param f
     */
    private void archiveLogFile(int f) {
        String name = null;
        anchorLock.lock();
        try {
            name = anchor.archivePath.toString() + "/" + anchor.logIndexes[f]
                    + ".log";
        } finally {
            anchorLock.unlock();
        }
        StorageContainer archive = null;
        try {
            archive = storageFactory.create(name);
            byte buf[] = new byte[8192];
            long position = 0;
            int n = files[0][f].read(position, buf, 0, buf.length);
            while (n > 0) {
                archive.write(position, buf, 0, n);
                position += n;
                n = files[0][f].read(position, buf, 0, buf.length);
            }
            if (position != anchor.logFileSize) {
                logger.error(LOG_CLASS_NAME, "archiveLogFile", mcat
                    .getMessage("EW0019"));
                throw new LogException(mcat.getMessage("EW0019"));
            }
            archive.flush();
        } finally {
            if (archive != null) {
                archive.close();
            }
        }
        // validateLogFile(anchor.logIndexes[f], name);
    }

    private volatile int lastArchivedFile = -1;

    /**
     * Process the next archive log file request, ensuring that only one archive
     * can run at any time. All of the real work is done in
     * {@link #handleNextArchiveRequest}.
     * 
     * @see #handleNextArchiveRequest_(ArchiveRequest)
     */
    void handleNextArchiveRequest(ArchiveRequest request) {
        archiveLock.lock();
        try {
            if (lastArchivedFile == -1) {
                lastArchivedFile = request.logIndex;
            } else {
                if (request.logIndex != lastArchivedFile + 1) {
                    this.errored = true;
                    logger.error(
                        LOG_CLASS_NAME,
                        "handleNextArchiveRequest",
                        mcat.getMessage(
                            "EW0020",
                            lastArchivedFile + 1,
                            request.logIndex));
                    throw new LogException(mcat.getMessage(
                        "EW0020",
                        lastArchivedFile + 1,
                        request.logIndex));
                }
                lastArchivedFile = request.logIndex;
            }
            handleNextArchiveRequest_(request);
        } finally {
            archiveLock.unlock();
        }
    }

    /**
     * Handles an Archive Log file request. The specified log file is archived,
     * and its status updated in the control file. Note the use of
     * {@link #readLocks} to prevent a reader from conflicting with the change
     * in the log file status.
     * <p>
     * After archiving the log file, the control file is updated.
     * 
     * @param request
     */
    private void handleNextArchiveRequest_(ArchiveRequest request) {

        // System.err.println(Thread.currentThread().getName() + ": handleNextArchiveRequest_: Archiving log index " + request.logIndex + " file " + request.fileno);
        archiveLogFile(request.fileno);

        ByteBuffer bb = null;

        try {
            /*
             * The log file has been archived, and now we need to update the
             * LogAnchor. We use an exclusivelock to prevent readers from
             * accessing a log file as it is being switched to archived status.
             */
            readLocks[request.fileno].lock();
            try {
                anchorLock.lock();
                try {
                    anchor.archivedLogIndex = request.logIndex;
                    // System.err.println(Thread.currentThread().getName() + ": handleNextArchiveRequest_: LOG FILE STATUS OF " + request.fileno + " CHANGED TO UNUSED");
                    assert anchor.fileStatus[request.fileno] == LOG_FILE_FULL;
                    anchor.fileStatus[request.fileno] = LOG_FILE_UNUSED;
                    anchor.logIndexes[request.fileno] = 0;
                    int n = anchor.getStoredLength();
                    bb = ByteBuffer.allocate(n);
                    anchor.store(bb);
                    bb.flip();
                    /*
                     * Since we do not want to hold on to the anchorlock while
                     * the anchor is being written, we obtain the anchor write
                     * lock which prevents multiple writes to the anchor
                     * concurrently.
                     */
                    anchorWriteLock.lock();
                } finally {
                    anchorLock.unlock();
                }
            } finally {
                readLocks[request.fileno].unlock();
            }
            updateLogAnchors(bb);
        } finally {
            if (anchorWriteLock.isHeldByCurrentThread())
                anchorWriteLock.unlock();
        }
        /*
         * Inform the log flush thread that a log file is now available.
         */
        logFilesSemaphore.release();
    }

    /**
     * Parse a ByteBuffer and recreate the LogRecord that was stored in it. The
     * LogRecord is validated in two ways - its Lsn must match the desired Lsn,
     * and it must have a valid checksum. The Lsn match ensures that the system
     * does not get confused by a valid but unexpected log record - for example,
     * an old record in a log file.
     * 
     * @param readLsn
     *            Lsn of the record being parsed
     * @param bb
     *            Data stream
     * @return
     * @see #LOGREC_HEADER_SIZE
     */
    private LogRecordImpl doRead(Lsn readLsn, ByteBuffer bb) {
        int offset = bb.position();
        int length = bb.getInt();
        int dataLength = length - LOGREC_HEADER_SIZE;
        LogRecordImpl logrec = new LogRecordImpl(dataLength);
        Lsn lsn = new Lsn();
        lsn.retrieve(bb);
        logrec.lsn = lsn;
        Lsn prevLsn = new Lsn();
        prevLsn.retrieve(bb);
        logrec.prevLsn = prevLsn;
        if (dataLength > 0) {
            bb.get(logrec.data, 0, dataLength);
        }
        /**
         * We cannot use bb.arrayOffset() as offset because it returns 0! This
         * is true in JDK 5.0.
         */
        long ck = ChecksumCalculator.compute(bb.array(), offset, bb.position()
                - offset);
        long checksum = bb.getLong();
        if (!lsn.equals(readLsn) || checksum != ck) {
            logger.error(LOG_CLASS_NAME, "doRead", mcat.getMessage(
                "EW0021",
                readLsn));
            throw new LogException(mcat.getMessage("EW0021", readLsn));
        }
        return logrec;
    }

    public final LogRecordImpl read(Lsn lsn) {
        return doRead(lsn);
    }

    private byte[] readLogRecordData(StorageContainer container, Lsn lsn) {
        long position = lsn.getOffset();
        byte[] lbytes = new byte[Integer.SIZE / Byte.SIZE];
        int n = container.read(position, lbytes, 0, lbytes.length);
        if (n != lbytes.length) {
            logger.error(LOG_CLASS_NAME, "doRead", mcat.getMessage(
                "EW0023",
                lsn));
            throw new LogException(mcat.getMessage("EW0023", lsn));
        }
        position += lbytes.length;
        ByteBuffer bb = ByteBuffer.wrap(lbytes);
        int length = bb.getInt();
        if (length < LOGREC_HEADER_SIZE || length > this.getMaxLogRecSize()) {
            logger.error(LOG_CLASS_NAME, "doRead", mcat.getMessage(
                "EW0024",
                lsn,
                length));
            throw new LogException(mcat.getMessage("EW0024", lsn, length));
        }
        byte[] bytes = new byte[length];
        System.arraycopy(lbytes, 0, bytes, 0, lbytes.length);
        n = container.read(position, bytes, lbytes.length, bytes.length
                - lbytes.length);
        if (n != (bytes.length - lbytes.length)) {
            logger.error(LOG_CLASS_NAME, "doRead", mcat.getMessage(
                "EW0025",
                lsn));
            throw new LogException(mcat.getMessage("EW0025", lsn));
        }
        return bytes;
    }

    /**
     * Reads the specified LogRecord, from log buffers if possible, otherwise
     * from disk. Handles the situation where a log record has been archived. To
     * avoid a conflict between an attempt to read a log record, and the
     * underlying log file being archived, locking is used. Before reading from
     * a log file, it is locked to ensure that the archive thread cannot change
     * the log file status while it is being accessed by the reader.
     * <p>
     * Must not check the maxlsn or other anchor fields, because this may be
     * called from scanToEof().
     * 
     * @param lsn
     *            Lsn of the LogRecord to be read
     * @return
     */
    final LogRecordImpl doRead(Lsn lsn) {

        /*
         * First check the log buffers.
         */
        bufferLock.lock();
        try {
            for (LogBuffer buf : logBuffers) {
                LogRecordBuffer rec = buf.find(lsn);
                if (rec != null) {
                    ByteBuffer bb = ByteBuffer.wrap(buf.buffer, rec
                        .getPosition(), rec.getLength());
                    return doRead(lsn, bb);
                }
            }
        } finally {
            bufferLock.unlock();
        }

        /*
         * LogRecord is not in the buffers, it could be in the current log files
         * or in archived log files.
         */
        while (true) {
            boolean archived = false;
            StorageContainer container = null;
            boolean readlocked = false;
            int fileno = -1;

            try {
                anchorLock.lock();
                try {
                    if (anchor.archivedLogIndex > 0
                            && lsn.getIndex() <= anchor.archivedLogIndex) {
                        /*
                         * The LogRecord is in archived log files.
                         */
                        archived = true;
                    } else {
                        /*
                         * The LogRecord is in one of the current log files.
                         */
                        fileno = -1;
                        for (int i = 0; i < anchor.n_LogFiles; i++) {
                            if (lsn.getIndex() == anchor.logIndexes[i]) {
                                fileno = i;
                            }
                        }
                        if (fileno == -1) {
                            logger.error(LOG_CLASS_NAME, "doRead", mcat
                                .getMessage("EW0022", lsn));
                            throw new LogException(mcat.getMessage(
                                "EW0022",
                                lsn));
                        }
                        /*
                         * Try to obtain a read lock on the file without
                         * waiting, because the order of locking here is
                         * opposite to that in handleNextArchiveRequest_()
                         */
                        if (readLocks[fileno].tryLock()) {
                            container = files[0][fileno];
                            readlocked = true;
                        } else {
                            /*
                             * Log file is being archived, and we could not
                             * obtain a lock on the file, so we need to retry.
                             */
                            continue;
                        }
                    }
                } finally {
                    anchorLock.unlock();
                }

                if (archived) {
                    String name = anchor.archivePath.toString() + "/"
                            + lsn.getIndex() + ".log";
                    /*
                     * TODO: We need to cache files and avoid opening and
                     * closing them repeatedly.
                     */
                    container = storageFactory.open(name);
                }
//				long position = lsn.getOffset();
//				byte[] lbytes = new byte[Integer.SIZE / Byte.SIZE];
//				int n = container.read(position, lbytes, 0, lbytes.length);
//				if (n != lbytes.length) {
//					logger.error(LOG_CLASS_NAME, "doRead", mcat.getMessage("EW0023", lsn));
//					throw new LogException(mcat.getMessage("EW0023", lsn));
//				}
//				position += lbytes.length;
//				ByteBuffer bb = ByteBuffer.wrap(lbytes);
//				int length = bb.getInt();
//				if (length < LOGREC_HEADER_SIZE || length > this.getMaxLogRecSize()) {
//					logger.error(LOG_CLASS_NAME, "doRead", mcat.getMessage("EW0024", lsn, length));
//					throw new LogException(mcat.getMessage("EW0024", lsn, length));
//				}
//				byte[] bytes = new byte[length];
//				System.arraycopy(lbytes, 0, bytes, 0, lbytes.length);
//				n = container.read(position, bytes, lbytes.length, bytes.length - lbytes.length);
//				if (n != (bytes.length - lbytes.length)) {
//					logger.error(LOG_CLASS_NAME, "doRead", mcat.getMessage("EW0025", lsn));
//					throw new LogException(mcat.getMessage("EW0025", lsn));
//				}
                byte[] bytes = readLogRecordData(container, lsn);
                ByteBuffer bb = ByteBuffer.wrap(bytes);
                return doRead(lsn, bb);
            } finally {
                if (!archived) {
                    /*
                     * If we obtained a read lock then we need to release the
                     * lock
                     */
                    if (readlocked) {
                        assert readLocks[fileno].isHeldByCurrentThread();
                        readLocks[fileno].unlock();
                    }
                } else {
                    if (container != null) {
                        container.close();
                    }
                }
            }
        }
    }

    final void validateLogFile(int logIndex, String name) {
        StorageContainer container = null;
        container = storageFactory.open(name);
        Lsn lsn = new Lsn(logIndex, FIRST_LSN.getOffset());
        try {
            while (true) {
                // System.err.println("VALIDATING LSN=" + lsn);
//				long position = lsn.getOffset();
//				byte[] lbytes = new byte[Integer.SIZE / Byte.SIZE];
//				int n = container.read(position, lbytes, 0, lbytes.length);
//				if (n != lbytes.length) {
//					logger.error(LOG_CLASS_NAME, "validateLogFile", mcat.getMessage("EW0023", lsn));
//					throw new LogException(mcat.getMessage("EW0023", lsn));
//				}
//				position += lbytes.length;
//				ByteBuffer bb = ByteBuffer.wrap(lbytes);
//				int length = bb.getInt();
//				if (length < LOGREC_HEADER_SIZE || length > this.getMaxLogRecSize()) {
//					logger.error(LOG_CLASS_NAME, "validateLogFile", mcat.getMessage("EW0024", lsn, length));
//					throw new LogException(mcat.getMessage("EW0024", lsn, length));
//				}
//				byte[] bytes = new byte[length];
//				System.arraycopy(lbytes, 0, bytes, 0, lbytes.length);
//				n = container.read(position, bytes, lbytes.length, bytes.length - lbytes.length);
//				if (n != (bytes.length - lbytes.length)) {
//					logger.error(LOG_CLASS_NAME, "validateLogFile", mcat.getMessage("EW0025", lsn));
//					throw new LogException(mcat.getMessage("EW0025", lsn));
//				}
//				bb = ByteBuffer.wrap(bytes);
                byte[] bytes = readLogRecordData(container, lsn);
                ByteBuffer bb = ByteBuffer.wrap(bytes);
                LogRecordImpl logrec = doRead(lsn, bb);
                if (logrec.getDataLength() == 0) {
                    break;
                }
                lsn = advanceToNextRecord(lsn, calculateLogRecordSize(logrec
                    .getDataLength()));
            }
        } finally {
            container.close();
        }
    }

    /**
     * Scans the Log file to locate the real End of Log. At startup, the control
     * block may not correctly point to the end of log. Hence, the log is
     * scanned from the recorded durableLsn until the real end of the log is
     * located. The control block is then updated.
     * 
     */
    private void scanToEof() {
        Lsn scanLsn, durableLsn, currentLsn;

        durableLsn = anchor.durableLsn;
        currentLsn = anchor.durableCurrentLsn;
        scanLsn = durableLsn;

        if (scanLsn.isNull()) {
            scanLsn = FIRST_LSN;
        }

        LogRecord logrec;
        for (;;) {
            try {
                logrec = read(scanLsn);
            } catch (Exception e) {
                /*
                 * We assume that an error indicates that we have gone past the
                 * end of log.
                 */
                break;
            }
            if (logrec == null) {
                break;
            }
            durableLsn = scanLsn;
            if (logrec.getDataLength() == 0) {
                /*
                 * TODO: Strictly speaking this should not be necessary, as log
                 * switches always result in the control files being updated.
                 */
                scanLsn = advanceToNextFile(scanLsn);
            } else {
                scanLsn = advanceToNextRecord(
                    scanLsn,
                    calculateLogRecordSize(logrec.getDataLength()));
            }
            currentLsn = scanLsn;
        }
        anchor.durableLsn = durableLsn;
        anchor.currentLsn = currentLsn;
        anchor.durableCurrentLsn = currentLsn;
        anchor.maxLsn = durableLsn;
        writeLogAnchor();
    }

    void logException(String className, String methodName, String key,
            Exception e) {
        logger.error(className, methodName, mcat.getMessage(key), e);
        exceptions.add(e);
        errored = true;
    }

    /**
     * Default (forward scanning) implementation of a <code>LogReader</code>.
     * 
     * @author Dibyendu Majumdar
     * @since Jul 6, 2005
     */
    static final class LogForwardReaderImpl implements LogReader {

        /**
         * The Log for which this reader is being used.
         */
        final LogManagerImpl log;

        /**
         * Always points to the Lsn of the next record to be read.
         */
        Lsn nextLsn;

        public LogForwardReaderImpl(LogManagerImpl log, Lsn startLsn) {
            this.log = log;
            this.nextLsn = startLsn;
        }

        public LogRecord getNext() {
            LogRecordImpl rec;
            for (;;) {
                Lsn maxLsn = log.getMaxLsn();
                if (nextLsn.isNull() || maxLsn.isNull()
                        || nextLsn.compareTo(LogManagerImpl.FIRST_LSN) < 0
                        || nextLsn.compareTo(maxLsn) > 0) {
                    return null;
                }
                rec = log.read(nextLsn);
                if (rec.getDataLength() > 0) {
                    nextLsn = log.advanceToNextRecord(nextLsn, rec.getLength());
                    break;
                } else {
                    /*
                     * EOF record, advance to next log file.
                     */
                    nextLsn = log.advanceToNextFile(nextLsn);
                }
            }
            return rec;
        }

        public void close() {
        }
    }

    /**
     * Backward scanning implementation of a <code>LogReader</code>.
     * 
     * @author Dibyendu Majumdar
     * @since Aug 7, 2005
     */
    static final class LogBackwardReaderImpl implements LogReader {

        /**
         * The Log for which this reader is being used.
         */
        final LogManagerImpl log;

        /**
         * Always points to the Lsn of the next record to be read.
         */
        Lsn nextLsn;

        public LogBackwardReaderImpl(LogManagerImpl log, Lsn startLsn) {
            this.log = log;
            this.nextLsn = startLsn;
        }

        public LogRecord getNext() {
            LogRecordImpl rec;
            for (;;) {
                Lsn maxLsn = log.getMaxLsn();
                if (nextLsn.isNull() || maxLsn.isNull()
                        || nextLsn.compareTo(LogManagerImpl.FIRST_LSN) < 0
                        || nextLsn.compareTo(maxLsn) > 0) {
                    return null;
                }
                rec = log.read(nextLsn);
                nextLsn = rec.prevLsn;
                if (rec.getDataLength() > 0) {
                    break;
                }
            }
            return rec;
        }

        public void close() {
        }
    }

    /**
     * Default implementation of a <code>LogRecord</code>.
     * 
     * @author Dibyendu Majumdar
     * @since Jul 6, 2005
     */
    static final class LogRecordImpl implements LogRecord {

        /**
         * Lsn of the log record.
         */
        Lsn lsn;

        /**
         * Lsn of the previous log record.
         */
        Lsn prevLsn;

        /**
         * Log record data, will be null for EOF records.
         */
        byte[] data;

        /**
         * Length of the log data, is 0 for EOF records.
         */
        final int length;

        LogRecordImpl(int length) {
            this.length = length;
            if (length > 0)
                data = new byte[length];
        }

        /**
         * Returns the overall length of the Log Record including the header
         * information.
         */
        public int getLength() {
            return length + LogManagerImpl.LOGREC_HEADER_SIZE;
        }

        public int getDataLength() {
            return length;
        }

        public byte[] getData() {
            return data;
        }

        public Lsn getLsn() {
            return lsn;
        }
    }

    /**
     * Holds information for a log flush request.
     * 
     * @author Dibyendu Majumdar
     * @since Jul 6, 2005
     */
    static final class FlushRequest {

        final Lsn upto;

        public FlushRequest(Lsn lsn) {
            this.upto = lsn;
        }
    }

    /**
     * Holds details for a single log write request.
     * 
     * @author Dibyendu Majumdar
     * 
     */
    static final class LogWriteRequest {
        /**
         * Index of the log file to which the write is required.
         */
        int logIndex;

        /**
         * Starting position within the LogBuffer for the write.
         */
        int startPosition;

        /**
         * Number of bytes to be written.
         */
        int length;

        /**
         * File offset where write should begin.
         */
        long offset;

        /**
         * Buffer that contains the data that is to be written.
         */
        LogBuffer buffer;

        /**
         * Indicates whether this write would cause the log file to become full.
         * This info can be used to trigger a log archive request.
         */
        boolean logfull;
    }

    /**
     * Holds details of an log archive request.
     * 
     * @author Dibyendu Majumdar
     */
    static final class ArchiveRequest {
        int fileno;

        int logIndex;
    }

    /**
     * Each Log file has a header record that identifies the Log file. When a
     * Log file is opened, its header is validated.
     * <p>
     * A Log file starts with the header, which is followed by variable sized
     * log records. The last log record is a special EOF marker. This is needed
     * because when scanning forward, there has to be some way of determining
     * whether the end of the Log file has been reached. Space for the EOF
     * record is always reserved; an ordinary Log record cannot use up this
     * space.
     */
    static final class LogFileHeader implements Storable {
        /**
         * This is the id of the Log Group to which this file belongs.
         */
        char id;

        /**
         * The index of the log file.
         */
        int index;

        /**
         * The LogFileHeader occupies {@value} bytes on disk.
         */
        static final int SIZE = (Character.SIZE / Byte.SIZE)
                + (Integer.SIZE / Byte.SIZE);

        LogFileHeader() {
        }

        LogFileHeader(char id, int index) {
            this.id = id;
            this.index = index;
        }

        public int getStoredLength() {
            return SIZE;
        }

        public void retrieve(ByteBuffer bb) {
            id = bb.getChar();
            index = bb.getInt();
        }

        public void store(ByteBuffer bb) {
            bb.putChar(id);
            bb.putInt(index);
        }

        @Override
        public String toString() {
            return "LogFileHeader(id=" + id + ",index=" + index + ")";
        }
    }

    /**
     * LogGroup represents a set of online log files. Within a group, all files
     * have the same group id - which is a single character. All files within
     * the group are stored in the same directory path.
     * 
     * @author Dibyendu Majumdar
     * 
     */
    static final class LogGroup implements Storable {

        /**
         * Id of the Log Group. The header record of each log file in the group
         * is updated with this id, so that it is possible to validate the log
         * file.
         * 
         * @see LogFileHeader
         */
        char id;

        /**
         * The status of the Log Group.
         * 
         * @see LogManagerImpl#LOG_GROUP_OK
         * @see LogManagerImpl#LOG_GROUP_INVALID
         */
        int status;

        /**
         * The path to the location for log files within this group.
         */
        ByteString path;

        /**
         * The fully qualified names of all the log files within this group. The
         * name of the log file is formed by combining path,group id, and a
         * sequence number.
         * 
         * @see #LogGroup()
         */
        ByteString files[];

        LogGroup() {
        }

        LogGroup(char id, String path, int status, int n_files) {
            this.id = id;
            this.path = new ByteString(".");
            this.status = status;
            this.files = new ByteString[n_files];
            for (int j = 0; j < n_files; j++) {
                this.files[j] = new ByteString(path + "/" + id + "."
                        + Integer.toString(j));
            }
        }

        /*
         * @see org.simpledbm.rss.io.Storable#getStoredLength()
         */
        public int getStoredLength() {
            int n = Character.SIZE / Byte.SIZE;
            n += Integer.SIZE / Byte.SIZE;
            n += path.getStoredLength();
            n += Short.SIZE / Byte.SIZE;
            for (ByteString file : files) {
                n += file.getStoredLength();
            }
            return n;
        }

        /*
         * @see org.simpledbm.rss.io.Storable#retrieve(java.nio.ByteBuffer)
         */
        public void retrieve(ByteBuffer bb) {
            id = bb.getChar();
            status = bb.getInt();
            path = new ByteString();
            path.retrieve(bb);
            short n = bb.getShort();
            files = new ByteString[n];
            for (short i = 0; i < n; i++) {
                files[i] = new ByteString();
                files[i].retrieve(bb);
            }
        }

        /*
         * @see org.simpledbm.rss.io.Storable#store(java.nio.ByteBuffer)
         */
        public void store(ByteBuffer bb) {
            bb.putChar(id);
            bb.putInt(status);
            path.store(bb);
            short n = (short) files.length;
            bb.putShort(n);
            for (short i = 0; i < n; i++) {
                files[i].store(bb);
            }
        }
    }

    /**
     * LogAnchor holds control information for a Log.
     */
    static final class LogAnchor implements Storable {

        /**
         * The number of Control Files associated with the log.
         * <p>
         * MT safe, because it is updated only once.
         * 
         * @see #ctlFiles
         */
        short n_CtlFiles;

        /**
         * The names of the Control Files for the Log.
         * <p>
         * MT safe, because it is updated only once.
         * 
         * @see #n_CtlFiles
         */
        ByteString ctlFiles[];

        /**
         * The number of Log Groups in use by the Log.
         * <p>
         * MT safe, because it is updated only once.
         * 
         * @see #groups
         */
        short n_LogGroups;

        /**
         * The Log Groups, including details of log files within each group.
         * <p>
         * MT safe, because it is updated only once.
         * 
         * @see #n_LogGroups
         */
        LogGroup groups[];

        /**
         * Number of log files within each log group.
         * <p>
         * MT safe, because it is updated only once.
         */
        short n_LogFiles;

        /**
         * A log file has a status associated with it; this is maintained in
         * this array.
         * <p>
         * Access to this array is protected by anchorLock
         * 
         * @see LogManagerImpl#anchorLock
         * @see LogManagerImpl#LOG_FILE_UNUSED
         * @see LogManagerImpl#LOG_FILE_CURRENT
         * @see LogManagerImpl#LOG_FILE_FULL
         * @see LogManagerImpl#LOG_FILE_INVALID
         */
        volatile short fileStatus[];

        /**
         * The Log Index uniquely identifies a log file; the indexes of all
         * online log files are stored in this array.
         * <p>
         * Access to this array is protected by anchorLock.
         * 
         * @see org.simpledbm.rss.api.wal.Lsn
         */
        int logIndexes[];

        /**
         * Indicates whether log files should be archived.
         * <p>
         * MT safe, because it is updated only once.
         */
        boolean archiveMode;

        /**
         * The path to the location of archived log files.
         * <p>
         * MT safe, because it is updated only once.
         * 
         * @see LogManagerImpl#DEFAULT_ARCHIVE_PATH
         */
        ByteString archivePath;

        /**
         * The size of a log buffer.
         * <p>
         * MT safe, because it is updated only once.
         */
        int logBufferSize;

        /**
         * The size of an individual Log file; all log files are of the same
         * size.
         * <p>
         * MT safe, because it is updated only once.
         */
        int logFileSize;

        /**
         * The log file that is currently being written to.
         * <p>
         * Access to this is protected by anchorLock.
         */
        short currentLogFile;

        /**
         * The index of the log file that is currently being written to.
         * <p>
         * Access to this is protected by anchorLock.
         */
        int currentLogIndex;

        /**
         * The index of the log file that is currently being written to.
         * <p>
         * Access to this is protected by readLocks, anchorLock.
         */
        int archivedLogIndex;

        /**
         * Protected by bufferLock, anchorLock
         */
        Lsn currentLsn;

        /**
         * Protected by bufferLock, anchorLock
         */
        volatile Lsn maxLsn;

        /**
         * Protected by anchorLock
         */
        volatile Lsn durableLsn;

        /**
         * Protected by anchorLock
         */
        Lsn durableCurrentLsn;

        Lsn checkpointLsn;

        /**
         * This is the oldest LSN that may be needed during restart
         * recovery. It is the lesser of: 
         * a) oldest start LSN amongst all active
         * transactions during a checkpoint.
         * b) oldest recovery LSN amongst all dirty pages
         * in the buffer pool.
         * c) checkpoint LSN.
         */
        Lsn oldestInterestingLsn;

        /**
         * Specifies the maximum number of log buffers to allocate. Thread safe.
         */
        int maxBuffers;

        /**
         * Specifies the interval between log flushes in seconds. Thread safe.
         */
        int logFlushInterval;

        public int getStoredLength() {
            int n = 0;
            int i;
            n += (Short.SIZE / Byte.SIZE); // n_CtlFiles
            for (i = 0; i < n_CtlFiles; i++) { // ctlFiles
                n += ctlFiles[i].getStoredLength();
            }
            n += (Short.SIZE / Byte.SIZE); // n_LogGroups
            for (i = 0; i < n_LogGroups; i++) {
                n += groups[i].getStoredLength();
            }
            n += (Short.SIZE / Byte.SIZE); // n_LogFiles
            n += n_LogFiles * (Short.SIZE / Byte.SIZE); // fileStatus
            n += n_LogFiles * (Integer.SIZE / Byte.SIZE); // logIndexes
            n += 1; // archiveMode
            n += archivePath.getStoredLength(); // archivePath
            n += (Integer.SIZE / Byte.SIZE); // logBufferSize
            n += (Integer.SIZE / Byte.SIZE); // logFileSize
            n += (Short.SIZE / Byte.SIZE); // currentLogFile
            n += (Integer.SIZE / Byte.SIZE); // currentLogIndex
            n += (Integer.SIZE / Byte.SIZE); // archivedLogIndex
            n += currentLsn.getStoredLength();
            n += maxLsn.getStoredLength();
            n += durableLsn.getStoredLength();
            n += durableCurrentLsn.getStoredLength();
            n += checkpointLsn.getStoredLength();
            n += oldestInterestingLsn.getStoredLength();
            n += (Integer.SIZE / Byte.SIZE); // maxBuffers
            n += (Integer.SIZE / Byte.SIZE); // logFlushInterval
            return n;
        }

        public void retrieve(ByteBuffer bb) {
            int i;
            n_CtlFiles = bb.getShort();
            ctlFiles = new ByteString[n_CtlFiles];
            for (i = 0; i < n_CtlFiles; i++) { // ctlFiles
                ctlFiles[i] = new ByteString();
                ctlFiles[i].retrieve(bb);
            }
            n_LogGroups = bb.getShort();
            groups = new LogGroup[n_LogGroups];
            for (i = 0; i < n_LogGroups; i++) {
                groups[i] = new LogGroup();
                groups[i].retrieve(bb);
            }
            n_LogFiles = bb.getShort();
            fileStatus = new short[n_LogFiles];
            logIndexes = new int[n_LogFiles];
            for (i = 0; i < n_LogFiles; i++) {
                fileStatus[i] = bb.getShort();
            }
            for (i = 0; i < n_LogFiles; i++) {
                logIndexes[i] = bb.getInt();
            }
            byte b = bb.get();
            archiveMode = b == 1;
            archivePath = new ByteString();
            archivePath.retrieve(bb);
            logBufferSize = bb.getInt();
            logFileSize = bb.getInt();
            currentLogFile = bb.getShort();
            currentLogIndex = bb.getInt();
            archivedLogIndex = bb.getInt();
            currentLsn = new Lsn();
            currentLsn.retrieve(bb);
            maxLsn = new Lsn();
            maxLsn.retrieve(bb);
            durableLsn = new Lsn();
            durableLsn.retrieve(bb);
            durableCurrentLsn = new Lsn();
            durableCurrentLsn.retrieve(bb);
            checkpointLsn = new Lsn();
            checkpointLsn.retrieve(bb);
            oldestInterestingLsn = new Lsn();
            oldestInterestingLsn.retrieve(bb);
            maxBuffers = bb.getInt();
            logFlushInterval = bb.getInt();
        }

        public void store(ByteBuffer bb) {
            int i;
            bb.putShort(n_CtlFiles);
            for (i = 0; i < n_CtlFiles; i++) { // ctlFiles
                ctlFiles[i].store(bb);
            }
            bb.putShort(n_LogGroups);
            for (i = 0; i < n_LogGroups; i++) {
                groups[i].store(bb);
            }
            bb.putShort(n_LogFiles);
            for (i = 0; i < n_LogFiles; i++) {
                bb.putShort(fileStatus[i]);
            }
            for (i = 0; i < n_LogFiles; i++) {
                bb.putInt(logIndexes[i]);
            }
            if (archiveMode)
                bb.put((byte) 1);
            else
                bb.put((byte) 0);
            archivePath.store(bb);
            bb.putInt(logBufferSize);
            bb.putInt(logFileSize);
            bb.putShort(currentLogFile);
            bb.putInt(currentLogIndex);
            bb.putInt(archivedLogIndex);
            currentLsn.store(bb);
            maxLsn.store(bb);
            durableLsn.store(bb);
            durableCurrentLsn.store(bb);
            checkpointLsn.store(bb);
            oldestInterestingLsn.store(bb);
            bb.putInt(maxBuffers);
            bb.putInt(logFlushInterval);
        }
    }

    /**
     * LogBuffer is a section of memory where LogRecords are stored while they
     * are in-flight. A LogBuffer may contain several LogRecords at a point in
     * time, each LogRecord is stored in a fully serialized format, ready to be
     * transferred to disk without further conversion. This is done to allow the
     * log writes to be performed in large chunks. To allow easy identification
     * of the LogRecords contained within a buffer, a separate list of pointers
     * (LogRecordBuffers) is maintained.
     * 
     * @author Dibyendu Majumdar
     */
    static final class LogBuffer {

        static volatile int nextID = 0;

        /**
         * The buffer contents.
         */
        final byte[] buffer;

        /**
         * Number of bytes remaining in the buffer.
         */
        volatile int remaining;

        /**
         * Current position within the buffer.
         */
        volatile int position;

        /**
         * List of records that are mapped to this buffer.
         */
        final TreeMap<Lsn, LogRecordBuffer> records;

        final int id = ++nextID;

        /**
         * Create a LogBuffer of specified size.
         * 
         * @param size
         */
        public LogBuffer(int size) {
            buffer = new byte[size];
            position = 0;
            remaining = size;
            records = new TreeMap<Lsn, LogRecordBuffer>();
        }

        /**
         * Returns the number of bytes left in the buffer.
         */
        int getRemaining() {
            return remaining;
        }

        /**
         * Add a new LogRecord to the buffer. Caller must ensure that there is
         * enough space to add the record.
         * 
         * @param lsn
         *            Lsn of the new LogRecord
         * @param b
         *            Data contents
         * @param length
         *            Length of the data
         * @param prevLsn
         *            Lsn of previous LogRecord
         * @see LogManagerImpl#LOGREC_HEADER_SIZE
         */
        void insert(Lsn lsn, byte[] b, int length, Lsn prevLsn) {
            int reclen = LogManagerImpl.calculateLogRecordSize(length);
            assert reclen <= remaining;
            LogRecordBuffer rec = new LogRecordBuffer(lsn, position, reclen);
            ByteBuffer bb = ByteBuffer.wrap(buffer, position, reclen);
            bb.putInt(reclen);
            lsn.store(bb);
            prevLsn.store(bb);
            bb.put(b, 0, length);
            long checksum = ChecksumCalculator.compute(buffer, position, reclen
                    - (Long.SIZE / Byte.SIZE));
            bb.putLong(checksum);
            position += reclen;
            remaining -= reclen;
            records.put(lsn, rec);
        }

        /**
         * Search for a particular log record within this LogBuffer.
         * 
         * @param lsn
         *            Lsn of the LogRecord being searched for.
         * @return LogRecordBuffer for the specified LogRecord if found, else
         *         null.
         */
        LogRecordBuffer find(Lsn lsn) {
            return records.get(lsn);
        }

        /**
         * Determine if the specified Lsn is contained within the LogRecords in
         * the buffer.
         * 
         * @param lsn
         */
        int contains(Lsn lsn) {
            Lsn lsn1 = records.firstKey();
            if (lsn1 != null) {
                Lsn lsn2 = records.lastKey();
                int rc2 = lsn.compareTo(lsn2);
                if (rc2 > 0) {
                    return 1;
                }
                int rc1 = lsn.compareTo(lsn1);
                if (rc1 >= 0 && rc2 <= 0) {
                    return 0;
                }
            }
            return -1;
        }

        @Override
        public String toString() {
            if (records.size() > 0) {
                Lsn lsn1 = records.firstKey();
                Lsn lsn2 = records.lastKey();
                return "LogBuffer(id=" + id + ", firstLsn=" + lsn1
                        + ", lastLsn=" + lsn2 + ")";
            }
            return "LogBuffer(id=" + id + ")";
        }

    }

    /**
     * LogRecords are initially stored in log buffers, and LogRecordBuffers are
     * used to maintain information about the position and length of each
     * LogRecord in the buffer.
     * 
     * @author Dibyendu Majumdar
     */
    static final class LogRecordBuffer implements Comparable<LogRecordBuffer> {

        /**
         * Position of the log record within the log buffer.
         */
        private final int position;

        /**
         * The length of the log record, including the header information.
         */
        private final int length;

        /**
         * Lsn of the log record.
         */
        private final Lsn lsn;

        LogRecordBuffer(Lsn lsn, int position, int length) {
            this.lsn = lsn;
            this.position = position;
            this.length = length;
        }

        public final int getLength() {
            return length;
        }

        public final Lsn getLsn() {
            return lsn;
        }

        public final int getPosition() {
            return position;
        }

        /**
         * Compute that length of the user supplied data content.
         */
        public final int getDataLength() {
            return length - LogManagerImpl.LOGREC_HEADER_SIZE;
        }

        public int compareTo(LogRecordBuffer o) {
            return lsn.compareTo(o.lsn);
        }
    }

    static final class ArchiveCleaner implements Runnable {

        final private LogManagerImpl log;

        public ArchiveCleaner(LogManagerImpl log) {
            this.log = log;
        }

        public void run() {
            Lsn oldestInterestingLsn = log.getOldestInterestingLsn();
            int archivedLogIndex = oldestInterestingLsn.getIndex() - 1;
            while (archivedLogIndex > 0) {
                String name = log.anchor.archivePath.toString() + "/"
                        + archivedLogIndex + ".log";
                try {
                    log.storageFactory.delete(name);
                    if (logger.isDebugEnabled()) {
                        logger.debug(
                            ArchiveCleaner.class.getName(),
                            "run",
                            "SIMPLEDBM-DEBUG: Removed archived log file "
                                    + name);
                    }
                    // System.err.println("REMOVED ARCHIVED LOG FILE " + name);
                } catch (StorageException e) {
                    // e.printStackTrace();
                    break;
                }
                archivedLogIndex--;
            }
        }
    }

    /**
     * Handles periodic log flushes. Scheduling is managed by
     * {@link LogManagerImpl#flushService}.
     * 
     * @author Dibyendu Majumdar
     * @since Jul 5, 2005
     */
    static final class LogWriter implements Runnable {

        final private LogManagerImpl log;

        public LogWriter(LogManagerImpl log) {
            this.log = log;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.util.concurrent.Callable#call()
         */
        public void run() {
            try {
                log.flush();
            } catch (Exception e) {
                log.logException(LogWriter.class.getName(), "run", "EW0026", e);
            }
        }
    }

    /**
     * Handles requests to create archive log files.
     * 
     * @author Dibyendu Majumdar
     * @since Jul 5, 2005
     */
    static final class ArchiveRequestHandler implements Callable<Boolean> {

        final private LogManagerImpl log;

        final private ArchiveRequest request;

        public ArchiveRequestHandler(LogManagerImpl log, ArchiveRequest request) {
            this.log = log;
            this.request = request;
        }

        /*
         * (non-Javadoc)
         * 
         * @see java.util.concurrent.Callable#call()
         */
        public Boolean call() {
            try {
                log.handleNextArchiveRequest(request);
            } catch (Exception e) {
                log.logException(
                    ArchiveRequestHandler.class.getName(),
                    "call",
                    "EW0027",
                    e);
            }
            return Boolean.TRUE;
        }
    }

    /**
     * If set, disables log flushes when explicitly requested by the buffer manager or transactions.
     * Log flushes still happen during log switches or when there is a checkpoint.
     * This option can improve performance at the expense of lost transactions after recovery.
     */
    public void setDisableExplicitFlushRequests(
            boolean disableExplicitFlushRequests) {
        this.disableExplicitFlushRequests = disableExplicitFlushRequests;
    }

    /**
     * If set, disables log flushes when explicitly requested by the buffer manager or transactions.
     * Log flushes still happen during log switches or when there is a checkpoint.
     * This option can improve performance at the expense of lost transactions after recovery.
     */
    public boolean getDisableExplicitFlushRequests() {
        return disableExplicitFlushRequests;
    }

}