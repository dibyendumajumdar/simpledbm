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
package org.simpledbm.rss.api.wal;

/**
 * The Log interface provides a mechanism for creating and reading Log Records.
 * The permitted operations are:
 * <ol>
 * <li>Create new Log Records.</li>
 * <li>Request for log records to be flushed to disk.</li>
 * <li>Read Log Records sequentially from a starting point.</li>
 * </ol>
 * Each Log Record is uniquely identified using an {@link Lsn}. The Log
 * implementation does not care about the contents of the log record; this is
 * done to ensure maximum reuse.
 * <p>
 * The interface is pretty abstract and has very little dependency on other
 * modules, thus allowing it to be reused in different contexts.
 * <p>
 * To obtain an instance of Log, you need to first obtain an instance of
 * {@link LogFactory}. A LogFactory implementation provides the
 * {@link org.simpledbm.rss.api.wal.LogFactory#getLog()}
 * method for obtaining an instance of an existing Log.
 * <p>
 * Here is an example of how this is done:
 * 
 * <pre>
 * LogFactory factory = new LogFactoryImpl(); // use default parameters
 * Log log = factory.openLog(null); // use default parameters
 * try {
 *     String s = &quot;hello world!&quot;;
 *     byte[] b = s.getBytes();
 *     Lsn lsn = log.insert(b, b.length);
 *     System.out.println(&quot;Lsn of new record = &quot; + lsn);
 * } finally {
 *     if (log != null)
 *         log.close();
 * }
 * </pre>
 * 
 * @author Dibyendu Majumdar
 * @since 11 June 2005
 * @see Lsn
 * @see LogFactory
 * @see LogReader
 */
public interface LogManager {

    public final String LOGGER_NAME = "org.simpledbm.walogmgr";

    /**
     * Start the LogMgr instance. This may initiate background threads.
     */
    void start();

    /**
     * Inserts a new Log Record and returns the Lsn assigned to the new record.
     * The new Log Record may or may not be flushed to disk - if the caller
     * wants to ensure this, the {@link #flush(Lsn)} method should be called.
     * 
     * @param data Data for the log record, will be copied.
     * @param length Length of the data
     * @return Lsn assigned to the new log record
     */
    Lsn insert(byte[] data, int length);

    /**
     * Returns the Lsn of the last Log Record, i.e., the End of Log. This record
     * may or may not be on disk.
     */
    Lsn getMaxLsn();

    /**
     * Returns the Lsn of the last Log Record that is known to have been flushed
     * to disk.
     */
    Lsn getDurableLsn();

    /**
     * Returns the LSN of the last Checkpoint log record.
     */
    Lsn getCheckpointLsn();

    /**
     * Get the LSN of the oldest log record that may be of interest for recovery
     * purposes.
     */
    Lsn getOldestInterestingLsn();

    /**
     * Sets the LSN of the latest Checkpoint log record. Log Manager must ensure
     * that this is reliably recorded on disk.
     * 
     * @param lsn LSN of the Checkpoint log record.
     */
    void setCheckpointLsn(Lsn lsn, Lsn oldestInterestingLsn);

    /**
     * Reads the specified LogRecord, from log buffers if possible, otherwise
     * from disk. Must handle the situation where a log record has been
     * archived. Must be thread safe.
     * 
     * @param lsn Lsn of the LogRecord to be read
     * @return LogRecord read
     * @throws LogException On error
     */
    LogRecord read(Lsn lsn);

    /**
     * Forces all the Log Records to disk upto the specified Lsn. Will block the
     * caller until the flush is completed. Note that depending upon the
     * implementation, flushing the Log may or may not impact other operations
     * such as inserting new records, and reading log records.
     * 
     * @param upto Lsn of the Log Record
     * @throws LogException On error
     */
    void flush(Lsn upto);

    /**
     * Forces all Log Records to disk. Will block the caller until the flush is
     * completed. Note that depending upon the implementation, flushing the Log
     * may or may not impact other operations such as inserting new records, and
     * reading log records. Note that after a complete flush, the methods
     * {@link #getMaxLsn()} and {@link #getDurableLsn()} will return the same
     * Lsn.
     * 
     * @throws LogException On error
     */
    void flush();

    /**
     * Obtains a forward scanning reader with the start Lsn set to the specified
     * Lsn. If the startLsn is <code>null</code>, reading will start at the
     * beginning of the Log. Note that log files typically get archived and
     * removed after some time, so it may not be possible to access old log
     * records beyond a point.
     * 
     * @param startLsn LSN to start reading from
     */
    LogReader getForwardScanningReader(Lsn startLsn);

    /**
     * Obtains a backward scanning reader with the start Lsn set to the
     * specified Lsn. If the startLsn is <code>null</code>, reading will start
     * at the end of the Log. Note that log files typically get archived and
     * removed after some time, so it may not be possible to access old log
     * records beyond a point.
     * 
     * @param startLsn The LSN to read from
     */
    LogReader getBackwardScanningReader(Lsn startLsn);

    /**
     * Closes the Log and releases any resources allocated by the Log.
     */
    void shutdown();

}
