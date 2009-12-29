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
package org.simpledbm.rss.impl.wal;

import java.util.ArrayList;
import java.util.Properties;

import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.util.mcat.MessageCatalog;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.wal.LogException;
import org.simpledbm.rss.api.wal.LogFactory;
import org.simpledbm.rss.api.wal.LogManager;

/**
 * Factory for creating LogMgr instances. The following parameters can specified
 * in the {@link #createLog} and {@link #getLog} methods.
 * <p>
 * <table border="1">
 * <tr>
 * <th>Property Name</th>
 * <th>Description</th>
 * </tr>
 * <tr>
 * <td>log.ctl.{n}</td>
 * <td>The fully qualified path to the log control file. The first file should
 * be specified as log.ctl.1, second as log.ctl.2, and so on. Upto a maximum of
 * {@link LogManagerImpl#MAX_CTL_FILES} can be specified.</td>
 * </tr>
 * <tr>
 * <td>log.groups.{n}.path</td>
 * <td>The path where log files of a group should be stored. The first log
 * group is specified as log.groups.1.path, the second as log.groups.2.path, and
 * so on. Upto a maximum of {@link LogManagerImpl#MAX_LOG_GROUPS} log groups can be
 * specified.</td>
 * </tr>
 * <tr>
 * <td>log.archive.path</td>
 * <td>Defines the path for storing archive files. Defaults to current
 * directory.</td>
 * </tr>
 * <tr>
 * <td>log.group.files</td>
 * <td>Specifies the number of log files within each group. Upto a maximum of
 * {@link LogManagerImpl#MAX_LOG_FILES} are allowed. Defaults to 2.</td>
 * </tr>
 * <tr>
 * <td>log.file.size</td>
 * <td>Specifies the size of each log file in bytes.</td>
 * </tr>
 * <tr>
 * <td>log.buffer.size</td>
 * <td>Specifies the size of the log buffer in bytes.</td>
 * </tr>
 * <tr>
 * <td>log.buffer.limit</td>
 * <td> Sets a limit on the maximum number of log buffers that can be allocated.
 * </td>
 * </tr>
 * <tr>
 * <td>log.flush.interval</td>
 * <td>Sets the interval (in seconds) between log flushes. </td>
 * </tr>
 * </table>
 * 
 * @author Dibyendu Majumdar
 * @since 26-Jun-2005
 */
public final class LogFactoryImpl implements LogFactory {
	
	final Platform platform;
	final PlatformObjects po;
	
	public LogFactoryImpl(Platform platform, Properties props) {
		this.platform = platform;
    	this.po = platform.getPlatformObjects(LogManager.LOGGER_NAME);
    	MessageCatalog mcat = po.getMessageCatalog();
		
        // Write Ahead Log Manager messages
        mcat.addMessage(
                "EW0001",
                "SIMPLEDBM-EW0001: Log record is {0} bytes whereas maximum allowed log record size is {0}");
        mcat.addMessage(
                "EW0002",
                "SIMPLEDBM-EW0002: Unexpected error ocurred while attempting to insert a log record");
        mcat.addMessage(
                "EW0003",
                "SIMPLEDBM-EW0003: Log is already open or has encountered an error");
        mcat.addMessage(
            "EW0004",
            "SIMPLEDBM-EW0004: Unexpected error occurred during shutdown");
        mcat.addMessage("EW0005", "SIMPLEDBM-EW0005: Unexpected error occurred");
        mcat.addMessage(
                "EW0006",
                "SIMPLEDBM-EW0006: Specified number of log control files {0} exceeds the maximum limit of {1}");
        mcat.addMessage(
                "EW0007",
                "SIMPLEDBM-EW0007: Specified number of log groups {0} exceeds the maximum limit of {1}");
        mcat.addMessage(
                "EW0008",
                "SIMPLEDBM-EW0008: Specified number of log files {0} exceeds the maximum limit of {1}");
        mcat.addMessage(
                "EW0009",
                "SIMPLEDBM-EW0009: Error occurred while reading Log Anchor header information");
        mcat.addMessage(
            "EW0010",
            "SIMPLEDBM-EW0010: Error occurred while reading Log Anchor body");
        mcat.addMessage(
                "EW0011",
                "SIMPLEDBM-EW0011: Error occurred while validating Log Anchor - checksums do not match");
        mcat.addMessage(
                "EW0012",
                "SIMPLEDBM-EW0012: Error occurred while reading header record for Log File {0}");
        mcat.addMessage(
                "EW0013",
                "SIMPLEDBM-EW0013: Error occurred while opening Log File {0} - header is corrupted");
        mcat.addMessage(
                "EW0014",
                "SIMPLEDBM-EW0014: Unexpected error occurred while closing Log File");
        mcat.addMessage(
                "EW0015",
                "SIMPLEDBM-EW0015: Unexpected error occurred while closing Control File");
        mcat.addMessage(
            "EW0016",
            "SIMPLEDBM-EW0016: Log file is not open or has encountered errors");
        mcat.addMessage(
            "EW0017",
            "SIMPLEDBM-EW0017: Log file {0} has unexpected status {1}");
        mcat.addMessage("EW0018", "SIMPLEDBM-EW0018: Unexpected error occurred");
        mcat.addMessage(
                "EW0019",
                "SIMPLEDBM-EW0019: Error occurred while attempting to archive Log File");
        mcat.addMessage(
                "EW0020",
                "SIMPLEDBM-EW0020: Error occurred while processing archive request - expected request {0} but got {1}");
        mcat.addMessage(
                "EW0021",
                "SIMPLEDBM-EW0021: Error occurred while reading Log Record {0} - checksum mismatch");
        mcat.addMessage(
                "EW0022",
                "SIMPLEDBM-EW0022: Error occurred while reading Log Record {0} - invalid log index");
        mcat.addMessage(
                "EW0023",
                "SIMPLEDBM-EW0023: Error occurred while reading Log Record {0} - log header cannot be read");
        mcat.addMessage(
                "EW0024",
                "SIMPLEDBM-EW0024: Log Record {0} has invalid length {1} - possibly garbage");
        mcat.addMessage(
                "EW0025",
                "SIMPLEDBM-EW0025: Error occurred while reading Log Record {0} - read error");
        mcat.addMessage(
            "EW0026",
            "SIMPLEDBM-EW0026: Error occurred while flushing the Log");
        mcat.addMessage(
            "EW0027",
            "SIMPLEDBM-EW0027: Error occurred while archiving a Log File");
        mcat.addMessage("IW0028", "SIMPLEDBM-IW0028: Log Writer STARTED");
        mcat.addMessage("IW0029", "SIMPLEDBM-IW0029: Archive Cleaner STARTED");
        mcat.addMessage("IW0030", "SIMPLEDBM-IW0030: Log Writer STOPPED");
        mcat.addMessage("IW0031", "SIMPLEDBM-IW0031: Archive Cleaner STOPPED");
        mcat.addMessage("IW0032", "SIMPLEDBM-IW0032: Write Ahead Log Manager STOPPED");

	}

    public final void createLog(StorageContainerFactory storageFactory,
            Properties props) throws LogException {
        LogMgrParms parms = new LogMgrParms(props);
        LogManagerImpl logmgr = new LogManagerImpl(po, storageFactory, 
        		parms.logBufferSize, parms.maxLogBuffers, 
        		parms.logFlushInterval, parms.disableExplicitFlushRequests);
        logmgr.setCtlFiles(parms.ctlFiles);
        logmgr.setArchivePath(parms.archivePath);
        logmgr.setArchiveMode(true);
        logmgr.setLogFileSize(parms.logFileSize);
        logmgr.setLogFiles(parms.groupPaths, parms.n_LogFiles);
        logmgr.create();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.simpledbm.log.LogFactory#openLog(java.lang.String)
     */
    public final LogManager getLog(StorageContainerFactory storageFactory,
            Properties props) throws LogException {
        LogMgrParms parms = new LogMgrParms(props);
        LogManagerImpl logmgr = new LogManagerImpl(po, storageFactory, 
        		parms.logBufferSize, parms.maxLogBuffers, 
        		parms.logFlushInterval, parms.disableExplicitFlushRequests);
        logmgr.setCtlFiles(parms.ctlFiles);
        return logmgr;
    }

    /**
     * Encapsulates the set of parameters that need to be supplied when creating
     * or opening a Log.
     * 
     * @author Dibyendu Majumdar
     */
    static final class LogMgrParms {

        String ctlFiles[];

        String groupPaths[];

        short n_LogFiles;

        String archivePath;

        int logBufferSize;

        int logFileSize;

        int maxLogBuffers;

        int logFlushInterval;

        boolean disableExplicitFlushRequests;

        final void setDefaults() {
            ctlFiles = new String[LogManagerImpl.DEFAULT_CTL_FILES];
            for (int i = 0; i < LogManagerImpl.DEFAULT_CTL_FILES; i++) {
                ctlFiles[i] = "ctl." + Integer.toString(i);
            }
            groupPaths = new String[LogManagerImpl.DEFAULT_LOG_GROUPS];
            for (int i = 0; i < LogManagerImpl.DEFAULT_LOG_GROUPS; i++) {
                groupPaths[i] = LogManagerImpl.DEFAULT_GROUP_PATH;
            }
            n_LogFiles = LogManagerImpl.DEFAULT_LOG_FILES;
            archivePath = LogManagerImpl.DEFAULT_ARCHIVE_PATH;
            logBufferSize = LogManagerImpl.DEFAULT_LOG_BUFFER_SIZE;
            logFileSize = LogManagerImpl.DEFAULT_LOG_FILE_SIZE;
            logFlushInterval = 60000;
            maxLogBuffers = n_LogFiles * 10;
            disableExplicitFlushRequests = false;
        }

        LogMgrParms(Properties props) {
            setDefaults();
            if (props == null) {
                return;
            }
            ArrayList<String> list = new ArrayList<String>();
            for (int i = 1; i <= LogManagerImpl.MAX_CTL_FILES; i++) {
                String name = "log.ctl." + String.valueOf(i);
                String value = props.getProperty(name);
                if (value == null) {
                    break;
                }
                list.add(value);
            }
            if (list.size() > 0) {
                ctlFiles = list.toArray(new String[1]);
                list.clear();
            }
            for (int i = 1; i <= LogManagerImpl.MAX_LOG_GROUPS; i++) {
                String name = "log.groups." + String.valueOf(i) + ".path";
                String value = props.getProperty(name);
                if (value == null) {
                    break;
                }
                list.add(value);
            }
            if (list.size() > 0) {
                groupPaths = list.toArray(new String[1]);
            }
            String key = "log.archive.path";
            String value = props.getProperty(key);
            if (value != null) {
                archivePath = value;
            }
            key = "log.group.files";
            value = props.getProperty(key);
            if (value != null) {
                n_LogFiles = Short.parseShort(value);
            }
            key = "log.file.size";
            value = props.getProperty(key);
            if (value != null) {
                logFileSize = Integer.parseInt(value);
            }
            key = "log.buffer.size";
            value = props.getProperty(key);
            if (value != null) {
                logBufferSize = Integer.parseInt(value);
            }
            key = "log.buffer.limit";
            value = props.getProperty(key);
            if (value != null) {
                maxLogBuffers = Integer.parseInt(value);
            }
            key = "log.flush.interval";
            value = props.getProperty(key);
            if (value != null) {
                logFlushInterval = Integer.parseInt(value);
            }
            key = "log.disableFlushRequests";
            value = props.getProperty(key);
            if (value != null) {
                disableExplicitFlushRequests = Boolean.parseBoolean(value);
            }
        }
    }
}
