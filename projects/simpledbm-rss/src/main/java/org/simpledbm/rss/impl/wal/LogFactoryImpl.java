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
package org.simpledbm.rss.impl.wal;

import java.util.ArrayList;
import java.util.Properties;

import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.wal.LogException;
import org.simpledbm.rss.api.wal.LogFactory;
import org.simpledbm.rss.api.wal.LogManager;

/**
 * Factory for creating LogMgr instances. The following parameters can specified
 * in the {@link #createLog} and {@link #getLog} methods.
 * <p>
 * <table border="1">
 * <caption>LogMgr Parameters</caption>
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
 * <td>The path where log files of a group should be stored. The first log group
 * is specified as log.groups.1.path, the second as log.groups.2.path, and so
 * on. Upto a maximum of {@link LogManagerImpl#MAX_LOG_GROUPS} log groups can be
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
 * <td>Sets a limit on the maximum number of log buffers that can be allocated.</td>
 * </tr>
 * <tr>
 * <td>log.flush.interval</td>
 * <td>Sets the interval (in seconds) between log flushes.</td>
 * </tr>
 * </table>
 * 
 * @author Dibyendu Majumdar
 * @since 26-Jun-2005
 */
public final class LogFactoryImpl implements LogFactory {

    final Platform platform;
    final PlatformObjects po;
    final Properties properties;
    final StorageContainerFactory storageContainerFactory;

    public LogFactoryImpl(Platform platform, StorageContainerFactory storageContainerFactory, Properties props) {
        this.platform = platform;
        this.po = platform.getPlatformObjects(LogManager.LOGGER_NAME);
        this.properties = props;
        this.storageContainerFactory = storageContainerFactory;
    }

    public final void createLog() throws LogException {
        LogMgrParms parms = new LogMgrParms(properties);
        LogManagerImpl logmgr = new LogManagerImpl(po, storageContainerFactory,
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
    public final LogManager getLog() throws LogException {
        LogMgrParms parms = new LogMgrParms(properties);
        LogManagerImpl logmgr = new LogManagerImpl(po, storageContainerFactory,
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
