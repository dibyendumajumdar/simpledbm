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

import java.util.ArrayList;
import java.util.Properties;

import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.wal.LogException;
import org.simpledbm.rss.api.wal.LogFactory;
import org.simpledbm.rss.api.wal.LogManager;
import org.simpledbm.rss.impl.st.FileStorageContainerFactory;

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

	public final void createLog(StorageContainerFactory storageFactory,
			Properties props) throws LogException {
		LogMgrParms parms = new LogMgrParms(props);
		LogManagerImpl logmgr = new LogManagerImpl(storageFactory);
		logmgr.setCtlFiles(parms.ctlFiles);
		logmgr.setArchivePath(parms.archivePath);
		logmgr.setArchiveMode(true);
		logmgr.setLogBufferSize(parms.logBufferSize);
		logmgr.setLogFileSize(parms.logFileSize);
		logmgr.setLogFiles(parms.groupPaths, parms.n_LogFiles);
		logmgr.setMaxBuffers(parms.maxLogBuffers);
		logmgr.setLogFlushInterval(parms.logFlushInterval);
		logmgr.setDisableExplicitFlushRequests(parms.disableExplicitFlushRequests);
		logmgr.create();
	}
	
	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.log.LogFactory#createLog(java.lang.String)
	 */
	public final void createLog(Properties props) throws LogException {
		createLog(new FileStorageContainerFactory(props), props);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.log.LogFactory#openLog(java.lang.String)
	 */
	public final LogManager getLog(StorageContainerFactory storageFactory, Properties props) throws LogException {
		LogMgrParms parms = new LogMgrParms(props);
		LogManagerImpl logmgr = new LogManagerImpl(storageFactory);
		logmgr.setCtlFiles(parms.ctlFiles);
		return logmgr;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.log.LogFactory#openLog(java.lang.String)
	 */
	public final LogManager getLog(Properties props) throws LogException {
		return getLog(new FileStorageContainerFactory(props), props);
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
			key = "log.disableExplicitFlushRequests";
			value = props.getProperty(key);
			if (value != null) {
				disableExplicitFlushRequests = Boolean.parseBoolean(value);
			}
		}
	}
}
