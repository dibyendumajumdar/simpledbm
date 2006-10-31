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
package org.simpledbm.rss.impl.st;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.Properties;

import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageException;
import org.simpledbm.rss.util.logging.Logger;

/**
 * Factory for creating instances of File based StorageContainer objects.
 * 
 * @author Dibyendu Majumdar
 * @since 24-Jun-2005
 */
public final class FileStorageContainerFactory implements
        StorageContainerFactory {

	static final String LOG_CLASS_NAME = FileStorageContainerFactory.class.getName();

	static final Logger log = Logger.getLogger(FileStorageContainerFactory.class.getPackage().getName());

	/**
	 * Mode for creating new container objects. This should be
	 * configurable.
	 */
	private final String createMode = "rws";
	
	/**
	 * Mode for openeing existing container objects. This should be 
	 * configurable.
	 */
	private final String openMode = "rws";

	/**
	 * Base path for containers. All containers will be
	 * created relative to the base path.
	 */
	private final String basePath;
	
	/**
	 * Name of the property that sets the base path.
	 */
	public static final String BASE_PATH = "storage.basePath";
	
	/**
	 * Default base path is the current directory.
	 */
	private static final String defaultBasePath = ".";
	
	/**
	 * Flags to indicate whether the base path has been verified.
	 */
	private boolean basePathVerified = false;
	
	public FileStorageContainerFactory(Properties props) {
		basePath = props.getProperty(BASE_PATH, defaultBasePath);
	}
	
	public FileStorageContainerFactory() {
		basePath = defaultBasePath;
	}

	/**
	 * Checks the existence of the base path. Optionally creates the base path.
	 */
	private void checkBasePath(boolean create) throws StorageException {
		if (basePathVerified) {
			return;
		}
		File file = new File(basePath);
		if (!file.exists()) {
			if (!create) {
				throw new StorageException("SIMPLEDBM-EIO: Directory specified by " + basePath + " does not exist");
			}
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "checkBasePath", "SIMPLEDBM-LOG: Creating base path " + basePath);
			}
			if (!file.mkdirs()) {
				throw new StorageException("SIMPLEDBM-EIO: Unable to create the directory specified by " + basePath);
			}
		}
		if (!file.isDirectory() || !file.canRead() || !file.canWrite()) {
			throw new StorageException("SIMPLEDBM-EIO: Specified base path " + basePath + " is not accessible or is not a directory");
		}
		basePathVerified = true;
	}

	/**
	 * Converts a logical name to a file name that. Optionally creates the path
	 * to the file.
	 */
	private String getFileName(String name, boolean checkParent) throws StorageException {
		File file = new File(basePath, name);
		String s = file.getPath();
		if (checkParent) {
			File parentFile = file.getParentFile();
			if (parentFile.exists()) {
				if (!parentFile.isDirectory() || !parentFile.canWrite() || !parentFile.canRead()) {
					throw new StorageException("SIMPLEDBM-EIO: Path name " + parentFile.getPath() + " must be a directory");
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug(LOG_CLASS_NAME, "getFileName", "SIMPLEDBM-LOG: Creating path " + parentFile.getPath());
				}
				if (!parentFile.mkdirs()) {
					throw new StorageException("SIMPLEDBM-EIO: Failed to create path " + parentFile.getPath());
				}
			}
		}
		return s;
	}
	
	
	/**
	 * Creates a new File based Storage Container object. If a container
	 * of the same name already exists, it is over-written. By default
	 * the container is opened in read/write mode.
	 */
    public final StorageContainer create(String logicalName)
            throws StorageException {
    	checkBasePath(true);
    	String name = getFileName(logicalName, true);
        RandomAccessFile rafile = null;
        File file = new File(name);
        try {
            if (file.exists()) {
                if (file.isFile()) {
                    if (!file.delete()) {
                        throw new StorageException("SIMPLEDBM-EIO-101: Unable to delete existing Storage Container - " + name);
                    }
                }
                else {
                    throw new StorageException("SIMPLEDBM-EIO-102: An object of this name already exists - " + name);
                }
            }
            rafile = new RandomAccessFile(name, createMode);
        }
        catch (FileNotFoundException e) {
            throw new StorageException("SIMPLEDBM-EIO-103: Unexpected error while creating Storage Container - " + name, e);
        }
        return new FileStorageContainer(logicalName, rafile);
    }

	/**
	 * <p>
	 * Opens an existing File based Storage Container object. If a container
	 * of the specified name does not exist, an Exception is thrown. By default
	 * the container is opened in read/write mode.
	 * </p>
	 */
    public final StorageContainer open(String logicalName)
            throws StorageException {
    	checkBasePath(false);
    	String name = getFileName(logicalName, false);
        RandomAccessFile rafile = null;
        File file = new File(name);
        try {
            if (!file.exists() || !file.isFile() || !file.canRead() || !file.canWrite()) {
                throw new StorageException("SIMPLEDBM-EIO-104: Storage Container either does not exist or is not accessible - " + name);
            }
            rafile = new RandomAccessFile(name, openMode);
        }
        catch (FileNotFoundException e) {
            throw new StorageException("SIMPLEDBM-EIO-105: Unexpected error while opening Storage Container - " + name, e);
        }
        return new FileStorageContainer(logicalName, rafile);
    }

    /**
     * @see org.simpledbm.rss.api.st.StorageContainerFactory#delete(java.lang.String)
     */
    public void delete(String logicalName) throws StorageException {
    	checkBasePath(false);
    	String name = getFileName(logicalName, false);
        File file = new File(name);
        if (file.exists()) {
            if (file.isFile()) {
                if (!file.delete()) {
                    throw new StorageException(
                            "SIMPLEDBM-EIO-101: Unable to delete Storage Container - "
                                    + name);
                }
            } else {
                throw new StorageException(
                        "SIMPLEDBM-EIO-102: Named object is not a storage container - "
                                + name);
            }
        }
    }
}
