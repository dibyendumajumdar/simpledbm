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
package org.simpledbm.rss.impl.st;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Properties;

import org.simpledbm.rss.api.exception.ExceptionHandler;
import org.simpledbm.rss.api.platform.Platform;
import org.simpledbm.rss.api.platform.PlatformObjects;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageException;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;

/**
 * Factory for creating instances of File based StorageContainer objects.
 * 
 * @author Dibyendu Majumdar
 * @since 24-Jun-2005
 */
public final class FileStorageContainerFactory implements
        StorageContainerFactory {

    final Logger log;

    final ExceptionHandler exceptionHandler;
    
    final MessageCatalog mcat;
    
    final PlatformObjects po;
    
    /**
     * Mode for creating new container objects. This should be
     * configurable.
     */
    private final String createMode;
    private static final String CREATE_MODE = "storage.createMode";
    private static final String defaultCreateMode = "rws";

    /**
     * Mode for opening existing container objects. This should be 
     * configurable.
     */
    private final String openMode;
    private static final String OPEN_MODE = "storage.openMode";
    private static final String defaultOpenMode = "rws";

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

    /**
     * Values can be noforce, force.true or force.false.
     */
    private final String flushMode;
    /**
     * Default flush mode
     */
    private static final String FLUSH_MODE = "storage.flushMode";
    private static final String DEFAULT_FLUSH_MODE = "force.true";

    public FileStorageContainerFactory(Platform platform, Properties props) {
    	po = platform.getPlatformObjects(StorageContainerFactory.LOGGER_NAME);
    	log = po.getLogger();
    	exceptionHandler = po.getExceptionHandler();
    	mcat = po.getMessageCatalog();
        basePath = props.getProperty(BASE_PATH, defaultBasePath);
        createMode = props.getProperty(CREATE_MODE, defaultCreateMode);
        openMode = props.getProperty(OPEN_MODE, defaultOpenMode);
        flushMode = props.getProperty(FLUSH_MODE, DEFAULT_FLUSH_MODE);
    }

    public FileStorageContainerFactory(Platform platform) {
    	po = platform.getPlatformObjects(StorageContainerFactory.LOGGER_NAME);
    	log = po.getLogger();
    	exceptionHandler = po.getExceptionHandler();
    	mcat = po.getMessageCatalog();
    	basePath = defaultBasePath;
        createMode = defaultCreateMode;
        openMode = defaultOpenMode;
        flushMode = DEFAULT_FLUSH_MODE;
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
            	exceptionHandler.errorThrow(this.getClass().getName(),
						"checkBasePath", new StorageException(mcat.getMessage(
								"ES0011", BASE_PATH, basePath)));
            }
            if (log.isDebugEnabled()) {
                log.debug(
                    this.getClass().getName(),
                    "checkBasePath",
                    "SIMPLEDBM-DEBUG: Creating base path " + basePath);
            }
            if (!file.mkdirs()) {
				exceptionHandler.errorThrow(this.getClass().getName(),
						"checkBasePath", new StorageException(mcat.getMessage(
								"ES0012", BASE_PATH, basePath)));
			}
        }
        if (!file.isDirectory() || !file.canRead() || !file.canWrite()) {
			exceptionHandler.errorThrow(this.getClass().getName(),
					"checkBasePath", new StorageException(mcat.getMessage(
							"ES0013", BASE_PATH, basePath)));
		}
        basePathVerified = true;
    }

    /**
     * Converts a logical name to a file name that. Optionally creates the path
     * to the file.
     */
    private String getFileName(String name, boolean checkParent)
            throws StorageException {
        File file = new File(basePath, name);
        String s = file.getPath();
        if (checkParent) {
            File parentFile = file.getParentFile();
            if (parentFile.exists()) {
                if (!parentFile.isDirectory() || !parentFile.canWrite()
                        || !parentFile.canRead()) {
                    exceptionHandler.errorThrow(this.getClass().getName(),
							"getFileName",
							new StorageException(mcat.getMessage("ES0014",
									parentFile.getPath())));
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug(
                        this.getClass().getName(),
                        "getFileName",
                        "SIMPLEDBM-DEBUG: Creating path "
                                + parentFile.getPath());
                }
                if (!parentFile.mkdirs()) {
                    exceptionHandler.errorThrow(this.getClass().getName(),
							"getFileName",
							new StorageException(mcat.getMessage("ES0015",
									parentFile.getPath())));
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
    public final StorageContainer createIfNotExisting(String logicalName)
            throws StorageException {
        if (log.isDebugEnabled()) {
            log.debug(
                this.getClass().getName(),
                "create",
                "SIMPLEDBM-DEBUG: Creating StorageContainer " + logicalName);
        }
        checkBasePath(true);
        String name = getFileName(logicalName, true);
        RandomAccessFile rafile = null;
        File file = new File(name);
        try {
            // Create the file atomically.
            if (!file.createNewFile()) {
                exceptionHandler.errorThrow(this.getClass().getName(), "create", 
                		new StorageException(mcat.getMessage(
                        "ES0017",
                        name)));
            }
            rafile = new RandomAccessFile(name, createMode);
        } catch (IOException e) {
            exceptionHandler.errorThrow(this.getClass().getName(), "create",
					new StorageException(mcat.getMessage("ES0018", name), e));
        }
        return new FileStorageContainer(po, logicalName, rafile, flushMode);
    }

    /**
     * Creates a new File based Storage Container object. If a container
     * of the same name already exists, it is over-written. By default
     * the container is opened in read/write mode.
     */
    public final StorageContainer create(String logicalName)
            throws StorageException {
        if (log.isDebugEnabled()) {
            log.debug(
                this.getClass().getName(),
                "create",
                "SIMPLEDBM-DEBUG: Creating StorageContainer " + logicalName);
        }
        checkBasePath(true);
        String name = getFileName(logicalName, true);
        RandomAccessFile rafile = null;
        File file = new File(name);
        try {
            if (file.exists()) {
                if (file.isFile()) {
                    if (!file.delete()) {
                        exceptionHandler.errorThrow(this.getClass().getName(),
								"create", new StorageException(mcat.getMessage(
										"ES0016", name)));
                    }
                } else {
                    exceptionHandler.errorThrow(this.getClass().getName(),
							"create", new StorageException(mcat.getMessage(
									"ES0017", name)));
                }
            }
            rafile = new RandomAccessFile(name, createMode);
        } catch (IOException e) {
            exceptionHandler.errorThrow(this.getClass().getName(), "create", 
            		new StorageException(mcat.getMessage("ES0018", name), e));
        }
        return new FileStorageContainer(po, logicalName, rafile, flushMode);
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
            if (!file.exists() || !file.isFile() || !file.canRead()
                    || !file.canWrite()) {
                exceptionHandler.errorThrow(this.getClass().getName(), "open", 
                		new StorageException(mcat.getMessage("ES0019", name)));
            }
            rafile = new RandomAccessFile(name, openMode);
        } catch (FileNotFoundException e) {
            exceptionHandler.errorThrow(this.getClass().getName(), "open", 
            		new StorageException(mcat.getMessage("ES0020", name), e));
        }
        return new FileStorageContainer(po, logicalName, rafile, flushMode);
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
                    exceptionHandler.errorThrow(this.getClass().getName(), "delete", 
                    		new StorageException(mcat.getMessage("ES0016", name)));
                }
            } else {
                exceptionHandler.errorThrow(this.getClass().getName(), "delete", 
                		new StorageException(mcat.getMessage("ES0021", name)));
            }
        }
    }

	/* (non-Javadoc)
	 * @see org.simpledbm.rss.api.st.StorageContainerFactory#exists(java.lang.String)
	 */
	public boolean exists(String logicalName) {
        checkBasePath(false);
        String name = getFileName(logicalName, false);
        File file = new File(name);
		return file.exists();
	}

	private void deleteRecursively(File dir) {
		if (dir.isDirectory()) {
			File[] files = dir.listFiles();
			for (File file : files) {
				if (file.isDirectory()) {
					deleteRecursively(file);
				} else {
			        if (log.isDebugEnabled()) {
			            log.debug(
			                this.getClass().getName(),
			                "deleteRecursively",
			                "SIMPLEDBM-DEBUG: Deleting " + file.getAbsolutePath());
			        }
					if (!file.delete()) {
						exceptionHandler.errorThrow(this.getClass().getName(),
								"deleteRecursively", new StorageException(mcat.getMessage("ES0016",
								file.getAbsolutePath())));
					}
				}
			}
		}
        if (log.isDebugEnabled()) {
            log.debug(
                this.getClass().getName(),
                "deleteRecursively",
                "SIMPLEDBM-DEBUG: Deleting " + dir.getAbsolutePath());
        }
		if (!dir.delete()) {
			exceptionHandler.errorThrow(this.getClass().getName(), "deleteRecursively", 
					new StorageException(mcat.getMessage("ES0024", dir
					.getAbsolutePath())));
		}
	}
	
	/* (non-Javadoc)
	 * @see org.simpledbm.rss.api.st.StorageContainerFactory#deleteTree(java.lang.String)
	 */
	public void delete() {
        checkBasePath(false);
		File file = new File(basePath);
		deleteRecursively(file);
	}
}
