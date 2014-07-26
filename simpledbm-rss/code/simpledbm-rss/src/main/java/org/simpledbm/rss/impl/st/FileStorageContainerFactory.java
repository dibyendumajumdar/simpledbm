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
package org.simpledbm.rss.impl.st;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Properties;

import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageException;

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

    final PlatformObjects po;

    /**
     * Mode for creating new container objects. This should be configurable.
     */
    private final String createMode;
    private static final String CREATE_MODE = "storage.createMode";
    private static final String defaultCreateMode = "rws";

    /**
     * Mode for opening existing container objects. This should be configurable.
     */
    private final String openMode;
    private static final String OPEN_MODE = "storage.openMode";
    private static final String defaultOpenMode = "rws";

    /**
     * Base path for containers. All containers will be created relative to the
     * base path.
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

    // storage manager messages
    static Message m_ES0011 = new Message('R', 'S', MessageType.ERROR, 11,
            "Directory specified by {0}={1} does not exist");
    static Message m_ES0012 = new Message('R', 'S', MessageType.ERROR, 12,
            "Error creating directory specified by {0}={1}");
    static Message m_ES0013 = new Message('R', 'S', MessageType.ERROR, 13,
            "Specified base path {0}={1} is not a directory or is not accessible");
    static Message m_ES0014 = new Message('R', 'S', MessageType.ERROR, 14,
            "Path name {0} must be a directory");
    static Message m_ES0015 = new Message('R', 'S', MessageType.ERROR, 15,
            "Error creating directory {0}");
    static Message m_ES0016 = new Message('R', 'S', MessageType.ERROR, 16,
            "Unable to delete StorageContainer {0}");
    static Message m_ES0017 = new Message(
            'R',
            'S',
            MessageType.ERROR,
            17,
            "Unable to create StorageContainer {0} because an object of the name already exists");
    static Message m_ES0018 = new Message('R', 'S', MessageType.ERROR, 18,
            "Unexpected error occurred while creating StorageContainer {0}");
    static Message m_ES0019 = new Message('R', 'S', MessageType.ERROR, 19,
            "StorageContainer {0} does not exist or is not accessible");
    static Message m_ES0020 = new Message('R', 'S', MessageType.ERROR, 20,
            "Unexpected error occurred while opening StorageContainer {0}");
    static Message m_ES0021 = new Message('R', 'S', MessageType.ERROR, 21,
            "Unable to delete {0} as named object is not a StorageContainer");
    static Message m_ES0024 = new Message('R', 'S', MessageType.ERROR, 24,
            "Unable to delete path name {0}");

    public FileStorageContainerFactory(Platform platform, Properties props) {
        po = platform.getPlatformObjects(StorageContainerFactory.LOGGER_NAME);
        log = po.getLogger();
        exceptionHandler = po.getExceptionHandler();
        basePath = props.getProperty(BASE_PATH, defaultBasePath);
        createMode = props.getProperty(CREATE_MODE, defaultCreateMode);
        openMode = props.getProperty(OPEN_MODE, defaultOpenMode);
        flushMode = props.getProperty(FLUSH_MODE, DEFAULT_FLUSH_MODE);
    }

    public FileStorageContainerFactory(Platform platform) {
        po = platform.getPlatformObjects(StorageContainerFactory.LOGGER_NAME);
        log = po.getLogger();
        exceptionHandler = po.getExceptionHandler();
        basePath = defaultBasePath;
        createMode = defaultCreateMode;
        openMode = defaultOpenMode;
        flushMode = DEFAULT_FLUSH_MODE;
    }

    public void init() {
        checkBasePath(true);
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
                exceptionHandler.errorThrow(getClass(),
                        "checkBasePath", new StorageException(
                                new MessageInstance(m_ES0011, BASE_PATH,
                                        basePath)));
            }
            if (log.isDebugEnabled()) {
                log.debug(getClass(), "checkBasePath",
                        "SIMPLEDBM-DEBUG: Creating base path " + basePath);
            }
            if (!file.mkdirs()) {
                exceptionHandler.errorThrow(getClass(),
                        "checkBasePath", new StorageException(
                                new MessageInstance(m_ES0012, BASE_PATH,
                                        basePath)));
            }
        }
        if (!file.isDirectory() || !file.canRead() || !file.canWrite()) {
            exceptionHandler.errorThrow(getClass(),
                    "checkBasePath", new StorageException(new MessageInstance(
                            m_ES0013, BASE_PATH, basePath)));
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
                    exceptionHandler.errorThrow(getClass(),
                            "getFileName", new StorageException(
                                    new MessageInstance(m_ES0014, parentFile
                                            .getPath())));
                }
            } else {
                if (log.isDebugEnabled()) {
                    log.debug(getClass(), "getFileName",
                            "SIMPLEDBM-DEBUG: Creating path "
                                    + parentFile.getPath());
                }
                if (!parentFile.mkdirs()) {
                    exceptionHandler.errorThrow(getClass(),
                            "getFileName", new StorageException(
                                    new MessageInstance(m_ES0015, parentFile
                                            .getPath())));
                }
            }
        }
        return s;
    }

    /**
     * Creates a new File based Storage Container object. If a container of the
     * same name already exists, it is over-written. By default the container is
     * opened in read/write mode.
     */
    public final StorageContainer createIfNotExisting(String logicalName)
            throws StorageException {
        if (log.isDebugEnabled()) {
            log
                    .debug(getClass(), "create",
                            "SIMPLEDBM-DEBUG: Creating StorageContainer "
                                    + logicalName);
        }
        checkBasePath(true);
        String name = getFileName(logicalName, true);
        RandomAccessFile rafile = null;
        File file = new File(name);
        try {
            // Create the file atomically.
            if (!file.createNewFile()) {
                exceptionHandler.errorThrow(getClass(),
                        "create", new StorageException(new MessageInstance(
                                m_ES0017, name)));
            }
            rafile = new RandomAccessFile(name, createMode);
        } catch (IOException e) {
            exceptionHandler
                    .errorThrow(getClass(), "create",
                            new StorageException(new MessageInstance(m_ES0018,
                                    name), e));
        }
        return new FileStorageContainer(po, logicalName, rafile, flushMode);
    }

    /**
     * Creates a new File based Storage Container object. If a container of the
     * same name already exists, it is over-written. By default the container is
     * opened in read/write mode.
     */
    public final StorageContainer create(String logicalName)
            throws StorageException {
        if (log.isDebugEnabled()) {
            log
                    .debug(getClass(), "create",
                            "SIMPLEDBM-DEBUG: Creating StorageContainer "
                                    + logicalName);
        }
        checkBasePath(true);
        String name = getFileName(logicalName, true);
        RandomAccessFile rafile = null;
        File file = new File(name);
        try {
            if (file.exists()) {
                if (file.isFile()) {
                    if (!file.delete()) {
                        exceptionHandler.errorThrow(getClass(),
                                "create", new StorageException(
                                        new MessageInstance(m_ES0016, name)));
                    }
                } else {
                    exceptionHandler.errorThrow(getClass(),
                            "create", new StorageException(new MessageInstance(
                                    m_ES0017, name)));
                }
            }
            rafile = new RandomAccessFile(name, createMode);
        } catch (IOException e) {
            exceptionHandler
                    .errorThrow(getClass(), "create",
                            new StorageException(new MessageInstance(m_ES0018,
                                    name), e));
        }
        return new FileStorageContainer(po, logicalName, rafile, flushMode);
    }

    /**
     * <p>
     * Opens an existing File based Storage Container object. If a container of
     * the specified name does not exist, an Exception is thrown. By default the
     * container is opened in read/write mode.
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
                exceptionHandler.errorThrow(getClass(), "open",
                        new StorageException(
                                new MessageInstance(m_ES0019, name)));
            }
            rafile = new RandomAccessFile(name, openMode);
        } catch (FileNotFoundException e) {
            exceptionHandler
                    .errorThrow(getClass(), "open",
                            new StorageException(new MessageInstance(m_ES0020,
                                    name), e));
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
                    exceptionHandler.errorThrow(getClass(),
                            "delete", new StorageException(new MessageInstance(
                                    m_ES0016, name)));
                }
            } else {
                exceptionHandler.errorThrow(getClass(),
                        "delete", new StorageException(new MessageInstance(
                                m_ES0021, name)));
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
                        log.debug(getClass(),
                                "deleteRecursively",
                                "SIMPLEDBM-DEBUG: Deleting "
                                        + file.getAbsolutePath());
                    }
                    if (!file.delete()) {
                        exceptionHandler.errorThrow(getClass(),
                                "deleteRecursively", new StorageException(
                                        new MessageInstance(m_ES0016, file
                                                .getAbsolutePath())));
                    }
                }
            }
        }
        if (log.isDebugEnabled()) {
            log.debug(getClass(), "deleteRecursively",
                    "SIMPLEDBM-DEBUG: Deleting " + dir.getAbsolutePath());
        }
        if (!dir.delete()) {
            exceptionHandler.errorThrow(getClass(),
                    "deleteRecursively",
                    new StorageException(new MessageInstance(m_ES0024, dir
                            .getAbsolutePath())));
        }
    }

    public void drop() {
        checkBasePath(false);
        File file = new File(basePath);
        deleteRecursively(file);
    }
}
