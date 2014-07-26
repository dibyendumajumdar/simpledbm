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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.util.Dumpable;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageException;

/**
 * Implements a File based StorageContainer.
 * 
 * @author Dibyendu Majumdar
 * @since 24-Jun-2005
 */
public final class FileStorageContainer implements StorageContainer, Dumpable {

    @SuppressWarnings("unused")
    private final Logger log;

    private final ExceptionHandler exceptionHandler;

    /**
     * The underlying file object.
     */
    private final RandomAccessFile file;

    private final String name;

    private final String flushMode;

    private FileLock lock;

    // storage manager messages
    static Message m_ES0001 = new Message('R', 'S', MessageType.ERROR, 1,
            "StorageContainer {0} is not valid");
    static Message m_ES0003 = new Message('R', 'S', MessageType.ERROR, 3,
            "Error occurred while writing to StorageContainer {0}");
    static Message m_ES0004 = new Message('R', 'S', MessageType.ERROR, 4,
            "Error occurred while reading from StorageContainer {0}");
    static Message m_ES0005 = new Message('R', 'S', MessageType.ERROR, 5,
            "Error occurred while flushing StorageContainer {0}");
    static Message m_ES0006 = new Message('R', 'S', MessageType.ERROR, 6,
            "Error occurred while closing StorageContainer {0}");
    static Message m_ES0007 = new Message('R', 'S', MessageType.ERROR, 7,
            "StorageContainer {0} is already locked");
    static Message m_ES0008 = new Message('R', 'S', MessageType.ERROR, 8,
            "An exclusive lock could not be obtained on StorageContainer {0}");
    static Message m_ES0009 = new Message('R', 'S', MessageType.ERROR, 9,
            "StorageContainer {0} is not locked");
    static Message m_ES0010 = new Message('R', 'S', MessageType.ERROR, 10,
            "Error occurred while releasing lock on StorageContainer {0}");

    /**
     * Creates a new FileStorageContainer from an existing file object.
     * 
     * @param file Existing file object.
     */
    FileStorageContainer(PlatformObjects po, String name,
            RandomAccessFile file, String flushMode) {
        this.log = po.getLogger();
        this.exceptionHandler = po.getExceptionHandler();
        this.name = name;
        this.file = file;
        this.flushMode = flushMode;
    }

    /**
     * Checks if the file is available for reading and writing.
     * 
     * @throws StorageException Thrown if the file has been closed.
     */
    private void isValid() throws StorageException {
        if (file == null || !file.getChannel().isOpen()) {
            exceptionHandler.errorThrow(getClass(), "isValid",
                    new StorageException(new MessageInstance(m_ES0001, name)));
        }
    }

    public final synchronized void write(long position,
                                         byte[] data,
                                         int offset,
                                         int length) {
        isValid();
        try {
            file.seek(position);
            file.write(data, offset, length);
        } catch (IOException e) {
            exceptionHandler
                    .errorThrow(getClass(), "write",
                            new StorageException(new MessageInstance(m_ES0003,
                                    name), e));
        }
    }

    public final synchronized int read(long position,
                                       byte[] data,
                                       int offset,
                                       int length) {
        isValid();
        int n = 0;
        try {
            file.seek(position);
            n = file.read(data, offset, length);
        } catch (IOException e) {
            exceptionHandler
                    .errorThrow(getClass(), "read",
                            new StorageException(new MessageInstance(m_ES0004,
                                    name), e));
        }
        return n;
    }

    public final synchronized void flush() {
        isValid();
        try {
            // FIXME hard coded values
            if ("force.true".equals(flushMode)) {
                file.getChannel().force(true);
            } else if ("force.false".equals(flushMode)) {
                file.getChannel().force(false);
            }
        } catch (IOException e) {
            exceptionHandler
                    .errorThrow(getClass(), "flush",
                            new StorageException(new MessageInstance(m_ES0005,
                                    name), e));
        }
    }

    public final synchronized void close() {
        isValid();
        try {
            file.close();
        } catch (IOException e) {
            exceptionHandler
                    .errorThrow(getClass(), "close",
                            new StorageException(new MessageInstance(m_ES0006,
                                    name), e));
        }
    }

    public final synchronized void lock() {
        isValid();
        if (lock != null) {
            exceptionHandler.errorThrow(this.getClass(), "lock",
                    new StorageException(new MessageInstance(m_ES0007, name)));
        }
        try {
            FileChannel channel = file.getChannel();
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException e) {
                // ignore this error
            }
            if (lock == null) {
                exceptionHandler.errorThrow(this.getClass(), "lock",
                        new StorageException(
                                new MessageInstance(m_ES0008, name)));
            }
        } catch (IOException e) {
            exceptionHandler
                    .errorThrow(this.getClass(), "lock",
                            new StorageException(new MessageInstance(m_ES0008,
                                    name), e));
        }
    }

    public final synchronized void unlock() {
        isValid();
        if (lock == null) {
            exceptionHandler.errorThrow(this.getClass(), "lock",
                    new StorageException(new MessageInstance(m_ES0009, name)));
        }
        try {
            lock.release();
            lock = null;
        } catch (IOException e) {
            exceptionHandler
                    .errorThrow(this.getClass(), "lock",
                            new StorageException(new MessageInstance(m_ES0010,
                                    name), e));
        }
    }

    public final String getName() {
        return name;
    }

    public final StringBuilder appendTo(StringBuilder sb) {
        sb.append("FileStorageContainer(name=").append(name).append(", file=")
                .append(file).append(")");
        return sb;
    }

    @Override
    public final String toString() {
        return appendTo(new StringBuilder()).toString();
    }
}
