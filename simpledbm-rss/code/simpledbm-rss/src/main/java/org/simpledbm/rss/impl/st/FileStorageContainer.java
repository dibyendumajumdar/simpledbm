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
            exceptionHandler.errorThrow(this.getClass().getName(), "isValid",
                    new StorageException(new MessageInstance(m_ES0001, name)));
        }
    }

    /*
     * (non-Javadoc)
     * @see org.simpledbm.io.StorageContainer#write(long, byte[], int, int)
     */
    public final synchronized void write(long position, byte[] data,
            int offset, int length) throws StorageException {
        isValid();
        try {
            file.seek(position);
            file.write(data, offset, length);
        } catch (IOException e) {
            exceptionHandler
                    .errorThrow(this.getClass().getName(), "write",
                            new StorageException(new MessageInstance(m_ES0003,
                                    name), e));
        }
    }

    /*
     * (non-Javadoc)
     * @see org.simpledbm.io.StorageContainer#read(long, byte[], int, int)
     */
    public final synchronized int read(long position, byte[] data, int offset,
            int length) throws StorageException {
        isValid();
        int n = 0;
        try {
            file.seek(position);
            n = file.read(data, offset, length);
        } catch (IOException e) {
            exceptionHandler
                    .errorThrow(this.getClass().getName(), "read",
                            new StorageException(new MessageInstance(m_ES0004,
                                    name), e));
        }
        return n;
    }

    /*
     * (non-Javadoc)
     * @see org.simpledbm.io.StorageContainer#flush()
     */
    public final synchronized void flush() throws StorageException {
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
                    .errorThrow(this.getClass().getName(), "flush",
                            new StorageException(new MessageInstance(m_ES0005,
                                    name), e));
        }
    }

    /*
     * (non-Javadoc)
     * @see org.simpledbm.io.StorageContainer#close()
     */
    public final synchronized void close() throws StorageException {
        isValid();
        try {
            file.close();
        } catch (IOException e) {
            exceptionHandler
                    .errorThrow(this.getClass().getName(), "close",
                            new StorageException(new MessageInstance(m_ES0006,
                                    name), e));
        }
    }

    public final synchronized void lock() {
        isValid();
        if (lock != null) {
            exceptionHandler.errorThrow(this.getClass().getName(), "lock",
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
                exceptionHandler.errorThrow(this.getClass().getName(), "lock",
                        new StorageException(
                                new MessageInstance(m_ES0008, name)));
            }
        } catch (IOException e) {
            exceptionHandler
                    .errorThrow(this.getClass().getName(), "lock",
                            new StorageException(new MessageInstance(m_ES0008,
                                    name), e));
        }
    }

    public final synchronized void unlock() {
        isValid();
        if (lock == null) {
            exceptionHandler.errorThrow(this.getClass().getName(), "lock",
                    new StorageException(new MessageInstance(m_ES0009, name)));
        }
        try {
            lock.release();
            lock = null;
        } catch (IOException e) {
            exceptionHandler
                    .errorThrow(this.getClass().getName(), "lock",
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

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public final String toString() {
        return appendTo(new StringBuilder()).toString();
    }
}
