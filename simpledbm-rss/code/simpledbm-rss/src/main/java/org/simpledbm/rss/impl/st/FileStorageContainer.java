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
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.rss.impl.st;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import org.simpledbm.rss.api.exception.ExceptionHandler;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageException;
import org.simpledbm.rss.util.Dumpable;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;

/**
 * Implements a File based StorageContainer.
 * 
 * @author Dibyendu Majumdar
 * @since 24-Jun-2005
 */
public final class FileStorageContainer implements StorageContainer, Dumpable {

    private static final Logger log = Logger
        .getLogger(StorageContainerFactory.LOGGER_NAME);

    private static final ExceptionHandler exceptionHandler = ExceptionHandler.getExceptionHandler(log);
    
    private static final MessageCatalog mcat = MessageCatalog.getMessageCatalog();

    /**
     * The underlying file object.
     */
    private final RandomAccessFile file;

    private final String name;
    
    private final String flushMode;

    private FileLock lock;

    /**
     * Creates a new FileStorageContainer from an existing
     * file object.
     * @param file Existing file object.
     */
    FileStorageContainer(String name, RandomAccessFile file, String flushMode) {
        this.name = name;
        this.file = file;
        this.flushMode = flushMode;
    }

    /**
     * Checks if the file is available for reading and writing.
     * @throws StorageException Thrown if the file has been closed.
     */
    private void isValid() throws StorageException {
        if (file == null || !file.getChannel().isOpen()) {
            exceptionHandler.errorThrow(this.getClass().getName(), "isValid", 
            		new StorageException(mcat.getMessage("ES0001", name)));
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
            exceptionHandler.errorThrow(this.getClass().getName(), "write", 
            		new StorageException(mcat.getMessage("ES0003", name), e));
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
            exceptionHandler.errorThrow(this.getClass().getName(), "read", 
            		new StorageException(mcat.getMessage("ES0004", name), e));
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
        	}
        	else if ("force.false".equals(flushMode)) {
        		file.getChannel().force(false);
        	}
        } catch (IOException e) {
            exceptionHandler.errorThrow(this.getClass().getName(), "flush", 
            		new StorageException(mcat.getMessage("ES0005", name), e));
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
            exceptionHandler.errorThrow(this.getClass().getName(), "close", 
            		new StorageException(mcat.getMessage("ES0006", name), e));
        }
    }

    public final synchronized void lock() {
        isValid();
        if (lock != null) {
            exceptionHandler.errorThrow(this.getClass().getName(), "lock", 
            		new StorageException(mcat.getMessage("ES0007", name)));
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
                		new StorageException(mcat.getMessage("ES0008", name)));
            }
        } catch (IOException e) {
            exceptionHandler.errorThrow(this.getClass().getName(), "lock", 
            		new StorageException(mcat.getMessage("ES0008", name), e));
        }
    }

    public final synchronized void unlock() {
        isValid();
        if (lock == null) {
            exceptionHandler.errorThrow(this.getClass().getName(), "lock", 
            		new StorageException(mcat.getMessage("ES0009", name)));
        }
        try {
            lock.release();
            lock = null;
        } catch (IOException e) {
            exceptionHandler.errorThrow(this.getClass().getName(), "lock", 
            		new StorageException(mcat.getMessage("ES0010", name), e));
        }
    }

    public final String getName() {
        return name;
    }

    public final StringBuilder appendTo(StringBuilder sb) {
        sb.append("FileStorageContainer(name=")
            .append(name)
            .append(", file=")
            .append(file)
            .append(")");
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
