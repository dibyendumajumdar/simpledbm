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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageException;

/**
 * Implements a File based StorageContainer.
 * 
 * @author Dibyendu Majumdar
 * @since 24-Jun-2005
 */
public final class FileStorageContainer implements StorageContainer {

    private static final String SIMPLEDBM_EIO006 = "SIMPLEDBM-EIO-006: Error occurred while closing StorageContainer: ";

    private static final String SIMPLEDBM_EIO005 = "SIMPLEDBM-EIO-005: Error occurred while flushing StorageContainer: ";

    private static final String SIMPLEDBM_EIO004 = "SIMPLEDBM-EIO-004: Error occurred while reading from StorageContainer: ";

    private static final String SIMPLEDBM_EIO003 = "SIMPLEDBM-EIO-003: Error occurred while writing to StorageContainer: ";

    private static final String SIMPLEDBM_EIO001 = "SIMPLEDBM-EIO-001: StorageContainer is not valid: ";

	/**
	 * The underlying file object.
	 */
    private final RandomAccessFile file;

    private final String name;

    private FileLock lock;
    
	/**
	 * Creates a new FileStorageContainer from an existing
	 * file object.
	 * @param file Existing file object.
	 */
	FileStorageContainer(String name, RandomAccessFile file) {
        this.name = name;
		this.file = file;
	}

	/**
	 * Checks if the file is available for reading and writing.
	 * @throws StorageException Thrown if the file has been closed.
	 */
	private void isValid() throws StorageException {
		if (file == null || !file.getChannel().isOpen()) {
			throw new StorageException(SIMPLEDBM_EIO001 + name);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.simpledbm.io.StorageContainer#write(long, byte[], int, int)
	 */
	public final synchronized void write(long position, byte[] data, int offset, int length) throws StorageException {
		isValid();
		try {
			file.seek(position);
			file.write(data, offset, length);
		} catch (IOException e) {
			throw new StorageException(SIMPLEDBM_EIO003 + name, e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.simpledbm.io.StorageContainer#read(long, byte[], int, int)
	 */
	public final synchronized int read(long position, byte[] data, int offset, int length) throws StorageException {
		isValid();
		int n = 0;
		try {
			file.seek(position);
			n = file.read(data, offset, length);
		} catch (IOException e) {
			throw new StorageException(SIMPLEDBM_EIO004 + name, e);
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
			file.getChannel().force(true);
		} catch (IOException e) {
			throw new StorageException(SIMPLEDBM_EIO005 + name, e);
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
			throw new StorageException(SIMPLEDBM_EIO006 + name, e);
		}
	}
    
	public final synchronized void lock() {
		isValid();
		if (lock != null) {
			throw new StorageException("Already locked");
		}
		try {
			FileChannel channel = file.getChannel();
			lock = channel.lock();
		}
		catch (IOException e) {
			throw new StorageException(e);
		}
	}

	public final synchronized void unlock() {
		isValid();
		if (lock == null) {
			throw new StorageException("Not locked");
		}
		try {
			lock.release();
			lock = null;
		}
		catch (IOException e) {
			throw new StorageException(e);
		}
	}
	
    public String getName() {
        return name;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    public String toString() {
    	return "FileStorageContainer(name=" + name + ", file=" + file + ")";
    }
}
