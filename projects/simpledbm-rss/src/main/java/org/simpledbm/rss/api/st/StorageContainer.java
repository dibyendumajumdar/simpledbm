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
package org.simpledbm.rss.api.st;

/**
 * Specifies the interface to a container on secondary storage. The interface is
 * small and generic to allow many different implementations. An important
 * requirement of the interface is to allow reads and writes from specified
 * positions within the container. Conceptually, the container is treated as a
 * stream of bytes.
 * 
 * @author Dibyendu Majumdar
 * @since 18-Jun-05
 */
public interface StorageContainer {

    /**
     * Returns the name of the storage container.
     */
    String getName();

    /**
     * Writes length number of bytes from byte array, beginning at offset, to
     * the specified position within the container. Thread safe.
     * 
     * @param position The position where the write must begin, &gt;= 0.
     * @param buffer Data to be written out.
     * @param bufferOffset The offset with data.
     * @param length The number of bytes that need to be written.
     * @throws StorageException Thrown if there was an error when writing the
     *             data.
     */
    void write(long position, byte[] buffer, int bufferOffset, int length);

    /**
     * Reads upto length bytes from the container into the byte array beginning
     * at offset, from the specified position within the container. Thread safe.
     * 
     * @param position The position where the read must begin, &gt;= 0.
     * @param buffer Data will be read into this array.
     * @param bufferOffset The offset within the array where the read data will
     *            be placed.
     * @param length The number of bytes that is to be read.
     * @throws StorageException Thrown if there was an error when reading the
     *             data.
     * @return Number of bytes read, or &lt;=0 if no more data available.
     */
    int read(long position, byte[] buffer, int bufferOffset, int length);

    /**
     * Ensures that all data written to the container is flushed to secondary
     * storage.
     * 
     * @throws StorageException Thrown if there is an error while flushing the
     *             data.
     */
    void flush();

    /**
     * Locks the container exclusively. Must not wait if lock is unavailable -
     * instead should throw a StorageException.
     * 
     * @throws StorageException Thrown if lock could not be obtained.
     */
    void lock();

    /**
     * Releases lock on the container.
     * 
     * @throws StorageException Thrown if there is an error releasing the lock.
     */
    void unlock();

    /**
     * Closes the container. A container cannot be used after it has been
     * closed.
     * 
     * @throws StorageException Thrown if there is an error while closing the
     *             container.
     */
    void close();

}
