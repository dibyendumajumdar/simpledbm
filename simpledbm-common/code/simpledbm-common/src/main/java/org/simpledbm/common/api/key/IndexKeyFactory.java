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
package org.simpledbm.common.api.key;

import java.nio.ByteBuffer;

/**
 * An IndexKeyFactory is responsible for generating keys. This interface
 * is typically implemented by the clients of the IndexManager module.
 *
 * @author Dibyendu Majumdar
 * @since Oct-2005
 */
public interface IndexKeyFactory {

    /**
     * Generates a new (empty) key for the specified
     * Container. The Container ID can be used to lookup
     * the index key type for a container.
     * 
     * @param containerId ID of the container for which a key is required
     */
    IndexKey newIndexKey(int containerId);

    /**
     * Generates a key that represents Infinity - it must be greater than
     * all possible keys in the domain for the key. The Container ID can be used to lookup
     * the index key type for a container.
     * 
     * @param containerId ID of the container for which a key is required
     */
    IndexKey maxIndexKey(int containerId);

    /**
     * Generates a key that represents negative Infinity - it must be smaller than
     * all possible keys in the domain for the key. The Container ID can be used to lookup
     * the index key type for a container.
     * <p>
     * The key returned by this method can be used as an argument to index scans.
     * The result will be a scan of the index starting from the first key in 
     * the index.
     * 
     * @param containerId ID of the container for which a key is required
     */
    IndexKey minIndexKey(int containerId);
    
    /**
     * Reconstructs an index key from the byte stream represented by the ByteBuffer.
     * The Container ID can be used to lookup the index key type for a container.
     * <p>
     * The IndexKey implementation must provide a constructor that takes a single
     * {@link ByteBuffer} parameter.
     * 
     * @param containerId The ID of the container for which the key is being read
     * @param bb ByteBuffer representing the byte stream that contains the key to be read
     * @return A newly instantiated key.
     */
    IndexKey newIndexKey(int containerId, ByteBuffer bb);
 
    /**
     * Parse the supplied string and construct a key. Not guaranteed to work,
     * but useful for creating test cases.
     */
    IndexKey parseIndexKey(int containerId, String s);
}
