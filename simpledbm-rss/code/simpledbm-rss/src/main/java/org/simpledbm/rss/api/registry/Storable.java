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
package org.simpledbm.rss.api.registry;

import java.nio.ByteBuffer;

/**
 * A Storable object can be written to (stored into) or read from (retrieved from) 
 * a ByteBuffer. The object must be able to predict its length in bytes; this 
 * not only allows clients to allocate ByteBuffer objects of suitable size, it is also 
 * be used by a StorageContainer to ensure that objects can be restored from
 * secondary storage.
 * <p>Storable objects must provide constructors that accept ByteBuffer as the sole
 * argument. In order to create such objects, implementations of {@link org.simpledbm.rss.api.registry.ObjectFactory ObjectFactory} must be
 * registered with the {@link org.simpledbm.rss.api.registry.ObjectRegistry ObjectRegistry}.
 * <p>The asymmetry between the way objects are serialized through calling {@link #store(ByteBuffer) store()} and
 * de-serialized using the ObjectFactory is to allow constructor based initialization of
 * objects during de-serialization. This allows objects to be defined as immutable without
 * introducing a back door facility for reading/writing final fields.
 * @author dibyendu
 * @since 10-June-2005
 */
public interface Storable {

    /**
     * Store this object into the supplied ByteBuffer in a format that can 
     * be subsequently used to reconstruct the object. ByteBuffer is assumed
     * to be setup correctly for writing.
     * @param bb ByteBuffer that will a stored representation of the object.
     */
    void store(ByteBuffer bb);

    /**
     * Predict the length of this object in bytes when it will be stored
     * in a ByteBuffer.
     * @return The length of this object when stored in a ByteBuffer.
     */
    int getStoredLength();

}
