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
package org.simpledbm.common.api.registry;

import java.nio.ByteBuffer;

/**
 * A Storable object can be written to (stored into) or read from (retrieved
 * from) a ByteBuffer. The object must be able to predict its length in bytes;
 * this not only allows clients to allocate ByteBuffer objects of suitable size,
 * it is also be used by a StorageContainer to ensure that objects can be
 * restored from secondary storage.
 * <p>
 * Storable objects must provide constructors that accept ByteBuffer as the sole
 * argument. In order to create such objects, implementations of
 * {@link org.simpledbm.common.api.registry.ObjectFactory ObjectFactory} must be
 * registered with the {@link org.simpledbm.common.api.registry.ObjectRegistry
 * ObjectRegistry}.
 * <p>
 * The asymmetry between the way objects are serialized through calling
 * {@link #store(ByteBuffer) store()} and de-serialized using the ObjectFactory
 * is to allow constructor based initialization of objects during
 * de-serialization. This allows objects to be defined as immutable without
 * introducing a back door facility for reading/writing final fields.
 * 
 * @author dibyendu
 * @since 10-June-2005
 */
public interface Storable {

    /**
     * Store this object into the supplied ByteBuffer in a format that can be
     * subsequently used to reconstruct the object. ByteBuffer is assumed to be
     * setup correctly for writing.
     * 
     * @param bb ByteBuffer that will a stored representation of the object.
     */
    void store(ByteBuffer bb);

    /**
     * Predict the length of this object in bytes when it will be stored in a
     * ByteBuffer.
     * 
     * @return The length of this object when stored in a ByteBuffer.
     */
    int getStoredLength();

}
