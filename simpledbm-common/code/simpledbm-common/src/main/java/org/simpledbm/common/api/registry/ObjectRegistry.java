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
 * The ObjectRegistry has a dual purpose; it provides a mechanism to cache
 * singletons, and secondly, it enables type codes to be assigned to persistable
 * classes, so that the type information can be used in serialization and
 * de-serialization of objects.
 * <p>
 * The ObjectRegistry provides methods for registering {@link ObjectFactory}
 * instances and singleton objects, associating each ObjectFactory/singleton
 * with a unique integer typecode. The typecode can be used subsequently to
 * re-construct instances of registered classes from byte streams, or to
 * retrieve the singletons.
 * <p>
 * The typecode is useful for recording type information when objects are
 * persisted, and allows objects to be re-created when data is read from
 * persistent store.
 * 
 * @author Dibyendu Majumdar
 * @since 07-Aug-2005
 */
public interface ObjectRegistry {

    public final String LOGGER_NAME = "org.simpledbm.registry";

    /**
     * Registers a type to the Object Registry. An ObjectFactory must be
     * provided for creating instances of the type.
     * 
     * @param typecode A unique type code for the type.
     * @param objectFactory The ObjectFactory implementation.
     */
    public void registerObjectFactory(int typecode, ObjectFactory objectFactory);

    /**
     * Registers a Singleton object to the registry.
     * 
     * @param typecode A unique type code for the type.
     * @param object The object to be registered.
     */
    public void registerSingleton(int typecode, Object object);

    /**
     * Gets the registered instance of the specified type.
     * 
     * @param typecode The code for the type
     * @return Object of the specified type.
     */
    Object getSingleton(int typecode);

    /**
     * Creates an instance of an object from the supplied ByteBuffer. The first
     * two bytes of the ByteBuffer must contain the the type code of the desired
     * object; the class in question must have a constructor that takes a
     * ByteBuffer as the only parameter.
     * 
     * <p>
     * It is an error to invoke this on a singleton.
     * 
     * <p>
     * Note that the supplied ByteBuffer will be passed on to the object in the
     * same state as it is passed to this method, ie, the object constructor
     * must expect the first two bytes to be the typecode.
     * 
     * @param buf The ByteBuffer to supply as argument to the object being
     *            created
     * @return Newly constructed object of the type
     */
    Object getInstance(ByteBuffer buf);
}
