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
 * The ObjectFactory provides a facility for registering Classes and 
 * Singleton objects, associating each Class/Singleton with a unique
 * integer typecode. The typecode can be used subsequently to
 * retrieve intances of registered classes, or the Singletons.
 * The typecode is useful for recording type information when 
 * objects are persisted, and allows objects to be re-created when
 * data is read from persistent store.
 * 
 * @author Dibyendu Majumdar
 * @since 07-Aug-2005
 */
public interface ObjectRegistry {

    /**
     * Registers a class to the Object Registry. 
     *  
     * @param typecode A unique type code for the type.
     * @param classname The class name.
     */
    public void registerType(int typecode, ObjectFactory objectFactory);

//    /**
//     * Registers a class to the Object Registry. 
//     * The class must implement a no-arg constructor.
//     * The class may optionally implement a constructor that takes a single ByteBuffer parameter.
//     * The class may optionally implement {@link ObjectRegistryAware}
//     * interface.
//     *  
//     * @param typecode A unique type code for the type.
//     * @param classname The class name.
//     */
//    public void registerType(int typecode, Class<?> klass);
    
    /**
     * Registers a Singleton object to the registry.
     * 
     * @param typecode A unique type code for the type.
     * @param object The object to be registered.
     */
    public void registerSingleton(int typecode, Object object);

    /**
     * Creates an instance of the specified type. If the
     * type refers to a class, then a new Object instance 
     * will be created. If the type refers to a Singleton, 
     * the Singleton will be returned.
     * 
     * @param typecode The code for the type
     * @return Object of the specified type.
     */
    Object getInstance(int typecode);
    
    /**
     * Creates an instance of the specified type, and initializes 
     * it using the supplied ByteBuffer; the class in question must have
     * a constructor that takes a ByteBuffer as the only parameter.
     * 
     * <p>It is an error to invoke this on a singleton.
     * 
     * @param typecode The code for the type
     * @param buf The ByteBuffer to supply as argument to the object being created
     * @return Newly constructed object of the type
     */
    Object getInstance(int typecode, ByteBuffer buf);
    
    /**
     * Creates an instance of an object from the supplied ByteBuffer.
     * The first two bytes of the ByteBuffer must contain the 
     * the type code of the desired object; the class in question must have
     * a constructor that takes a ByteBuffer as the only parameter.
     * 
     * <p>It is an error to invoke this on a singleton.
     * 
     * <p>Note that the supplied ByteBuffer will be passed on to the
     * object in the same state as it is passed to this method, ie,
     * the object constructor must expect the first two bytes to be 
     * the typecode.
     * 
     * @param buf The ByteBuffer to supply as argument to the object being created
     * @return Newly constructed object of the type
     */
    Object getInstance(ByteBuffer buf);
}
