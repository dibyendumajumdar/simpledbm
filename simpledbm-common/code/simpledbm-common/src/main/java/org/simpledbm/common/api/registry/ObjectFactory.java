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
package org.simpledbm.common.api.registry;

import java.nio.ByteBuffer;

/**
 * ObjectFactory interface provides methods for creating instances of objects from
 * a byte stream wrapped by a ByteBuffer. It is the primary mechanism for restoring
 * persisted objects.
 * <p>
 * Non-singleton object types that want to register with the ObjectRegistry must
 * provide a ObjectFactory implementation for constructing objects.
 * 
 * @author dibyendu majumdar
 * @since 24 Aug 08
 */
public interface ObjectFactory {

	/**
	 * Construct an object from the supplied ByteBuffer.
	 * The first two bytes should be interpreted as a type code. 
	 */
	Object newInstance(ByteBuffer buf);
	
	/**
	 * Gets the class of the objects managed by this factory.
	 */
	Class<?> getType();
	
}
