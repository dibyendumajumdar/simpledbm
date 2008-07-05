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
package org.simpledbm.typesystem.api;

import org.simpledbm.typesystem.impl.DefaultTypeFactory;
import org.simpledbm.typesystem.impl.GenericRowFactory;
import org.simpledbm.typesystem.impl.SimpleDictionaryCache;

/**
 * TypeSystemFactory is the entry point for external clients to obtain access
 * to the type system interfaces.
 * 
 * @author dibyendumajumdar
 */
public class TypeSystemFactory {

	public static TypeFactory getDefaultTypeFactory() {
		return new DefaultTypeFactory();
	}
	
	public static RowFactory getDefaultRowFactory(TypeFactory typeFactory) {
		return new GenericRowFactory(typeFactory, new SimpleDictionaryCache());
	}
	
	public static RowFactory getDefaultRowFactory(TypeFactory typeFactory, DictionaryCache dictionaryCache) {
		return new GenericRowFactory(typeFactory, dictionaryCache);
	}
}
