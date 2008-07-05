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

/**
 * DictionaryCache provides a facility to register type information for 
 * SimpleDBM tables and indexes. A table or index is identified by the
 * container Id.
 * 
 * @author dibyendu majumdar
 */
public interface DictionaryCache {

	/**
	 * Returns the TypeDescriptor[] associated with the container Id.
	 * Thread safe.
	 */
	TypeDescriptor[] getTypeDescriptor(int containerId);
	
	/**
	 * Registers the row definition for a particular container ID.
	 * Thread safe.
	 * @param containerId container ID for which the row type information is being registered
	 * @param rowTypeDesc An array of types that describe the fields in the row.
	 */
	void registerRowType(int containerId, TypeDescriptor[] rowTypeDesc);
}
