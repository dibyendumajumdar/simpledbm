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
package org.simpledbm.rss.api.im;

/**
 * An IndexKeyFactory is resposible for generating keys. This interface
 * is typically implemented by the clients of the IndexManager module.
 *
 * @author Dibyendu Majumdar
 * @since Oct-2005
 */
public interface IndexKeyFactory {

	/**
	 * Generates a new (empty) key for the specified
	 * Container. The Container ID is meant to be used as key
	 * for locating information specific to a container; for instance,
	 * the attributes of an Index.
	 * 
	 * @param containerId ID of the container for which a key is required
	 */
	IndexKey newIndexKey(int containerId);

	/**
	 * Generates a key that represents Infinity - it must be greater than
	 * all possible keys in the domain for the key.

	 * @param containerId ID of the container for which a key is required
	 */
	IndexKey maxIndexKey(int containerId);
}
