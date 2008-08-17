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

/**
 * 
 * <p>Defines the interface to low level IO sub-system. The objective is to
 * present a generic interface that can potentially be implemented in a
 * number of different ways. The default implementation is in {@link org.simpledbm.rss.api.st.impl}.</p>
 * <h2>Overview</h2>
 * <p>Database Managers typically use files to store various types of data,
 * such as, log files, data files, etc. However, from the perspective of a DBMS,
 * the concept of a file is a logical one; all the DBMS cares about is a named storage
 * container that supports random positioned IO. As long as this requirement is met, it is
 * not important whether a container maps to a file or to some other device.</p>
 * <p>The objective of this package is to provide a level of abstraction to the rest of the DBMS so that
 * the mapping of a container to a file becomes an implementation artifact. If desired,
 * containers may be mapped to raw devices, or to segments within a file.</p>
 * <p>Container names are usually not good identifiers for the rest of the system.
 * Integer identifiers are better, especially when other objects need to refer to
 * specific containers. Integers take less amount of storage, and also remove the
 * dependency between the container's name and the rest of the system. To support this
 * requirement, the {@link org.simpledbm.rss.api.st.StorageManager} interface is provided, which maintains a 
 * mapping of StorageContainers to integer identifiers. Note that the Storage
 * sub-system does not decide how to map the containers to ids; it enables the 
 * registration of these mappings and allows StorageContainer objects to be retrieved
 * using their numeric identifiers.
 */
package org.simpledbm.rss.api.st;

