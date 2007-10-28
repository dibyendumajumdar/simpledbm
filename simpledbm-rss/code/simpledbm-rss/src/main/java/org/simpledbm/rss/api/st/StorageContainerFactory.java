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
package org.simpledbm.rss.api.st;

/**
 * Factory interface for creating new instances of StorageContainer objects.
 *
 * @author Dibyendu Majumdar
 * @since 18-Jun-2005
 */
public interface StorageContainerFactory {

    /**
     * Creates a new StorageContainer of the specified name. Note that the
     * behaviour of create is not specified here. It is expected that
     * an implementation will use some form of configuration data to
     * define the behaviour.
     *
     * @param name Name of the storage container.
     * @return Newly created StorageContainer object.
     * @throws StorageException Thrown if the StorageContainer cannot be created.
     */
    StorageContainer create(String name);

    /**
     * Creates a new StorageContainer of the specified name only if
     * there isn't an existing StorageContainer of the same name.
     */
    StorageContainer createIfNotExisting(String name);
    
    /**
     * Opens an existing StorageContainer. Note that the
     * behaviour of open is not specified here. It is expected that
     * an implementation will use some form of configuration data to
     * define the behaviour.
     *
     * @param name Name of the storage container.
     * @return Instance of StorageContainer object
     * @throws StorageException Thrown if the StorageContainer does not exist or cannot be opened.
     */
    StorageContainer open(String name);

    /**
     * Removes a container physically.
     * @throws StorageException 
     */
    void delete(String name);
}
