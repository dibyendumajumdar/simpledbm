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
package org.simpledbm.rss.api.st;

/**
 * Factory interface for creating new instances of StorageContainer objects.
 * 
 * @author Dibyendu Majumdar
 * @since 18-Jun-2005
 */
public interface StorageContainerFactory {

    public final String LOGGER_NAME = "org.simpledbm.storagemgr";

    /**
     * Initialize the storage hierarchy.
     */
    void init();

    /**
     * Creates a new StorageContainer of the specified name. Note that the
     * behaviour of create is not specified here. It is expected that an
     * implementation will use some form of configuration data to define the
     * behaviour.
     * 
     * @param name Name of the storage container.
     * @return Newly created StorageContainer object.
     * @throws StorageException Thrown if the StorageContainer cannot be
     *             created.
     */
    StorageContainer create(String name);

    /**
     * Creates a new StorageContainer of the specified name only if there isn't
     * an existing StorageContainer of the same name.
     */
    StorageContainer createIfNotExisting(String name);

    /**
     * Opens an existing StorageContainer. Note that the behaviour of open is
     * not specified here. It is expected that an implementation will use some
     * form of configuration data to define the behaviour.
     * 
     * @param name Name of the storage container.
     * @return Instance of StorageContainer object
     * @throws StorageException Thrown if the StorageContainer does not exist or
     *             cannot be opened.
     */
    StorageContainer open(String name);

    /**
     * Checks whether a container of specified name exists.
     * 
     * @param name Name of the storage container
     * @return true if the container exists
     */
    boolean exists(String name);

    /**
     * Removes a container physically.
     */
    void delete(String name);

    /**
     * Remove an entire storage hierarchy.
     */
    void drop();
}
