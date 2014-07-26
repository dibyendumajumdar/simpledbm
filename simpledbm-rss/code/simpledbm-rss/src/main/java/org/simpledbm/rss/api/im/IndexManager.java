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
package org.simpledbm.rss.api.im;

import org.simpledbm.common.api.locking.LockMode;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.api.tx.TransactionalModule;

/**
 * The Index Manager interface provides facilities for creating creating new
 * Indexes and obtaining instances of existing Indexes.
 * 
 * @author Dibyendu Majumdar
 */
public interface IndexManager extends TransactionalModule {

    public final String LOGGER_NAME = "org.simpledbm.indexmgr";

    /**
     * Creates a new index with specified container name and ID. Prior to
     * calling this method, an exclusive lock should be obtained on the
     * container ID to ensure that no other transaction is simultaneously
     * attempting to access the same container. If successful, by the end of
     * this call, the container should have been created and registered with the
     * StorageManager, and an empty instance of the index created within the
     * container.
     * 
     * @param trx Transaction managing the creation of the index
     * @param name Name of the container
     * @param containerId ID of the new container, must be unused
     * @param extentSize Number of pages in each extent of the container
     * @param keyFactoryType Identifies the factory for creating IndexKey
     *            objects
     * @param locationFactoryType Identifies the factory for creating Location
     *            objects
     * @param unique If true, the new index will not allow duplicates keys
     */
    void createIndex(Transaction trx, String name, int containerId,
            int extentSize, int keyFactoryType, int locationFactoryType,
            boolean unique);

    /**
     * Obtains an existing index with specified container ID. A Shared lock is
     * obtained on the container ID to ensure that no other transaction is
     * simultaneously attempting to create/delete the same container.
     * 
     * @param containerId ID of the container, must have been initialized as an
     *            Index prior to this call
     */
    IndexContainer getIndex(Transaction trx, int containerId);

    /**
     * Locks an index container in specified mode for COMMIT duration.
     * 
     * @param trx Transaction acquiring the lock
     * @param containerId ID of the index container
     * @param mode The Lock mode
     */
    void lockIndexContainer(Transaction trx, int containerId, LockMode mode);

}
