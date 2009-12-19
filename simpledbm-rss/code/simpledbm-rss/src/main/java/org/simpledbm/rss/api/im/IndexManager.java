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
 *    Linking this library statically or dynamically with other modules 
 *    is making a combined work based on this library. Thus, the terms and
 *    conditions of the GNU General Public License cover the whole
 *    combination.
 *    
 *    As a special exception, the copyright holders of this library give 
 *    you permission to link this library with independent modules to 
 *    produce an executable, regardless of the license terms of these 
 *    independent modules, and to copy and distribute the resulting 
 *    executable under terms of your choice, provided that you also meet, 
 *    for each linked independent module, the terms and conditions of the 
 *    license of that module.  An independent module is a module which 
 *    is not derived from or based on this library.  If you modify this 
 *    library, you may extend this exception to your version of the 
 *    library, but you are not obligated to do so.  If you do not wish 
 *    to do so, delete this exception statement from your version.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.rss.api.im;

import org.simpledbm.common.api.locking.LockMode;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.api.tx.TransactionalModule;

/**
 * The Index Manager interface provides facilities for creating creating new Indexes 
 * and obtaining instances of existing Indexes.
 * 
 * @author Dibyendu Majumdar
 */
public interface IndexManager extends TransactionalModule {
	
	public final String LOGGER_NAME = "org.simpledbm.indexmgr";

    /**
     * Creates a new index with specified container name and ID. Prior to calling this
     * method, an exclusive lock should be obtained on the container ID to ensure that no other
     * transaction is simultaneously attempting to access the same container. If successful, by the
     * end of this call, the container should have been created and registered with the StorageManager,
     * and an empty instance of the index created within the container.
     * 
     * @param trx Transaction managing the creation of the index
     * @param name Name of the container
     * @param containerId ID of the new container, must be unused
     * @param extentSize Number of pages in each extent of the container
     * @param keyFactoryType Identifies the factory for creating IndexKey objects
     * @param locationFactoryType Identifies the factory for creating Location objects
     * @param unique If true, the new index will not allow duplicates keys
     */
    void createIndex(Transaction trx, String name, int containerId,
            int extentSize, int keyFactoryType, int locationFactoryType,
            boolean unique);

    /**
     * Obtains an existing index with specified container ID. A Shared lock is obtained on 
     * the container ID to ensure that no other transaction is simultaneously attempting to 
     * create/delete the same container. 
     * 
     * @param containerId ID of the container, must have been initialized as an Index prior to this call
     */
    IndexContainer getIndex(Transaction trx, int containerId);

	
	/**
	 * Locks an index container in specified mode for COMMIT duration.
	 * @param trx Transaction acquiring the lock
	 * @param containerId ID of the index container
	 * @param mode The Lock mode
	 */
    void lockIndexContainer(Transaction trx, int containerId, LockMode mode);    
    
}
