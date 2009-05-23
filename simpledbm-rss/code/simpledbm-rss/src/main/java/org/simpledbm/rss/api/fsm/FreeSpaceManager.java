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
package org.simpledbm.rss.api.fsm;

import org.simpledbm.common.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.tx.Transaction;

/**
 * The FreeSpaceManager module interface supports creating new {@link StorageContainer} objects, and extending the size of 
 * existing containers.
 * <p>
 * StorageContainers as created by {@link StorageContainerFactory} interface do not have a defined
 * structure. The {@link PageManager} interface provides page structured view of a raw StorageContainer.
 * The FreeSpaceManager adds to this view the concept of free space map pages, and data pages. 
 */
public interface FreeSpaceManager {
	
	public final String LOGGER_NAME = "org.simpledbm.freespacemgr";

    /**
     * Creates a new Container with the specified name and registers it with the specified id.
     * The new container will have space map pages containing <code>spaceBits</code> bits
     * per page. The container space is managed in groups of pages called extents - an extent is the
     * minimum unit of space allocation. Note that a container can have only one type of data page.
     * <p>
     * Pre-condition 1: Caller must hold an exclusive lock on the container id.
     * <p>
     * Pre-condition 2: There must exist an open container with ID = 0. This container must contain at
     * least 1 page. 
     * <p>
     * 
     * @param trx The transaction that will manage this operation
     * @param containerName Name of the new container - must not conflict with any existing container
     * @param containerid The ID to be allocated to the new container - must not conflict with any existing container
     * @param spaceBits The number of bits to be used to represent free space data for an individual page; either 1 or 2.
     * @param extentSize Allocation unit - number of pages in each extent.
     * @param dataPageType The type of data pages this container will hold. The page type must have a registered
     *        {@link PageFactory} implementation.
     * @see PageFactory
     * @see PageManager
     * @see ObjectRegistry
     */
    public void createContainer(Transaction trx, String containerName,
            int containerid, int spaceBits, int extentSize, int dataPageType);

    /**
     * Adds a new extent to the container. New free space map pages will be added if necessary.
     * <p>
     * Precondition: caller must hold a shared lock on the container ID.
     *
     * @param trx Transaction that will manage this action.
     * @param containerId ID of the container that will be extended.
     */
    public void extendContainer(Transaction trx, int containerId);

    /**
     * Drop an existing container. To avoid having to log the entire contents of the
     * container in case the transaction is aborted, the actual
     * action of dropping the container is deferred until it is known that the transaction is
     * definitely committing.
     * <p>
     * Precondition: Caller must hold an exclusive lock on the container ID.
     *
     * @param trx Transaction that will manage this action.
     * @param containerId ID of the container that will be dropped.
     */
    public void dropContainer(Transaction trx, int containerId);

    /**
     * Get a cursor for traversing free space information within the container.
     */
    public FreeSpaceCursor getSpaceCursor(int containerId);

    /**
     * Get a cursor for traversing free space information within the container.
     * Implementation may return a pooled cursor.
     */
    public FreeSpaceCursor getPooledSpaceCursor(int containerId);
    
    /**
     * Returns a space cursor to the pool.
     */
    public void releaseSpaceCursor(FreeSpaceCursor fsc);
    
    /**
     * Opens a scan of all the pages within the container that are marked
     * non-empty. Scan will return pages in order from the beginning of the
     * container. Note that the scan may return non data pages; the caller must
     * check the page type before attempting to modify or access a page's 
     * content.
     */
    public FreeSpaceScan openScan(int containerId);
}
