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
package org.simpledbm.rss.api.fsm;

import org.simpledbm.rss.api.tx.Transaction;

/**
 * Space Manager module interface. Supports creating new containers, and extending the size of 
 * existing containers.
 */
public interface FreeSpaceManager {

    /**
     * Creates a new Container with the specified name and registers it with the specified id.
     * The new container will have space map pages containing <code>spaceBits</code> bits
     * per page. The container space is managed in groups of pages called extents - an extent is the
     * minimum unit of space allocation. Note that a container can have only one type of data page.
     * <p>
     * Pre-condition 1: caller must have acquired exclusive lock on the container id.
     * <p>
     * Pre-condition 2: There must exist an open container with ID = 0. This container must contain at
     * least 1 page. 
     * 
     * @param trx The transaction that will manage this operation
     * @param containerName Name of the new container - mst be unused
     * @param containerid The ID to be allocated to the new container - must not be in use.
     * @param spaceBits The number of bits to be used to represent free space data for an individual page.
     * @param extentSize Allocation unit - number of pages in the extent.
     * @param dataPageType The type of data pages this container will hold.
     */
    public void createContainer(Transaction trx, String containerName,
            int containerid, int spaceBits, int extentSize, int dataPageType);

    /**
     * Adds a new extent to the container. New free space map pages will be added if necessary.
     * <p>
     * Precondition: caller must have a shared lock on the container.
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
