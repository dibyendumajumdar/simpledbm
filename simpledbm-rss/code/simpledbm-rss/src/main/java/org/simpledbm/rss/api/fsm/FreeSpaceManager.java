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
package org.simpledbm.rss.api.fsm;

import org.simpledbm.common.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.tx.Transaction;

/**
 * The FreeSpaceManager module interface supports creating new
 * {@link StorageContainer} objects, and extending the size of existing
 * containers.
 * <p>
 * StorageContainers as created by {@link StorageContainerFactory} interface do
 * not have a defined structure. The {@link PageManager} interface provides page
 * structured view of a raw StorageContainer. The FreeSpaceManager adds to this
 * view the concept of free space map pages, and data pages.
 */
public interface FreeSpaceManager {

    public final String LOGGER_NAME = "org.simpledbm.freespacemgr";

    /**
     * Creates a new Container with the specified name and registers it with the
     * specified id. The new container will have space map pages containing
     * <code>spaceBits</code> bits per page. The container space is managed in
     * groups of pages called extents - an extent is the minimum unit of space
     * allocation. Note that a container can have only one type of data page.
     * <p>
     * Pre-condition 1: Caller must hold an exclusive lock on the container id.
     * <p>
     * Pre-condition 2: There must exist an open container with ID = 0. This
     * container must contain at least 1 page.
     * <p>
     * 
     * @param trx The transaction that will manage this operation
     * @param containerName Name of the new container - must not conflict with
     *            any existing container
     * @param containerid The ID to be allocated to the new container - must not
     *            conflict with any existing container
     * @param spaceBits The number of bits to be used to represent free space
     *            data for an individual page; either 1 or 2.
     * @param extentSize Allocation unit - number of pages in each extent.
     * @param dataPageType The type of data pages this container will hold. The
     *            page type must have a registered {@link PageFactory}
     *            implementation.
     * @see PageFactory
     * @see PageManager
     * @see ObjectRegistry
     */
    public void createContainer(Transaction trx, String containerName,
            int containerid, int spaceBits, int extentSize, int dataPageType);

    /**
     * Adds a new extent to the container. New free space map pages will be
     * added if necessary.
     * <p>
     * Precondition: caller must hold a shared lock on the container ID.
     * 
     * @param trx Transaction that will manage this action.
     * @param containerId ID of the container that will be extended.
     */
    public void extendContainer(Transaction trx, int containerId);

    /**
     * Drop an existing container. To avoid having to log the entire contents of
     * the container in case the transaction is aborted, the actual action of
     * dropping the container is deferred until it is known that the transaction
     * is definitely committing.
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
