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
package org.simpledbm.rss.api.bm;

import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.tx.TransactionManager;
import org.simpledbm.rss.api.wal.Lsn;

/**
 * The Buffer Manager is responsible for maintaining a Buffer Pool in memory
 * where disk pages are temporarily stored (cached) while they are being
 * accessed. The primary reason for this is to improve performance, as memory
 * access is several orders of magnitude faster than disk access. The Buffer
 * Manager also coordinates the flushing of the Write Ahead Log to ensure that
 * any changes to pages are logged before the pages are written to disk. This is
 * the basis for recovery.
 * 
 * @author Dibyendu Majumdar
 * @since 18-Aug-2005
 */
public interface BufferManager {

    public final static String LOGGER_NAME = "org.simpledbm.bufmgr";

    /**
     * Starts the Buffer Manager instance. This may cause background threads to
     * be started.
     */
    public void start();

    /**
     * Shuts down the Buffer Manager instance. Any background threads will be
     * stopped. It is recommended that the Buffer Manager writes all dirty pages
     * to disk before shutting down.
     */
    public void shutdown();

    /**
     * Fixes a page in memory, reading it from disk if necessary, and latches it
     * in shared mode. It is an error if the page does not already exist in
     * persistent storage. Note that while the page is fixed in memory, the
     * Buffer Manager cannot swap it to disk to make room for other pages. Hence
     * it is advisable to fix pages for a short while only.
     * 
     * @param pageid The identity of the page that should be fixed.
     * @param hint A hint to indicate which end of the LRU chain the page should
     *            be inserted to.
     * @return A {@link BufferAccessBlock} containing a reference to the desired
     *         page.
     * @see BufferAccessBlock#unfix()
     */
    public BufferAccessBlock fixShared(PageId pageid, int hint);

    /**
     * Fixes a page in memory, reading it from disk if necessary, and latches it
     * in exclusive mode.
     * <p>
     * An exclusive latch on the page may be downgraded to an update latch by
     * calling {@link BufferAccessBlock#downgradeExclusiveLatch()}.
     * <p>
     * Unless the request is being made to fix a new page, the page must already
     * exist in persistent storage. Caller must ensure that request for a new
     * page is not made for a page already in the buffer pool.
     * 
     * @param pageid The identity of the page that should be fixed.
     * @param isNew If this is set to true, the page will not be read from disk.
     *            It is assumed that the requested page is new and has not been
     *            previously saved to disk.
     * @param pagetype Specifies the type of page to create; only used when
     *            isNew is set. The page type must be associated with a subclass
     *            of {@link Page} and must have a {@link PageFactory} registered
     *            with the {@link ObjectRegistry}.
     * @param hint A hint to indicate which end of the LRU chain the page should
     *            be inserted to. The meaning of the hint is implementation
     *            defined.
     * @return A {@link BufferAccessBlock} containing a reference to the desired
     *         page.
     * @see BufferAccessBlock#unfix()
     * @see BufferAccessBlock#downgradeExclusiveLatch()
     * @see BufferAccessBlock#setDirty(Lsn)
     */
    public BufferAccessBlock fixExclusive(PageId pageid, boolean isNew,
            int pagetype, int hint);

    /**
     * Fixes a page in memory, reading it from disk if necessary, and latches
     * the page in update mode. A page that is latched in update mode can be
     * upgraded to exclusive mode by calling
     * {@link BufferAccessBlock#upgradeUpdateLatch()}. It is an error if the
     * page does not already exist in persistent storage.
     * 
     * @param pageid The identity of the page that should be fixed.
     * @param hint A hint to indicate which end of the LRU chain the page should
     *            be inserted to.
     * @return A {@link BufferAccessBlock} containing a reference to the desired
     *         page.
     * @see BufferAccessBlock#unfix()
     * @see BufferAccessBlock#upgradeUpdateLatch()
     */
    public BufferAccessBlock fixForUpdate(PageId pageid, int hint);

    /**
     * Returns information about dirty pages in the Buffer Pool. This method is
     * called by the Transaction Manager during checkpoints.
     * <p>
     * For each dirty page, the Buffer Manager must return the
     * {@link DirtyPageInfo#getPageId() pageId} and
     * {@link DirtyPageInfo#getRecoveryLsn() recoveryLsn}. The recoveryLsn is
     * the LSN of the oldest log record that may have modified the page since it
     * was last written to disk.
     * 
     * @return An array of {@link DirtyPageInfo} objects containing information
     *         about dirty pages.
     */
    public DirtyPageInfo[] getDirtyPages();

    /**
     * Synchronizes recoveryLsns of pages in the buffer pool with the
     * {@link TransactionManager}. Typically this is called at system restart
     * after recovery has been completed.
     * <p>
     * The Buffer Manager must maintain for each page in the Buffer Pool a
     * recoveryLsn, which should point to the earliest log record that made
     * changes to the page since it was last written to disk.
     * <p>
     * This method is intended for use by the Transaction Manager. Since this
     * method is called during restart recovery only, there is no need to make
     * it thread safe.
     * 
     * @param dirty_pages List of dirty pages as determined by Transaction
     *            Manager
     */
    public void updateRecoveryLsns(DirtyPageInfo[] dirty_pages);

    /**
     * Marks all pages of specified container as invalid. Usually this means
     * that the container has been dropped.
     * 
     * @param containerId ID of the container that is to be invalidated
     */
    void invalidateContainer(int containerId);

    /**
     * Requests that buffers be flushed to disk. Note that this is a hint only;
     * the buffer manager is not required to honor this request.
     */
    void writeBuffers();
}
