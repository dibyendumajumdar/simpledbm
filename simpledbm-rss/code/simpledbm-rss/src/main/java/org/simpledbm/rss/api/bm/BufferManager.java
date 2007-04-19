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
package org.simpledbm.rss.api.bm;

import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.wal.Lsn;

/**
 * The Buffer Manager is responsible for maintaining a Buffer Pool in memory
 * where disk pages are temporarily stored (cached) while they are being accessed. The
 * primary reason for this is to improve performance, as memory access is
 * several orders of magnitude faster than disk access. The Buffer Manager also
 * coordinates the flushing of the Write Ahead Log to ensure that any
 * changes to pages are logged before the pages are written to disk. This is the
 * basis for recovery.
 * 
 * @author Dibyendu Majumdar
 * @since 18-Aug-2005
 */
public interface BufferManager {

	/**
	 * Starts the Buffer Manager instance. This may cause background threads to
	 * be started.
	 */
	public void start();
	
    /**
	 * Shuts down the Buffer Manager instance. Any background threads will be
	 * stopped. It is recommended that the Buffer Manager writes all buffered
	 * pages to disk before shutting down.
	 */
    public void shutdown();

    /**
	 * Fixes a page in memory, reading it from disk if necessary, and latches it
	 * in shared mode. It is an error if the page does not already exist in
	 * persistent storage. Note that while the page is fixed in memory, the
	 * Buffer Manager cannot swap it to disk to make room for other pages. Hence
	 * it is advisable to fix pages for a short while only.
	 * 
	 * @param pageid
	 *            The identity of the page that should be fixed.
	 * @param hint
	 *            A hint to indicate which end of the LRU chain the page should
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
	 * A exclusive latch on the page may be downgraded to an update latch by
	 * calling {@link BufferAccessBlock#downgradeExclusiveLatch()}.
	 * <p>
	 * Unless the request is being made to fix a new page, the page must already
	 * exist in persistent storage. Caller must ensure that request for a new
	 * page is not made for a page already in the buffer pool.
	 * 
	 * @param pageid
	 *            The identity of the page that should be fixed.
	 * @param isNew
	 *            If this is set to true, the page will not be read from disk.
	 *            It is assumed that the requested page is new and has not been
	 *            previously saved to disk.
	 * @param pagetype
	 *            Specifies the type of page to create; only used when isNew is
	 *            set. The pagetype must be associated with a subclass of
	 *            {@link org.simpledbm.rss.api.pm.Page Page} and must have been
	 *            registered with the
	 *            {@link org.simpledbm.rss.api.registry.ObjectRegistry ObjectRegistry}.
	 * @param hint
	 *            A hint to indicate which end of the LRU chain the page should
	 *            be inserted to. The meaning of the hint is implementation
	 *            defined.
	 * @return A {@link BufferAccessBlock} containing a reference to the desired
	 *         page.
	 * @see BufferAccessBlock#unfix()
	 * @see BufferAccessBlock#downgradeExclusiveLatch()
	 * @see BufferAccessBlock#setDirty(Lsn)
	 */
    public BufferAccessBlock fixExclusive(PageId pageid, boolean isNew, int pagetype,
            int hint);

    /**
	 * Fixes a page in memory, reading it from disk if necessary, and latches
	 * the page in Update mode. A page that is latched in update mode can be
	 * upgraded to exclusive mode by calling
	 * {@link BufferAccessBlock#upgradeUpdateLatch()}. It is an error if the
	 * page does not already exist in persistent storage.
	 * 
	 * @param pageid
	 *            The identity of the page that should be fixed.
	 * @param hint
	 *            A hint to indicate which end of the LRU chain the page should
	 *            be inserted to.
	 * @return A {@link BufferAccessBlock} containing a reference to the desired page.
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
	 * {@link org.simpledbm.rss.api.tx.TransactionManager TransactionManager}.
	 * Typically this is called at system restart after recovery has been
	 * completed.
	 * <p>
	 * The Buffer Manager must maintain for each page in the Buffer Pool a
	 * recoveryLsn, which should point to the earliest log record that made
	 * changes to the page since it was last written to disk.
	 * <p>
	 * This method is intended for use by the Transaction Manager. Since this
	 * method is called during restart recovery only, there is no need to make
	 * it thread safe.
	 * 
	 * @param dirty_pages
	 *            List of dirty pages as determined by Transaction Manager
	 */
	public void updateRecoveryLsns(DirtyPageInfo[] dirty_pages);

    /**
	 * Marks all pages of specified container as invalid. Usually this means
	 * that the container has been dropped.
	 * 
	 * @param containerId
	 *            ID of the container that is to be invalidated
	 */
    void invalidateContainer(int containerId);    
}
