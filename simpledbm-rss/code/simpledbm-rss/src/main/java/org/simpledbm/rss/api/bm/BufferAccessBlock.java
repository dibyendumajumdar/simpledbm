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
package org.simpledbm.rss.api.bm;

import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.wal.Lsn;

/**
 * A BufferAccessBlock encapsulates access to a page that has been fixed in
 * memory. While the page is fixed, it cannot be swapped out to disk by the
 * Buffer Manager.
 * <p>
 * The page associated with the BufferAccessBlock can be obtained by calling
 * {@link #getPage()}. Note that the page should be cast to the appropriate page
 * type by the caller. If the page has been latched exclusively, it is legal to
 * modify it. Once you are done with the page, you should call
 * {@link #setDirty(Lsn) setDirty()} if you have modified the page, and then
 * {@link #unfix()} to inform the Buffer Manager that the page can be swapped
 * out if necessary.
 * <p>
 * Note that you must follow the Write Ahead Log protocol when making changes to
 * pages. This means that before you make a change, you should generate a log
 * record that represents the changes, and supply the
 * {@link org.simpledbm.rss.api.wal.Lsn Lsn} of the log record to
 * {@link #setDirty(Lsn) setDirty()}. This will ensure that the log will be
 * flushed up to the Lsn before the page is swapped out.
 * <p>
 * A BufferAccessBlock must only be used by one thread at any point in time.
 * 
 * @author Dibyendu Majumdar
 * @since 18-Aug-2005
 */
public interface BufferAccessBlock {

    /**
     * Marks the page as dirty. Only applicable if the page was obtained for
     * exclusive access.
     * 
     * @param lsn Lsn of the log record that represents the modifications made
     *            to the page.
     * @throws IllegalStateException If page is not held in exclusive latch mode
     */
    public void setDirty(Lsn lsn);

    /**
     * Gets the page being accessed. It is assumed that the caller will cast the
     * page to appropriate type.
     * 
     * @return Page object which must be cast to the required type by the
     *         caller.
     */
    public Page getPage();

    /**
     * Upgrades page latch from UPDATE to EXCLUSIVE. Latch must be held in
     * UPDATE mode, else an Exception will be thrown.
     * 
     * @throws IllegalStateException If latch is not held in update mode
     */
    public void upgradeUpdateLatch();

    /**
     * Downgrades page latch from EXCLUSIVE to UPDATE. Latch must be held in
     * EXCLUSIVE mode, else an Exception will be thrown.
     * 
     * @throws IllegalStateException If latch is not held exclusively
     */
    public void downgradeExclusiveLatch();

    /**
     * Unfixes the page and releases latch on the page. It is important that
     * this is called for every fix. Failure to do so will cause pages to get
     * stuck in the buffer pool, eventually causing the system to run of slots
     * in the buffer pool.
     */
    public void unfix();

    /**
     * Tests whether the page is currently latched in EXCLUSIVE mode.
     */
    public boolean isLatchedExclusively();

    /**
     * Tests whether the page is currently latched in UPDATE mode.
     */
    public boolean isLatchedForUpdate();

    /**
     * Tests whether the page is currently latched in SHARED mode.
     */
    public boolean isLatchedShared();

}
