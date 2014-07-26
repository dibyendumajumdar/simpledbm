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
package org.simpledbm.rss.api.pm;

import java.nio.ByteBuffer;

import org.simpledbm.common.api.registry.ObjectRegistry;
import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.Dumpable;
import org.simpledbm.common.util.TypeSize;
import org.simpledbm.rss.api.latch.Latch;
import org.simpledbm.rss.api.wal.Lsn;

/**
 * The base class for all Page implementations. The basic most common page
 * attributes are implemented. Page Size is determined by the
 * {@link PageManager} and cannot be changed by sub-classes and cannot be
 * changed by implementations.
 * 
 * @author Dibyendu Majumdar
 * @since 14-Aug-2005
 */
public abstract class Page implements Storable, Dumpable {

    public static final int SIZE = TypeSize.SHORT + Lsn.SIZE;

    /**
     * The type code for the page. Used to re-create correct type of page when
     * reading from disk.
     */
    private final short type;

    /**
     * The identity of the page. Transient.
     */
    private final PageId pageId;

    /**
     * Page LSN is the LSN of the last log record that made changes to the page.
     */
    private volatile Lsn pageLsn = new Lsn();

    /**
     * PageFactory that created this page. Note that the page size is fixed by
     * the factory. Transient.
     */
    private final PageManager pageManager;

    /**
     * A read/write latch to protect access to the page. Transient.
     */
    protected final Latch lock;

    /**
     * Protected constructor for sub-classes to use.
     * 
     * @param pageManager PageManager responsible for managing pages of this
     *            type
     * @param type The 2-byte type code used to identify pages in persistent
     *            store
     * @param pageId The pageId for which the page is being constructed
     */
    protected Page(PageManager pageManager, int type, PageId pageId) {
        this.pageManager = pageManager;
        this.type = (short) type;
        this.pageId = pageId;
        this.lock = pageManager.getLatchFactory().newReadWriteUpdateLatch();
    }

    /**
     * Protected constructor for sub-classes to use when reading from a byte
     * stream wrapped by a ByteBuffer.
     * 
     * @param pageManager The PageManager responsible for managing pages of this
     *            type
     * @param pageId The page Id of the page being read
     * @param bb The ByteBuffer that provides the input data
     */
    protected Page(PageManager pageManager, PageId pageId, ByteBuffer bb) {
        this.pageManager = pageManager;
        this.lock = pageManager.getLatchFactory().newReadWriteUpdateLatch();
        this.pageId = pageId;
        type = bb.getShort();
        pageLsn = new Lsn(bb);
    }

    /**
     * Returns the type code for the page. The type code is used to retrieve the
     * relevant {@link PageFactory} implementation from the
     * {@link ObjectRegistry}.
     * 
     * @return page type code
     */
    public final int getType() {
        return type;
    }

    /**
     * Returns the PageId for the page. The pageId identifies the storage
     * container and the page number.
     * 
     * @return {@link PageId}
     */
    public final PageId getPageId() {
        return pageId;
    }

    public final Lsn getPageLsn() {
        return pageLsn;
    }

    public final void setPageLsn(Lsn lsn) {
        pageLsn = lsn;
    }

    /**
     * Returns the page's on disk storage size. Cannot be over-ridden by derived
     * classes. {@inheritDoc}
     */
    public final int getStoredLength() {
        return pageManager.getPageSize();
    }

    /**
     * Returns the space available for sub-classes to use.
     * 
     * @return Space available for sub-classes to use.
     */
    public final int getAvailableLength() {
        /*
         * Get the usable page size and subtract this page's overhead.
         * The usable page size may be less than the full page size because
         * the PageManager may add additional bits.
         */
        return pageManager.getUsablePageSize() - SIZE;
    }

    /**
     * Serialize the contents of the page to the target ByteBuffer.
     * {@inheritDoc}
     */
    public void store(ByteBuffer bb) {
        bb.putShort(type);
        pageLsn.store(bb);
    }

    /**
     * Acquire an exclusive latch on this page.
     */
    public final void latchExclusive() {
        lock.exclusiveLock();
    }

    /**
     * Release an exclusive latch on this page.
     */
    public final void unlatchExclusive() {
        lock.unlockExclusive();
    }

    /**
     * Acquire a shared latch on this page.
     */
    public final void latchShared() {
        lock.sharedLock();
    }

    /**
     * Release a shared latch on this page.
     */
    public final void unlatchShared() {
        lock.unlockShared();
    }

    /**
     * Acquire an update latch on this page.
     */
    public final void latchUpdate() {
        lock.updateLock();
    }

    /**
     * Release an update latch on this page.
     */
    public final void unlatchUpdate() {
        lock.unlockUpdate();
    }

    /**
     * Promote an update latch to exclusive latch on this page.
     */
    public final void upgradeUpdate() {
        lock.upgradeUpdateLock();
    }

    /**
     * Demote an exclusive latch to an update latch on this page.
     */
    public final void downgradeExclusive() {
        lock.downgradeExclusiveLock();
    }

    @Override
    public final int hashCode() {
        return pageId.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final Page page = (Page) o;

        if (type != page.type)
            return false;
        if (!pageId.equals(page.pageId))
            return false;
        if (!pageLsn.equals(page.pageLsn))
            return false;

        return true;
    }

    public StringBuilder appendTo(StringBuilder sb) {
        sb.append("pageType=").append(type).append(", pageId=");
        pageId.appendTo(sb).append(", pageLsn=");
        pageLsn.appendTo(sb);
        return sb;
    }

    @Override
    public String toString() {
        return appendTo(new StringBuilder()).toString();
    }
}
