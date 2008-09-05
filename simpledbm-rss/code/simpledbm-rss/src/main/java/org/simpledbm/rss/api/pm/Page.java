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
package org.simpledbm.rss.api.pm;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.latch.Latch;
import org.simpledbm.rss.api.st.Storable;
import org.simpledbm.rss.api.wal.Lsn;
import org.simpledbm.rss.util.Dumpable;
import org.simpledbm.rss.util.TypeSize;

/**
 * The root of all Page implementations. The basic most common page attributes
 * are implemented. Page Size is fixed and cannot be changed by implementations.
 * 
 * @author Dibyendu Majumdar
 * @since 14-Aug-2005
 */
public abstract class Page implements Storable, Dumpable {

    public static final int SIZE = TypeSize.SHORT + Lsn.SIZE;

    /**
     * The type code for the page. Used to re-create correct type of
     * page when reading from disk.
     */
    private final short type;

    /**
     * The identity of the page.
     * Transient.
     */
    private final PageId pageId;
    
    /**
     * Page LSN is the LSN of the last log record that made
     * changes to the page. 
     */
    private volatile Lsn pageLsn = new Lsn();

    /**
     * PageFactory that created this page. Note that the page
     * size is fixed by the factory.
     * Transient.
     */
    private final PageFactory pageFactory;

    /**
     * A read/write latch to protect access to the page.
     * Transient.
     */
    protected final Latch lock;

    protected Page(PageFactory pageFactory, int type, PageId pageId) {
    	this.pageFactory = pageFactory;
    	this.type = (short) type;
    	this.pageId = pageId;
    	this.lock = pageFactory.getLatchFactory().newReadWriteUpdateLatch();
    }

    protected Page(PageFactory pageFactory, PageId pageId, ByteBuffer bb) {
    	this.pageFactory = pageFactory;
    	this.lock = pageFactory.getLatchFactory().newReadWriteUpdateLatch();
    	this.pageId = pageId;
        type = bb.getShort();
        pageLsn = new Lsn(bb);
    }    
    
    public final int getType() {
        return type;
    }

    public final PageId getPageId() {
        return pageId;
    }

    public final Lsn getPageLsn() {
        return pageLsn;
    }

    public final void setPageLsn(Lsn lsn) {
        pageLsn = new Lsn(lsn);
    }

    /**
     * @see org.simpledbm.rss.api.st.Storable#getStoredLength()
     */
    public final int getStoredLength() {
        return pageFactory.getUsablePageSize();
    }

    public void store(ByteBuffer bb) {
        bb.putShort(type);
        pageLsn.store(bb);
    }

    public final void latchExclusive() {
        lock.exclusiveLock();
    }

    public final void unlatchExclusive() {
        lock.unlockExclusive();
    }

    public final void latchShared() {
        lock.sharedLock();
    }

    public final void unlatchShared() {
        lock.unlockShared();
    }

    public final void latchUpdate() {
        lock.updateLock();
    }

    public final void unlatchUpdate() {
        lock.unlockUpdate();
    }

    public final void upgradeUpdate() {
        lock.upgradeUpdateLock();
    }

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
