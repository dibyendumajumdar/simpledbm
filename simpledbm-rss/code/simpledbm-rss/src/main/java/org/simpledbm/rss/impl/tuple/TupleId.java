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
package org.simpledbm.rss.impl.tuple;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.tuple.TupleException;
import org.simpledbm.rss.api.tx.BaseLockable;
import org.simpledbm.rss.util.Dumpable;
import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;

/**
 * TupleId uniquely identifies the location of a tuple within the Relation.
 * It consists of the page ID and the slot number.
 * <p>
 * Note that we take care to ensure that instances of this object are
 * immutable - the only way to modify them is to read 
 * 
 * @author Dibyendu Majumdar
 * @since 08-Dec-2005
 */
public final class TupleId extends BaseLockable implements Location, Dumpable {

    private static final Logger log = Logger.getLogger(TupleId.class
        .getPackage()
        .getName());
    private static final MessageCatalog mcat = new MessageCatalog();

    PageId pageId;
    int slotNumber;

    public TupleId() {
        super((byte) 'T');
        pageId = new PageId();
        slotNumber = -1;
    }

    public TupleId(TupleId other) {
        super((byte) 'T');
        pageId = new PageId(other.pageId);
        slotNumber = other.slotNumber;
    }

    public TupleId(PageId pageId, int slotNumber) {
        super((byte) 'T');
        setPageId(pageId);
        setSlotNumber(slotNumber);
    }

    public final boolean isNull() {
        return pageId.isNull() || slotNumber == -1;
    }

    public void parseString(String string) {
        log.error(this.getClass().getName(), "parseString", mcat
            .getMessage("ET0001"));
        throw new TupleException(mcat.getMessage("ET0001"));
    }

    public final void retrieve(ByteBuffer bb) {
        pageId = new PageId();
        pageId.retrieve(bb);
        slotNumber = bb.getShort();
    }

    public final void store(ByteBuffer bb) {
        pageId.store(bb);
        bb.putShort((short) slotNumber);
    }

    public final int getStoredLength() {
        return pageId.getStoredLength() + TypeSize.SHORT;
    }

    public final int compareTo(Location location) {
        if (location == this) {
            return 0;
        }
        if (getClass() != location.getClass()) {
            log.error(this.getClass().getName(), "compareTo", mcat.getMessage(
                "ET0002",
                location,
                getClass().getName()));
            throw new TupleException(mcat.getMessage(
                "ET0002",
                location,
                getClass().getName()));
        }
        TupleId other = (TupleId) location;
        int comp = pageId.compareTo(other.pageId);
        if (comp == 0) {
            comp = slotNumber - other.slotNumber;
        }
        return comp;
    }

    @Override
    public StringBuilder appendTo(StringBuilder sb) {
        sb.append("TupleId(");
        super.appendTo(sb).append(", pageId=");
        pageId.appendTo(sb);
        sb.append(", slot=").append(slotNumber).append(")");
        return sb;
    }

    @Override
    public final String toString() {
        return appendTo(new StringBuilder()).toString();
    }

    private final void setPageId(PageId pageId) {
        this.pageId = new PageId(pageId);
    }

    public final PageId getPageId() {
        return pageId;
    }

    private final void setSlotNumber(int slotNumber) {
        this.slotNumber = slotNumber;
    }

    public final int getSlotNumber() {
        return slotNumber;
    }

    public int getContainerId() {
        if (pageId == null) {
            log.error(this.getClass().getName(), "getContainerId", mcat
                .getMessage("ET0003", this));
            throw new TupleException(mcat.getMessage("ET0003", this));
        }
        return pageId.getContainerId();
    }

    @Override
    public int hashCode() {
        final int PRIME = 31;
        int result = super.hashCode();
        result = PRIME * result + ((pageId == null) ? 0 : pageId.hashCode());
        result = PRIME * result + slotNumber;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        final TupleId other = (TupleId) obj;
        if (pageId == null) {
            if (other.pageId != null)
                return false;
        } else if (!pageId.equals(other.pageId))
            return false;
        if (slotNumber != other.slotNumber)
            return false;
        return true;
    }

}