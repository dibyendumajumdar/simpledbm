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
package org.simpledbm.rss.impl.tuple;

import java.nio.ByteBuffer;

import org.simpledbm.common.util.Dumpable;
import org.simpledbm.common.util.TypeSize;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.tx.BaseLockable;

/**
 * TupleId uniquely identifies the location of a tuple within the Relation.
 * It consists of the page ID and the slot number.
 * <p>
 * Immutable.
 * 
 * @author Dibyendu Majumdar
 * @since 08-Dec-2005
 */
public final class TupleId extends BaseLockable implements Location, Dumpable {

//    private static final Logger log = Logger.getLogger(TupleManager.LOGGER_NAME);
//    private static final ExceptionHandler exceptionHandler = ExceptionHandler.getExceptionHandler(log);
//    private static final MessageCatalog mcat = MessageCatalog.getMessageCatalog();

    private final PageId pageId;
    private final int slotNumber;

    public TupleId() {
        super((byte) 'T');
        pageId = new PageId();
        slotNumber = -1;
    }

    public TupleId(TupleId other) {
        super((byte) 'T');
        pageId = other.pageId;
        slotNumber = other.slotNumber;
    }

    public TupleId(PageId pageId, int slotNumber) {
        super((byte) 'T');
        this.pageId = pageId;
        this.slotNumber = slotNumber;
    }

    public TupleId(ByteBuffer bb) {
        super((byte) 'T');
        pageId = new PageId(bb);
        slotNumber = bb.getShort();
    }

    public TupleId(String string) {
        super((byte) 'T');
        pageId = new PageId();
        slotNumber = -1;
        throw new UnsupportedOperationException();
//        exceptionHandler.errorThrow(this.getClass().getName(), "TupleId", 
//        		new TupleException(mcat.getMessage("ET0001")));
    }    
    
    public Location cloneLocation() {
		return new TupleId(this);
	}

	public final boolean isNull() {
        return pageId.isNull() || slotNumber == -1;
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
        	throw new IllegalArgumentException();
//            log.error(this.getClass().getName(), "compareTo", mcat.getMessage(
//                "ET0002",
//                location,
//                getClass().getName()));
//            throw new TupleException(mcat.getMessage(
//                "ET0002",
//                location,
//                getClass().getName()));
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

    public final PageId getPageId() {
        return pageId;
    }

    public final int getSlotNumber() {
        return slotNumber;
    }

    public int getContainerId() {
        if (pageId == null) {
        	throw new IllegalStateException();
//            log.error(this.getClass().getName(), "getContainerId", mcat
//                .getMessage("ET0003", this));
//            throw new TupleException(mcat.getMessage("ET0003", this));
        }
        return pageId.getContainerId();
    }
    
    public int getX() {
        if (pageId == null) {
        	throw new IllegalStateException();
//            log.error(this.getClass().getName(), "getX", mcat
//                .getMessage("ET0003", this));
//            throw new TupleException(mcat.getMessage("ET0003", this));
        }
        return pageId.getPageNumber();
    }
    
    public int getY() {
        if (pageId == null) {
        	throw new IllegalStateException();
//            log.error(this.getClass().getName(), "getY", mcat
//                .getMessage("ET0003", this));
//            throw new TupleException(mcat.getMessage("ET0003", this));
        }
        return slotNumber;
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