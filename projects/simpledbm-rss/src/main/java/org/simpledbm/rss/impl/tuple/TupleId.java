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
package org.simpledbm.rss.impl.tuple;

import java.nio.ByteBuffer;

import org.simpledbm.common.util.Dumpable;
import org.simpledbm.common.util.TypeSize;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.tx.BaseLockable;

/**
 * TupleId uniquely identifies the location of a tuple within the Relation. It
 * consists of the page ID and the slot number.
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