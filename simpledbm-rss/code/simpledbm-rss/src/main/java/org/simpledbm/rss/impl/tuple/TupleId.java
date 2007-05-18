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
import org.simpledbm.rss.api.tx.BaseLockable;
import org.simpledbm.rss.util.Dumpable;
import org.simpledbm.rss.util.TypeSize;

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
public class TupleId extends BaseLockable implements Location, Dumpable {
	
	PageId pageId;
	int slotNumber;

	public TupleId() {
		super((byte)'T');
		pageId = new PageId();
		slotNumber = -1;
	}

	public TupleId(TupleId other) {
		super((byte)'T');
		pageId = new PageId(other.pageId);
		slotNumber = other.slotNumber;
	}

	public TupleId(PageId pageId, int slotNumber) {
		super((byte)'T');
		setPageId(pageId);
		setSlotNumber(slotNumber);
	}

	public final boolean isNull() {
		return pageId.isNull() || slotNumber == -1;
	}

	public void parseString(String string) {
		throw new UnsupportedOperationException("SIMPLEDBM-ERROR: This operation is not yet implemented");
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

	public final int compareTo(Location arg0) {
		if (arg0 == this) {
			return 0;
		}
		if (!(arg0 instanceof TupleId)) {
			throw new IllegalArgumentException("SIMPLEDBM-ERROR: Object " + arg0 + " is not of the required type");
		}
		TupleId other = (TupleId) arg0;
		int comp = pageId.compareTo(other.pageId);
		if (comp == 0) {
			comp = slotNumber - other.slotNumber;
		}
		return comp;
	}

	public StringBuilder appendTo(StringBuilder sb) {
		sb.append("TupleId(");
		pageId.appendTo(sb);
		sb.append(", slot=").append(slotNumber).append(")");
		return sb;
	}

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
			throw new IllegalStateException("TupleId has not been initialized");
		}
		return pageId.getContainerId();
	}

	public int hashCode() {
		final int PRIME = 31;
		int result = super.hashCode();
		result = PRIME * result + ((pageId == null) ? 0 : pageId.hashCode());
		result = PRIME * result + slotNumber;
		return result;
	}

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