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
package org.simpledbm.rss.api.tx;

import java.nio.ByteBuffer;

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.Dumpable;
import org.simpledbm.common.util.TypeSize;

/**
 * Transaction Identifier. Must be a monotonically increasing value that 
 * can be converted to an integer. This property is useful because it allows the
 * ids to be mapped to bitmaps.
 * <p>
 * Immutable.
 * 
 * @author Dibyendu Majumdar
 * @since 23-Aug-2005
 */
public final class TransactionId implements Storable, Dumpable {

    public static final int SIZE = TypeSize.LONG;

    private final long id;

    public TransactionId() {
        id = -1;
    }

    public TransactionId(long id) {
        this.id = id;
    }

    public TransactionId(ByteBuffer bb) {
        id = bb.getLong();
    }
    
    public final void store(ByteBuffer bb) {
        bb.putLong(id);
    }

    public final int getStoredLength() {
        return SIZE;
    }

    public final long longValue() {
        return id;
    }

	@Override
	public int hashCode() {
		final int PRIME = 31;
		int result = 1;
		result = PRIME * result + (int) (id ^ (id >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		final TransactionId other = (TransactionId) obj;
		if (id != other.id)
			return false;
		return true;
	}
    
	public final boolean isNull() {
        return id == -1;
    }

    public final StringBuilder appendTo(StringBuilder sb) {
        sb.append("TrxId(").append(id).append(")");
        return sb;
    }

    @Override
    public final String toString() {
        return appendTo(new StringBuilder()).toString();
    }
}
