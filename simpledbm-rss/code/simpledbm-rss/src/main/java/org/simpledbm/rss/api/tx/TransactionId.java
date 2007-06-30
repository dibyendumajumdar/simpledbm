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
package org.simpledbm.rss.api.tx;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.st.Storable;
import org.simpledbm.rss.util.Dumpable;
import org.simpledbm.rss.util.TypeSize;

/**
 * Transaction Identifier. Must be a monotonically increasing value that 
 * can be converted to an integer. This property is useful because it allows the
 * ids to be mapped to bitmaps.
 * 
 * @author Dibyendu Majumdar
 * @since 23-Aug-2005
 */
public final class TransactionId implements Storable, Dumpable {

    public static final int SIZE = TypeSize.LONG;

    private long id;

    public TransactionId() {
        id = -1;
    }

    public TransactionId(long id) {
        this.id = id;
    }

    public final void retrieve(ByteBuffer bb) {
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
    public final boolean equals(Object obj) {
        if (obj instanceof TransactionId) {
            return id == ((TransactionId) obj).id;
        }
        return false;
    }

    @Override
    public final int hashCode() {
        return (int) (id ^ (id >>> 32));
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
