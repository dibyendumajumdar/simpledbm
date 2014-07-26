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
package org.simpledbm.rss.api.tx;

import java.nio.ByteBuffer;

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.Dumpable;
import org.simpledbm.common.util.TypeSize;

/**
 * Transaction Identifier. Must be a monotonically increasing value that can be
 * converted to an integer. This property is useful because it allows the ids to
 * be mapped to bitmaps.
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
