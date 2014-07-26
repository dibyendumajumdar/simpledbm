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
package org.simpledbm.rss.api.wal;

import java.nio.ByteBuffer;

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.Dumpable;
import org.simpledbm.common.util.TypeSize;

/**
 * Lsn is short for Log Sequence Number, and is a unique monotonically
 * increasing numeric id given to log records.
 * <p>
 * Immutable.
 * 
 * @author dibyendu
 * @since 10-June-2005
 * 
 */
public final class Lsn implements Comparable<Lsn>, Storable, Dumpable {

    /**
     * Size of Lsn in bytes.
     */
    public final static int SIZE = TypeSize.INTEGER * 2;

    /**
     * The index identifies the log file by number.
     */
    private final int index;

    /**
     * The offset is the position within the Log file.
     */
    private final int offset;

    public Lsn() {
        index = 0;
        offset = 0;
    }

    public Lsn(int index, int offset) {
        this.index = index;
        this.offset = offset;
    }

    public Lsn(Lsn lsn) {
        this.index = lsn.index;
        this.offset = lsn.offset;
    }

    public Lsn(ByteBuffer bb) {
        index = bb.getInt();
        offset = bb.getInt();
    }

    public final boolean isNull() {
        return index == 0 && offset == 0;
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj instanceof Lsn) {
            Lsn lsn = (Lsn) obj;
            return index == lsn.index && offset == lsn.offset;
        }
        return false;
    }

    public final int compareTo(Lsn lsn) {
        if (index == lsn.index) {
            if (offset == lsn.offset)
                return 0;
            else if (offset > lsn.offset)
                return 1;
            else
                return -1;
        } else if (index > lsn.index)
            return 1;
        else
            return -1;
    }

    public final boolean lessThan(Lsn lsn) {
        return compareTo(lsn) < 0;
    }

    public final int getStoredLength() {
        return SIZE;
    }

    public final int getIndex() {
        return index;
    }

    public final int getOffset() {
        return offset;
    }

    public final void store(ByteBuffer bb) {
        bb.putInt(index);
        bb.putInt(offset);
    }

    public StringBuilder appendTo(StringBuilder sb) {
        sb.append("Lsn(").append(index).append(",").append(offset).append(")");
        return sb;
    }

    @Override
    public final String toString() {
        return appendTo(new StringBuilder()).toString();
    }

    @Override
    public int hashCode() {
        return index ^ offset;
    }

}
