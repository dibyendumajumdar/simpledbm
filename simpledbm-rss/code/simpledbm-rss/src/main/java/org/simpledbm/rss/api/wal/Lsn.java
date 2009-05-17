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
package org.simpledbm.rss.api.wal;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.registry.Storable;
import org.simpledbm.rss.util.Dumpable;
import org.simpledbm.rss.util.TypeSize;

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
