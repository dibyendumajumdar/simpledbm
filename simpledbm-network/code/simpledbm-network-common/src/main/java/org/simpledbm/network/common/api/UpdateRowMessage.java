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
package org.simpledbm.network.common.api;

import java.nio.ByteBuffer;

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.TypeSize;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.RowFactory;

public class UpdateRowMessage implements Storable {

    final int scanId;
    final int tableId;
    final Row row;

    public UpdateRowMessage(int scanId, int tableId, Row row) {
        this.scanId = scanId;
        this.tableId = tableId;
        this.row = row;
    }

    public UpdateRowMessage(RowFactory rowFactory, ByteBuffer bb) {
        scanId = bb.getInt();
        tableId = bb.getInt();
        row = rowFactory.newRow(tableId, bb);
    }

    public int getStoredLength() {
        return TypeSize.INTEGER * 2 + row.getStoredLength();
    }

    public void store(ByteBuffer bb) {
        bb.putInt(scanId);
        bb.putInt(tableId);
        row.store(bb);
    }

    public int getScanId() {
        return scanId;
    }

    public Row getRow() {
        return row;
    }

    @Override
    public String toString() {
        return "UpdateRowMessage [row=" + row + ", scanId=" + scanId
                + ", tableId=" + tableId + "]";
    }

}
