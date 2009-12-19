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

public class OpenScanMessage implements Storable {

	int containerId;
	int indexNo;
	Row startRow;
	boolean forUpdate;

	public OpenScanMessage(int containerId, int indexNo, Row startRow,
			boolean forUpdate) {
		this.containerId = containerId;
		this.indexNo = indexNo;
		this.startRow = startRow;
		this.forUpdate = forUpdate;
	}

	public OpenScanMessage(RowFactory rowFactory, ByteBuffer bb) {
		containerId = bb.getInt();
		indexNo = bb.getInt();
		boolean hasStartRow = (bb.get() == 1);
		if (hasStartRow) {
			startRow = rowFactory.newRow(containerId, bb);
		}
		else {
			startRow = null;
		}
		forUpdate = (bb.get() == 1 ? true : false);
	}

	public int getStoredLength() {
		return TypeSize.INTEGER * 2
				+ (startRow != null ? startRow.getStoredLength() : 0)
				+ TypeSize.BYTE * 2;
	}

	public void store(ByteBuffer bb) {
		bb.putInt(containerId);
		bb.putInt(indexNo);
		bb.put((byte) (startRow != null ? 1 : 0));
		if (startRow != null) {
			startRow.store(bb);
		}
		bb.put((byte) (forUpdate ? 1 : 0));
	}

	public int getContainerId() {
		return containerId;
	}

	public void setContainerId(int containerId) {
		this.containerId = containerId;
	}

	public int getIndexNo() {
		return indexNo;
	}

	public void setIndexNo(int indexNo) {
		this.indexNo = indexNo;
	}

	public Row getStartRow() {
		return startRow;
	}

	public void setStartRow(Row startRow) {
		this.startRow = startRow;
	}

	public boolean isForUpdate() {
		return forUpdate;
	}

	public void setForUpdate(boolean forUpdate) {
		this.forUpdate = forUpdate;
	}

}
