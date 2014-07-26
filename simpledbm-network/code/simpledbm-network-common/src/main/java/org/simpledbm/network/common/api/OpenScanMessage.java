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
package org.simpledbm.network.common.api;

import java.nio.ByteBuffer;

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.TypeSize;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.RowFactory;

public class OpenScanMessage implements Storable {

    final int containerId;
    final int indexNo;
    final Row startRow;
    final boolean forUpdate;

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
        } else {
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

    public int getIndexNo() {
        return indexNo;
    }

    public Row getStartRow() {
        return startRow;
    }

    public boolean isForUpdate() {
        return forUpdate;
    }

    @Override
    public String toString() {
        return "OpenScanMessage [containerId=" + containerId + ", forUpdate="
                + forUpdate + ", indexNo=" + indexNo + ", startRow=" + startRow
                + "]";
    }
}
