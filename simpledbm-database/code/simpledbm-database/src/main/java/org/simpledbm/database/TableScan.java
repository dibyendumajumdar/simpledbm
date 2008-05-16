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
package org.simpledbm.database;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.im.IndexContainer;
import org.simpledbm.rss.api.im.IndexScan;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.tuple.TupleContainer;
import org.simpledbm.rss.api.tx.Savepoint;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.typesystem.api.Row;

public class TableScan {

    final TableImpl table;
    final IndexScan indexScan;
    final Row startRow;
    final TupleContainer tcont;
    final IndexContainer icont;
    final Transaction trx;
    Row currentRow;

    TableScan(Transaction trx, TableImpl table, int indexNo, Row tableRow, boolean forUpdate) {
        this.table = table;
        this.trx = trx;
        tcont = table.definition.getDatabase().getServer().getTupleContainer(trx, table.definition.getContainerId());
        IndexDefinition index = table.definition.getIndexes().get(indexNo);
        icont = table.definition.getDatabase().getServer().getIndex(trx, index.containerId);
        this.startRow = table.definition.getIndexRow(index, tableRow);
        indexScan = icont.openScan(trx, startRow, null, forUpdate);
    }

    public boolean fetchNext() {
        boolean okay = indexScan.fetchNext();
        if (okay) {
            Location location = indexScan.getCurrentLocation();
            // fetch tuple data
            byte[] data = tcont.read(location);
            // parse the data
            ByteBuffer bb = ByteBuffer.wrap(data);
            currentRow = table.definition.getRow();
            currentRow.retrieve(bb);
        }
        return okay;
    }

    public Row getCurrentRow() {
        return currentRow;
    }

    public Row getCurrentIndexRow() {
        return (Row) indexScan.getCurrentKey();
    }

    public void fetchCompleted(boolean matched) {
        indexScan.fetchCompleted(matched);
    }

    public void close() {
        indexScan.close();
    }

    public void updateCurrentRow(Row tableRow) {
        // Start a new transaction
        Savepoint sp = trx.createSavepoint(false);
        boolean success = false;
        try {
            IndexDefinition pkey = table.definition.getIndexes().get(0);
            // New secondary key
            Row newPrimaryKeyRow = table.definition.getIndexRow(pkey, tableRow);
            if (indexScan.getCurrentKey().equals(newPrimaryKeyRow)) {
                // Get location of the tuple
                Location location = indexScan.getCurrentLocation();
                // We need the old row data to be able to delete indexes
                // fetch tuple data
                byte[] data = tcont.read(location);
                // parse the data
                ByteBuffer bb = ByteBuffer.wrap(data);
                Row oldTableRow = table.definition.getRow();
                oldTableRow.retrieve(bb);
                // Okay, now update the table row
                tcont.update(trx, location, tableRow);
                // Update secondary indexes
                // Old secondary key
                for (int i = 1; i < table.definition.getIndexes().size(); i++) {
                    IndexDefinition skey = table.definition.getIndexes().get(i);
                    IndexContainer secondaryIndex = table.definition.getDatabase().getServer().getIndex(trx, skey.containerId);
                    // old secondary key
                    Row oldSecondaryKeyRow = table.definition.getIndexRow(skey, tableRow);
                    // New secondary key
                    Row secondaryKeyRow = table.definition.getIndexRow(skey, tableRow);
                    if (!oldSecondaryKeyRow.equals(secondaryKeyRow)) {
                        // Delete old key
                        secondaryIndex.delete(trx, oldSecondaryKeyRow, location);
                        // Insert new key
                        secondaryIndex.insert(trx, secondaryKeyRow, location);
                    }
                }
            } else {
                table.addRow(trx, tableRow);
            }
            success = true;
        } finally {
            if (!success) {
                trx.rollback(sp);
            }
        }
    }

    public void deleteRow() {
        // Start a new transaction
        Savepoint sp = trx.createSavepoint(false);
        boolean success = false;
        try {
            // Get location of the tuple
            Location location = indexScan.getCurrentLocation();
            // We need the old row data to be able to delete indexes
            // fetch tuple data
            byte[] data = tcont.read(location);
            // parse the data
            ByteBuffer bb = ByteBuffer.wrap(data);
            Row oldTableRow = table.definition.getRow();
            oldTableRow.retrieve(bb);
            // Okay, now update the table row
            tcont.delete(trx, location);
            // Update indexes
            for (int i = table.definition.getIndexes().size() - 1; i >= 0; i--) {
                IndexDefinition skey = table.definition.getIndexes().get(i);
                IndexContainer index = table.definition.getDatabase().getServer().getIndex(
                        trx, skey.containerId);
                // old secondary key
                Row indexRow = table.definition.getIndexRow(skey, oldTableRow);
                // Delete old key
                index.delete(trx, indexRow, location);
            }
            success = true;
        } finally {
            if (!success) {
                trx.rollback(sp);
            }
        }
    }
}
