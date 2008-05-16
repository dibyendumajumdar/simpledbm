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

import org.simpledbm.database.api.Table;
import org.simpledbm.database.api.TableDefinition;
import org.simpledbm.rss.api.im.IndexContainer;
import org.simpledbm.rss.api.im.IndexScan;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.tuple.TupleContainer;
import org.simpledbm.rss.api.tuple.TupleInserter;
import org.simpledbm.rss.api.tx.Savepoint;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.typesystem.api.Row;

/**
 * A Table instance
 * 
 * @author Dibyendu Majumdar
 */
public class TableImpl implements Table {

    private final TableDefinition definition;

    public TableImpl(TableDefinition definition) {
    	this.definition = definition;
    }
    
    /* (non-Javadoc)
	 * @see org.simpledbm.database.Table#addRow(org.simpledbm.rss.api.tx.Transaction, org.simpledbm.typesystem.api.Row)
	 */
    public Location addRow(Transaction trx, Row tableRow) {

        Location location = null;
        // Create a savepoint so that we can rollback to a consistent
        // state in case there is a problem.
        Savepoint savepoint = trx.createSavepoint(false);
        boolean success = false;
        try {
            // Get a handle to the table container.
            // Note - will be locked in shared mode.
            TupleContainer table = getDefinition().getDatabase().getServer().getTupleContainer(trx,
                    getDefinition().getContainerId());

            // Lets create the new row and lock the location
            TupleInserter inserter = table.insert(trx, tableRow);

            // Insert the keys. The first key should be the primary key.
            // Insertion of primary key may fail with unique constraint
            // violation
            for (IndexDefinition idx : getDefinition().getIndexes()) {
                IndexContainer index = getDefinition().getDatabase().getServer().getIndex(trx,
                        idx.containerId);
                Row indexRow = getDefinition().getIndexRow(idx, tableRow);
                index.insert(trx, indexRow, inserter.getLocation());
            }
            // All keys inserted successfully, so now complete the insert
            inserter.completeInsert();
            location = inserter.getLocation();
            success = true;
        } finally {
            if (!success) {
                // Oops - need to rollback to the savepoint
                trx.rollback(savepoint);
            }
        }
        return location;
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.Table#updateRow(org.simpledbm.rss.api.tx.Transaction, org.simpledbm.typesystem.api.Row)
	 */
    public void updateRow(Transaction trx, Row tableRow) {

        // Start a new transaction
        Savepoint sp = trx.createSavepoint(false);
        boolean success = false;
        try {
            TupleContainer table = getDefinition().getDatabase().getServer().getTupleContainer(trx,
                    getDefinition().getContainerId());

            IndexDefinition pkey = getDefinition().getIndexes().get(0);
            // New primary key
            Row primaryKeyRow = getDefinition().getIndexRow(pkey, tableRow);

            IndexContainer primaryIndex = getDefinition().getDatabase().getServer().getIndex(trx,
                    pkey.containerId);

            // Start a scan, with the primary key as argument
            IndexScan indexScan = primaryIndex.openScan(trx, primaryKeyRow,
                    null, true);
            if (indexScan.fetchNext()) {
                // Scan always return item >= search key, so let's
                // check if we had an exact match
                boolean matched = indexScan.getCurrentKey().equals(
                        primaryKeyRow);
                try {
                    if (matched) {
                        // Get location of the tuple
                        Location location = indexScan.getCurrentLocation();
                        // We need the old row data to be able to delete indexes
                        // fetch tuple data
                        byte[] data = table.read(location);
                        // parse the data
                        ByteBuffer bb = ByteBuffer.wrap(data);
                        Row oldTableRow = getDefinition().getRow();
                        oldTableRow.retrieve(bb);
                        // Okay, now update the table row
                        table.update(trx, location, tableRow);
                        // Update secondary indexes
                        // Old secondary key
                        for (int i = 1; i < getDefinition().getIndexes().size(); i++) {
                            IndexDefinition skey = getDefinition().getIndexes().get(i);
                            IndexContainer secondaryIndex = getDefinition().getDatabase().getServer().getIndex(trx, skey.containerId);
                            // old secondary key
                            Row oldSecondaryKeyRow = getDefinition().getIndexRow(skey,
                                    oldTableRow);
                            // New secondary key
                            Row secondaryKeyRow = getDefinition().getIndexRow(skey, tableRow);
                            if (!oldSecondaryKeyRow.equals(secondaryKeyRow)) {
                                // Delete old key
                                secondaryIndex.delete(trx, oldSecondaryKeyRow,
                                        location);
                                // Insert new key
                                secondaryIndex.insert(trx, secondaryKeyRow,
                                        location);
                            }
                        }
                    }
                } finally {
                    indexScan.fetchCompleted(matched);
                }
            }
            success = true;
        } finally {
            if (!success) {
                trx.rollback(sp);
            }
        }
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.Table#deleteRow(org.simpledbm.rss.api.tx.Transaction, org.simpledbm.typesystem.api.Row)
	 */
    public void deleteRow(Transaction trx, Row tableRow) {

        // Start a new transaction
        Savepoint sp = trx.createSavepoint(false);
        boolean success = false;
        try {
            TupleContainer table = getDefinition().getDatabase().getServer().getTupleContainer(trx,
                    getDefinition().getContainerId());

            IndexDefinition pkey = getDefinition().getIndexes().get(0);
            // New primary key
            Row primaryKeyRow = getDefinition().getIndexRow(pkey, tableRow);

            IndexContainer primaryIndex = getDefinition().getDatabase().getServer().getIndex(trx,
                    pkey.containerId);

            // Start a scan, with the primary key as argument
            IndexScan indexScan = primaryIndex.openScan(trx, primaryKeyRow,
                    null, true);
            if (indexScan.fetchNext()) {
                // Scan always return item >= search key, so let's
                // check if we had an exact match
                boolean matched = indexScan.getCurrentKey().equals(
                        primaryKeyRow);
                try {
                    if (matched) {
                        // Get location of the tuple
                        Location location = indexScan.getCurrentLocation();
                        // We need the old row data to be able to delete indexes
                        // fetch tuple data
                        byte[] data = table.read(location);
                        // parse the data
                        ByteBuffer bb = ByteBuffer.wrap(data);
                        Row oldTableRow = getDefinition().getRow();
                        oldTableRow.retrieve(bb);
                        // Okay, now update the table row
                        table.delete(trx, location);
                        // Update secondary indexes
                        // Old secondary key
                        for (int i = 1; i < getDefinition().getIndexes().size(); i++) {
                            IndexDefinition skey = getDefinition().getIndexes().get(i);
                            IndexContainer secondaryIndex = getDefinition().getDatabase().getServer().getIndex(trx, skey.containerId);
                            // old secondary key
                            Row oldSecondaryKeyRow = getDefinition().getIndexRow(skey,
                                    oldTableRow);
                            // Delete old key
                            secondaryIndex.delete(trx, oldSecondaryKeyRow,
                                    location);
                        }
                        primaryIndex.delete(trx, primaryKeyRow, location);
                    }
                } finally {
                    indexScan.fetchCompleted(matched);
                }
            }
            success = true;
        } finally {
            if (!success) {
                trx.rollback(sp);
            }
        }
    }

    /* (non-Javadoc)
	 * @see org.simpledbm.database.Table#openScan(org.simpledbm.rss.api.tx.Transaction, int, org.simpledbm.typesystem.api.Row, boolean)
	 */
    public TableScan openScan(Transaction trx, int indexno, Row startRow,
            boolean forUpdate) {
        return new TableScan(trx, this, indexno, startRow, forUpdate);
    }

	public TableDefinition getDefinition() {
		return definition;
	}
}
