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
package org.simpledbm.database.impl;

import static org.simpledbm.database.impl.DatabaseImpl.m_ED0015;

import java.nio.ByteBuffer;

import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.database.api.Database;
import org.simpledbm.database.api.Table;
import org.simpledbm.database.api.TableScan;
import org.simpledbm.exception.DatabaseException;
import org.simpledbm.rss.api.im.IndexContainer;
import org.simpledbm.rss.api.im.IndexScan;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.tuple.TupleContainer;
import org.simpledbm.rss.api.tuple.TupleInserter;
import org.simpledbm.rss.api.tx.Savepoint;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.typesystem.api.IndexDefinition;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.TableDefinition;

/**
 * A Table instance.
 * 
 * @author Dibyendu Majumdar
 */
public class TableImpl implements Table {

    final Logger log;
    final PlatformObjects po;

    final Database database;
    private final TableDefinition definition;

    TableImpl(PlatformObjects po, Database database, TableDefinition definition) {
        this.po = po;
        this.log = po.getLogger();
        this.database = database;
        this.definition = definition;
    }

    public Database getDatabase() {
        return database;
    }

    public boolean validateRow(Row tableRow) {
        for (IndexDefinition idx : getDefinition().getIndexes()) {
            Row indexRow = getDefinition().getIndexRow(idx, tableRow);
            for (int i = 0; i < indexRow.getNumberOfColumns(); i++) {
                if (indexRow.isNull(i)) {
                    return false;
                }
            }
        }
        return true;
    }

    /* (non-Javadoc)
     * @see org.simpledbm.database.Table#addRow(org.simpledbm.rss.api.tx.Transaction, org.simpledbm.typesystem.api.Row)
     */
    public Location addRow(Transaction trx, Row tableRow) {

        if (!validateRow(tableRow)) {
            log.error(getClass().getName(), "addRow", new MessageInstance(
                    m_ED0015, tableRow).toString());
            throw new DatabaseException(new MessageInstance(m_ED0015, tableRow));
        }
        Location location = null;
        // Create a savepoint so that we can rollback to a consistent
        // state in case there is a problem.
        Savepoint savepoint = trx.createSavepoint(false);
        boolean success = false;
        try {
            // Get a handle to the table container.
            // Note - will be locked in shared mode.
            TupleContainer table = database.getServer().getTupleContainer(trx,
                    getDefinition().getContainerId());

            // Lets create the new row and lock the location
            TupleInserter inserter = table.insert(trx, tableRow);

            // Insert the keys. The first key should be the primary key.
            // Insertion of primary key may fail with unique constraint
            // violation
            for (IndexDefinition idx : getDefinition().getIndexes()) {
                IndexContainer index = database.getServer().getIndex(trx,
                        idx.getContainerId());
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

        if (!validateRow(tableRow)) {
            log.error(getClass().getName(), "updateRow", new MessageInstance(
                    m_ED0015, tableRow).toString());
            throw new DatabaseException(new MessageInstance(m_ED0015, tableRow));
        }
        // Start a new transaction
        Savepoint sp = trx.createSavepoint(false);
        boolean success = false;
        try {
            TupleContainer table = database.getServer().getTupleContainer(trx,
                    getDefinition().getContainerId());

            IndexDefinition pkey = getDefinition().getIndexes().get(0);
            // New primary key
            Row primaryKeyRow = getDefinition().getIndexRow(pkey, tableRow);

            IndexContainer primaryIndex = database.getServer().getIndex(trx,
                    pkey.getContainerId());

            // Start a scan, with the primary key as argument
            IndexScan indexScan = primaryIndex.openScan(trx, primaryKeyRow,
                    null, true);
            try {
                if (indexScan.fetchNext()) {
                    // Scan always return item >= search key, so let's
                    // check if we had an exact match
                    boolean matched = indexScan.getCurrentKey().equals(
                            primaryKeyRow);
                    try {
                        if (matched) {
                            // Get location of the tuple
                            Location location = indexScan.getCurrentLocation();
                            // We need the old row data to be able to delete
                            // indexes
                            // fetch tuple data
                            byte[] data = table.read(location);
                            // parse the data
                            ByteBuffer bb = ByteBuffer.wrap(data);
                            Row oldTableRow = getDefinition().getRow(bb);
                            // oldTableRow.retrieve(bb);
                            // Okay, now update the table row
                            table.update(trx, location, tableRow);
                            // Update secondary indexes
                            // Old secondary key
                            for (int i = 1; i < getDefinition().getIndexes()
                                    .size(); i++) {
                                IndexDefinition skey = getDefinition()
                                        .getIndexes().get(i);
                                IndexContainer secondaryIndex = database
                                        .getServer().getIndex(trx,
                                                skey.getContainerId());
                                // old secondary key
                                Row oldSecondaryKeyRow = getDefinition()
                                        .getIndexRow(skey, oldTableRow);
                                // New secondary key
                                Row secondaryKeyRow = getDefinition()
                                        .getIndexRow(skey, tableRow);
                                if (!oldSecondaryKeyRow.equals(secondaryKeyRow)) {
                                    // Delete old key
                                    secondaryIndex.delete(trx,
                                            oldSecondaryKeyRow, location);
                                    // Insert new key
                                    secondaryIndex.insert(trx, secondaryKeyRow,
                                            location);
                                }
                            }
                        }
                    } finally {
                        //						indexScan.fetchCompleted(matched);
                    }
                }
            } finally {
                indexScan.close();
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
            TupleContainer table = database.getServer().getTupleContainer(trx,
                    getDefinition().getContainerId());

            IndexDefinition pkey = getDefinition().getIndexes().get(0);
            // New primary key
            Row primaryKeyRow = getDefinition().getIndexRow(pkey, tableRow);

            IndexContainer primaryIndex = database.getServer().getIndex(trx,
                    pkey.getContainerId());

            // Start a scan, with the primary key as argument
            IndexScan indexScan = primaryIndex.openScan(trx, primaryKeyRow,
                    null, true);
            try {
                if (indexScan.fetchNext()) {
                    // Scan always return item >= search key, so let's
                    // check if we had an exact match
                    boolean matched = indexScan.getCurrentKey().equals(
                            primaryKeyRow);
                    try {
                        if (matched) {
                            // Get location of the tuple
                            Location location = indexScan.getCurrentLocation();
                            // We need the old row data to be able to delete
                            // indexes
                            // fetch tuple data
                            byte[] data = table.read(location);
                            // parse the data
                            ByteBuffer bb = ByteBuffer.wrap(data);
                            Row oldTableRow = getDefinition().getRow(bb);
                            // oldTableRow.retrieve(bb);
                            // Okay, now update the table row
                            table.delete(trx, location);
                            // Update secondary indexes
                            // Old secondary key
                            for (int i = 1; i < getDefinition().getIndexes()
                                    .size(); i++) {
                                IndexDefinition skey = getDefinition()
                                        .getIndexes().get(i);
                                IndexContainer secondaryIndex = database
                                        .getServer().getIndex(trx,
                                                skey.getContainerId());
                                // old secondary key
                                Row oldSecondaryKeyRow = getDefinition()
                                        .getIndexRow(skey, oldTableRow);
                                // Delete old key
                                secondaryIndex.delete(trx, oldSecondaryKeyRow,
                                        location);
                            }
                            primaryIndex.delete(trx, primaryKeyRow, location);
                        }
                    } finally {
                        //						indexScan.fetchCompleted(matched);
                    }
                }
            } finally {
                indexScan.close();
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
        return new TableScanImpl(po, trx, this, indexno, startRow, forUpdate);
    }

    public TableDefinition getDefinition() {
        return definition;
    }

    public Row getIndexRow(int indexNo, Row tableRow) {
        return definition.getIndexRow(indexNo, tableRow);
    }

    public Row getRow() {
        return definition.getRow();
    }
}
