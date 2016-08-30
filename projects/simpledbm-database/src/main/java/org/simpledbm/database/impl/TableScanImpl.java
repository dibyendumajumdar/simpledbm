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
package org.simpledbm.database.impl;

import static org.simpledbm.database.impl.DatabaseImpl.m_ED0014;
import static org.simpledbm.database.impl.DatabaseImpl.m_ED0015;

import java.nio.ByteBuffer;

import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.database.api.Table;
import org.simpledbm.database.api.TableScan;
import org.simpledbm.exception.DatabaseException;
import org.simpledbm.rss.api.im.IndexContainer;
import org.simpledbm.rss.api.im.IndexScan;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.tuple.TupleContainer;
import org.simpledbm.rss.api.tx.Savepoint;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.typesystem.api.IndexDefinition;
import org.simpledbm.typesystem.api.Row;

public class TableScanImpl implements TableScan {

    final Logger log;

    private final Table table;
    final IndexScan indexScan;
    final Row startRow;
    final TupleContainer tcont;
    final IndexContainer icont;
    final Transaction trx;
    Row currentRow;

    TableScanImpl(PlatformObjects po, Transaction trx, Table table,
            int indexNo, Row tableRow, boolean forUpdate) {
        this.log = po.getLogger();
        this.table = table;
        this.trx = trx;
        tcont = table.getDatabase().getServer().getTupleContainer(trx,
                table.getDefinition().getContainerId());
        IndexDefinition index = table.getDefinition().getIndex(indexNo);
        icont = table.getDatabase().getServer().getIndex(trx,
                index.getContainerId());
        if (tableRow == null) {
            /*
             * Create a start row that begins at negative infinity
             */
            this.startRow = table.getDefinition().getIndex(indexNo).getRow();
            for (int i = 0; i < startRow.getNumberOfColumns(); i++) {
                startRow.setNegativeInfinity(i);
            }
        } else {
            this.startRow = table.getDefinition().getIndexRow(index, tableRow);
        }
        indexScan = icont.openScan(trx, startRow, null, forUpdate);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.database.TableScan#fetchNext()
     */
    public boolean fetchNext() {
        boolean okay = indexScan.fetchNext();
        if (okay) {
            Location location = indexScan.getCurrentLocation();
            // fetch tuple data
            byte[] data = tcont.read(location);
            // parse the data
            ByteBuffer bb = ByteBuffer.wrap(data);
            currentRow = getTable().getDefinition().getRow(bb);
        }
        return okay;
    }

    /* (non-Javadoc)
     * @see org.simpledbm.database.TableScan#getCurrentRow()
     */
    public Row getCurrentRow() {
        return currentRow.cloneMe();
    }

    /* (non-Javadoc)
     * @see org.simpledbm.database.TableScan#getCurrentIndexRow()
     */
    public Row getCurrentIndexRow() {
        return ((Row) indexScan.getCurrentKey()).cloneMe();
    }

    /* (non-Javadoc)
     * @see org.simpledbm.database.TableScan#fetchCompleted(boolean)
     */
    public void fetchCompleted(boolean matched) {
        //        indexScan.fetchCompleted(matched);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.database.TableScan#close()
     */
    public void close() {
        indexScan.close();
    }

    /* (non-Javadoc)
     * @see org.simpledbm.database.TableScan#updateCurrentRow(org.simpledbm.typesystem.api.Row)
     */
    public void updateCurrentRow(Row tableRow) {
        if (!getTable().validateRow(tableRow)) {
            log.error(getClass(), "updateCurrentRow",
                    new MessageInstance(m_ED0015, tableRow).toString());
            throw new DatabaseException(new MessageInstance(m_ED0015, tableRow));
        }
        Savepoint sp = trx.createSavepoint(false);
        boolean success = false;
        try {
            IndexDefinition pkey = getTable().getDefinition().getIndexes().get(
                    0);
            // New secondary key
            Row newPrimaryKeyRow = getTable().getDefinition().getIndexRow(pkey,
                    tableRow);
            if (indexScan.getCurrentKey().equals(newPrimaryKeyRow)) {
                // Get location of the tuple
                Location location = indexScan.getCurrentLocation();
                // We need the old row data to be able to delete indexes
                // fetch tuple data
                byte[] data = tcont.read(location);
                // parse the data
                ByteBuffer bb = ByteBuffer.wrap(data);
                Row oldTableRow = getTable().getDefinition().getRow(bb);
                // Okay, now update the table row
                tcont.update(trx, location, tableRow);
                // Update secondary indexes
                // Old secondary key
                for (int i = 1; i < getTable().getDefinition().getIndexes()
                        .size(); i++) {
                    IndexDefinition skey = getTable().getDefinition()
                            .getIndexes().get(i);
                    IndexContainer secondaryIndex = getTable().getDatabase()
                            .getServer().getIndex(trx, skey.getContainerId());
                    // old secondary key
                    Row oldSecondaryKeyRow = getTable().getDefinition()
                            .getIndexRow(skey, oldTableRow);
                    // New secondary key
                    Row secondaryKeyRow = getTable().getDefinition()
                            .getIndexRow(skey, tableRow);
                    if (!oldSecondaryKeyRow.equals(secondaryKeyRow)) {
                        // Delete old key
                        secondaryIndex
                                .delete(trx, oldSecondaryKeyRow, location);
                        // Insert new key
                        secondaryIndex.insert(trx, secondaryKeyRow, location);
                    }
                }
            } else {
                // getTable().addRow(trx, tableRow);
                // Can't add as we do not know that this was intended
                // FIXME use exceptionHandler
                log.error(getClass(), "updateCurrentRow",
                        new MessageInstance(m_ED0014).toString());
                throw new DatabaseException(new MessageInstance(m_ED0014));
            }
            success = true;
        } finally {
            if (!success) {
                trx.rollback(sp);
            }
        }
    }

    /* (non-Javadoc)
     * @see org.simpledbm.database.TableScan#deleteRow()
     */
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
            Row oldTableRow = getTable().getDefinition().getRow(bb);
            // Okay, now update the table row
            tcont.delete(trx, location);
            // Update indexes
            for (int i = getTable().getDefinition().getIndexes().size() - 1; i >= 0; i--) {
                IndexDefinition skey = getTable().getDefinition().getIndexes()
                        .get(i);
                IndexContainer index = getTable().getDatabase().getServer()
                        .getIndex(trx, skey.getContainerId());
                // old secondary key
                Row indexRow = getTable().getDefinition().getIndexRow(skey,
                        oldTableRow);
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

    public Table getTable() {
        return table;
    }
}
