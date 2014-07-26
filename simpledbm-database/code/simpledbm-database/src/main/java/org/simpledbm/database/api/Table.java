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
package org.simpledbm.database.api;

import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.TableDefinition;

/**
 * A Table is a collection of rows. Each row is made up of columns (fields). A
 * table must have a primary key defined which uniquely identifies each row in
 * the table.
 * <p>
 * A Table is created by {@link Database#createTable(TableDefinition)
 * Database.createTable()}. Once created, the Table object can be accessed by
 * calling {@link Database#getTable(Transaction, int) Database.getTable()}
 * method.
 * 
 * @author dibyendu majumdar
 */
public interface Table {

    /**
     * Adds a row to the table. The primary key of the row must be unique and
     * different from all other rows in the table.
     * 
     * @param trx The Transaction managing this row insert
     * @param tableRow The row to be inserted
     * @return Location of the new row
     */
    public abstract Location addRow(Transaction trx, Row tableRow);

    /**
     * Updates the supplied row in the table. Note that the row to be updated is
     * identified by its primary key.
     * 
     * @param trx The Transaction managing this update
     * @param tableRow The row to be updated.
     */
    public abstract void updateRow(Transaction trx, Row tableRow);

    /**
     * Deletes the supplied row from the table. Note that the row to be deleted
     * is identified by its primary key.
     * 
     * @param trx The Transaction managing this delete
     * @param tableRow The row to be deleted.
     */
    public abstract void deleteRow(Transaction trx, Row tableRow);

    /**
     * Opens a Table Scan, which allows rows to be fetched from the Table, and
     * updated.
     * 
     * @param trx Transaction managing the scan
     * @param indexno The index to be used for the scan
     * @param startRow The starting row of the scan
     * @param forUpdate A boolean value indicating whether the scan will be used
     *            to update rows
     * @return A TableScan
     */
    public abstract TableScan openScan(Transaction trx, int indexno,
            Row startRow, boolean forUpdate);

    /**
     * Gets the table definition associated with this table.
     * 
     * @return TableDefinition
     */
    public abstract TableDefinition getDefinition();

    /**
     * Constructs an empty row for the table.
     * 
     * @return Row
     */
    public abstract Row getRow();

    /**
     * Constructs an row for the specified Index. Appropriate columns from the
     * table are copied into the Index row.
     * 
     * @param index The Index for which the row is to be constructed
     * @param tableRow The table row
     * @return An initialized Index Row
     */
    public abstract Row getIndexRow(int index, Row tableRow);

    /**
     * Check the table row for validity.
     */
    public abstract boolean validateRow(Row tableRow);

    public Database getDatabase();
}