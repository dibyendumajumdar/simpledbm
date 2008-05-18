package org.simpledbm.database.api;

import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.typesystem.api.Row;

/**
 * A Table is a collection of rows. Each row is made up of columns (fields).
 * A table must have a primary key defined which uniquely identifies each row in the
 * table.
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
	 * Updates the supplied row in the table. Note that the row to be
	 * updated is identified by its primary key.
	 * 
	 * @param trx The Transaction managing this update
	 * @param tableRow The row to be updated.
	 */
	public abstract void updateRow(Transaction trx, Row tableRow);

	/**
	 * Deletes the supplied row from the table. Note that the row to be
	 * deleted is identified by its primary key.
	 * 
	 * @param trx The Transaction managing this delete
	 * @param tableRow The row to be deleted.
	 */
	public abstract void deleteRow(Transaction trx, Row tableRow);
	
	/**
	 * Opens a Table Scan, which allows rows to be fetched from the Table,
	 * and updated.
	 * 
	 * @param trx Transaction managing the scan
	 * @param indexno The index to be used for the scan
	 * @param startRow The starting row of the scan
	 * @param forUpdate A boolean value indicating whether the scan will be used to update rows
	 * @return A TableScan
	 */
	public abstract TableScan openScan(Transaction trx, int indexno,
			Row startRow, boolean forUpdate);
	
	
	/**
	 * Gets the table definition associated with this table.
	 * @return TableDefinition
	 */
	public abstract TableDefinition getDefinition();

}