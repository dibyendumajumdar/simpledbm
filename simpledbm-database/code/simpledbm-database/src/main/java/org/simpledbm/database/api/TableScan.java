package org.simpledbm.database.api;

import org.simpledbm.typesystem.api.Row;

/**
 * A TableScan is an Iterator that allows clients to iterate through the
 * contents of a Table. The iteraion is always ordered through an Index.
 * The Transaction managing the iteration defines the Lock Isolation level.
 * 
 * @author dibyendumajumdar
 */
public interface TableScan {

	/**
	 * Fetches the next row from the Table. The row to be fetched depends
	 * upon the current position of the scan, and the Index ordering of 
	 * the scan.
	 * @return A boolean value indicating success of EOF
	 */
	public abstract boolean fetchNext();

	/**
	 * Returns the data for the current Row.
	 * @return Row
	 */
	public abstract Row getCurrentRow();

	/**
	 * Returns the keys for the current Index Row.
	 * @return Row
	 */
	public abstract Row getCurrentIndexRow();

	/**
	 * Notifies the scan that the fetch has been completed and locks may be
	 * released (depending upon the Isolation level).
	 * @param matched A boolean value that should be true if the row is part of the search criteria match result. If set to false, this indicates that no further fetches are required.
	 */
	public abstract void fetchCompleted(boolean matched);

	/**
	 * Closes the scan, releasing locks and other resources acquired by the scan.
	 */
	public abstract void close();

	/**
	 * Updates the current row. 
	 * @param tableRow Row to be updated.
	 */
	public abstract void updateCurrentRow(Row tableRow);

	/**
	 * Deletes the current row.
	 */
	public abstract void deleteRow();

}