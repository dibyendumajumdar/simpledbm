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