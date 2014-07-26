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

import org.simpledbm.typesystem.api.Row;

/**
 * A TableScan is an Iterator that allows clients to iterate through the
 * contents of a Table. The iteraion is always ordered through an Index. The
 * Transaction managing the iteration defines the Lock Isolation level.
 * 
 * @author dibyendumajumdar
 */
public interface TableScan {

    /**
     * Fetches the next row from the Table. The row to be fetched depends upon
     * the current position of the scan, and the Index ordering of the scan.
     * 
     * @return A boolean value indicating success of EOF
     */
    public abstract boolean fetchNext();

    /**
     * Returns the data for the current Row.
     * 
     * @return Row
     */
    public abstract Row getCurrentRow();

    /**
     * Returns the keys for the current Index Row.
     * 
     * @return Row
     */
    public abstract Row getCurrentIndexRow();

    /**
     * Notifies the scan that the fetch has been completed and locks may be
     * released (depending upon the Isolation level).
     * 
     * @param matched A boolean value that should be true if the row is part of
     *            the search criteria match result. If set to false, this
     *            indicates that no further fetches are required.
     * @deprecated
     */
    public abstract void fetchCompleted(boolean matched);

    /**
     * Closes the scan, releasing locks and other resources acquired by the
     * scan.
     */
    public abstract void close();

    /**
     * Updates the current row.
     * 
     * @param tableRow Row to be updated.
     */
    public abstract void updateCurrentRow(Row tableRow);

    /**
     * Deletes the current row.
     */
    public abstract void deleteRow();

    /**
     * Returns the table associated with the scan.
     */
    public Table getTable();
}