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
package org.simpledbm.network.client.api;

import org.simpledbm.typesystem.api.Row;

/**
 * A Table represents a collection of related containers, one of which is a Data
 * Container, and the others, Index Containers. The Data Container hold rows of
 * table data, and the Index Containers provide access paths to the table rows.
 * At least one index must be created because the database uses the index to
 * manage the primary key and lock isolation modes.
 * 
 * @author Dibyendu Majumdar
 */
public interface Table {

    /**
     * Starts a new Table Scan which allows the client to iterate through
     * the table's rows.
     * 
     * @param indexno The index to be used; first index is 0, second 1, etc.
     * @param startRow The search key - a suitable initialized table row.
     *                 Only columns used in the index are relevant.
     *                 This parameter can be set to null if the scan 
     *                 should start from the first available row
     * @param forUpdate A boolean flag to indicate whether the client
     *                  intends to update rows, in which case this parameter
     *                  should be set to true. If set, rows will be 
     *                  locked in UPDATE mode to allow subsequent updates.
     * @return A TableScan object
     */
    public TableScan openScan(int indexno, Row startRow, boolean forUpdate);

    /**
     * Obtains an empty row, in which all columns are set to NULL.
     */
    public Row getRow();

    /**
     * Adds the given row to the table. The add operation may fail if another
     * row with the same primary key already exists.
     * 
     * @param row Row to be added
     */
    public void addRow(Row row);

}
