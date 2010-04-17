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
