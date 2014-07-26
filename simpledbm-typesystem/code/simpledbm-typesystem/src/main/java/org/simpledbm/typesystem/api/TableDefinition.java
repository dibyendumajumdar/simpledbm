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
package org.simpledbm.typesystem.api;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.Dumpable;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;

/**
 * A TableDefinition holds information about a table, such as its name,
 * container ID, types and number of columns, etc..
 * 
 * @author dibyendumajumdar
 */
public interface TableDefinition extends Storable, Dumpable {

    /**
     * Adds an Index to the table definition. Only one primay index is allowed.
     * 
     * @param containerId Container ID for the new index.
     * @param name Name of the Index Container
     * @param columns Array of Column identifiers - columns to be indexed
     * @param primary A boolean flag indicating that this is the primary index
     *            or not
     * @param unique A boolean flag indicating whether the index should allow
     *            only unique values
     */
    public abstract void addIndex(int containerId, String name, int[] columns,
            boolean primary, boolean unique);

    /**
     * Gets the Database to which this Table is associated
     * 
     * @return Database
     */
    //	public abstract Database getDatabase();

    public RowFactory getRowFactory();

    /**
     * Gets the Container ID associated with the table.
     * 
     * @return Container ID
     */
    public abstract int getContainerId();

    /**
     * Returns the Table's container name.
     * 
     * @return Container name
     */
    public abstract String getName();

    /**
     * Returns an array of type descriptors that represent column types for a
     * row in this table.
     * 
     * @return Array of TypeDescriptor objects
     */
    public abstract TypeDescriptor[] getRowType();

    /**
     * Returns an array of IndexDefinition objects associated with the table.
     * 
     * @return ArrayList of IndexDefinition objects
     */
    public abstract ArrayList<IndexDefinition> getIndexes();

    /**
     * Returns the specified index. Index positions start at 0.
     * 
     * @param indexNo Index position
     */
    public abstract IndexDefinition getIndex(int indexNo);

    /**
     * Constructs an empty row for the table.
     * 
     * @return Row
     */
    public abstract Row getRow();

    /**
     * Constructs an empty row for the table.
     * 
     * @return Row
     */
    public abstract Row getRow(ByteBuffer bb);

    /**
     * Constructs an row for the specified Index. Appropriate columns from the
     * table are copied into the Index row.
     * 
     * @param index The Index for which the row is to be constructed
     * @param tableRow The table row
     * @return An initialized Index Row
     */
    public abstract Row getIndexRow(IndexDefinition index, Row tableRow);

    /**
     * Returns the number of indexes associated with the table.
     */
    public abstract int getNumberOfIndexes();

    /**
     * Constructs an row for the specified Index. Appropriate columns from the
     * table are copied into the Index row.
     * 
     * @param indexNo The Index for which the row is to be constructed
     * @param tableRow The table row
     * @return An initialized Index Row
     */
    public abstract Row getIndexRow(int indexNo, Row tableRow);

}