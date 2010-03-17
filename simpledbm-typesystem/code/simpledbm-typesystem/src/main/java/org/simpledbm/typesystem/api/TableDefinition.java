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