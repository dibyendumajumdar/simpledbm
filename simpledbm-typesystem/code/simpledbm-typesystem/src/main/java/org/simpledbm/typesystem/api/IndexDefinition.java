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

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.Dumpable;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.TypeDescriptor;

/**
 * The IndexDefinition type holds information about a single index on a table.
 * Columns are identified by their position in the table row (column position
 * starts from 0).
 * <p>
 * IndexDefinition objects are indirectly created by calling
 * {@link TableDefinition#addIndex(int, String, int[], boolean, boolean)
 * TableDefinition.addIndex()}.
 * <p>
 * The list of IndexDefinitions associated with a Table can be obtained using
 * {@link TableDefinition#getIndexes()}
 * 
 * @author dibyendumajumdar
 */
public interface IndexDefinition extends Storable, Dumpable {

    /**
     * Returns the TableDefinition for the table associated with this index.
     * 
     * @return TableDefinition
     */
    public abstract TableDefinition getTable();

    /**
     * Returns the container ID associated with this Index.
     * 
     * @return Container ID
     */
    public abstract int getContainerId();

    /**
     * Returns the name of the index container.
     * 
     * @return Index container name
     */
    public abstract String getName();

    /**
     * Returns the columns, identified by their positions, that are part of this
     * index. For instance, if the array contains [0,2,5], it means that the
     * columns [0], [2] and [5] in the table row are indexed.
     * 
     * @return Array of column positions
     */
    public abstract int[] getColumns();

    /**
     * Returns a row type descriptor for generating rows for the Index.
     * 
     * @return Row Type Descriptor array
     */
    public abstract TypeDescriptor[] getRowType();

    /**
     * Returns a boolean value indicating whether this is the primary index or
     * not.
     * 
     * @return Boolean value
     */
    public abstract boolean isPrimary();

    /**
     * Returns a boolean value indicating whether this is a unique index.
     * 
     * @return Boolean value.
     */
    public abstract boolean isUnique();

    /**
     * Generates a new Row instance for this index.
     * 
     * @return Row instance.
     */
    public abstract Row getRow();

}