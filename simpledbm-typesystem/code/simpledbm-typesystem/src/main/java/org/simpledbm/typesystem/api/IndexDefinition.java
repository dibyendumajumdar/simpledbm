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

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.util.Dumpable;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.TypeDescriptor;

/**
 * The IndexDefinition type holds information about a single
 * index on a table. Columns are identified by their position
 * in the table row (column position starts from 0).
 * <p>
 * IndexDefinition objects are indirectly created by calling
 * {@link TableDefinition#addIndex(int, String, int[], boolean, boolean) TableDefinition.addIndex()}.
 * <p>
 * The list of IndexDefinitions associated with a Table can be
 * obtained using {@link TableDefinition#getIndexes()} 
 * 
 * @author dibyendumajumdar
 */
public interface IndexDefinition extends Storable, Dumpable {

	/**
	 * Returns the TableDefinition for the table associated with this index.
	 * @return TableDefinition
	 */
	public abstract TableDefinition getTable();

	/**
	 * Returns the container ID associated with this Index.
	 * @return Container ID
	 */
	public abstract int getContainerId();

	/**
	 * Returns the name of the index container.
	 * @return Index container name
	 */
	public abstract String getName();

	/**
	 * Returns the columns, identified by their positions, that are
	 * part of this index. For instance, if the array contains [0,2,5],
	 * it means that the columns [0], [2] and [5] in the table row are
	 * indexed.
	 * @return Array of column positions
	 */
	public abstract int[] getColumns();

	/**
	 * Returns a row type descriptor for generating rows for the
	 * Index.
	 * @return Row Type Descriptor array
	 */
	public abstract TypeDescriptor[] getRowType();

	/**
	 * Returns a boolean value indicating whether this is the primary index
	 * or not.
	 * @return Boolean value
	 */
	public abstract boolean isPrimary();

	/**
	 * Returns a boolean value indicating whether this is a unique index. 
	 * @return Boolean value.
	 */
	public abstract boolean isUnique();

	/**
	 * Generates a new Row instance for this index.
	 * @return Row instance.
	 */
	public abstract Row getRow();

}