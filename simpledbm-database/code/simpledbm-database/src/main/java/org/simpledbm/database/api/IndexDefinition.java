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
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.database.api;

import org.simpledbm.rss.api.registry.Storable;
import org.simpledbm.rss.util.Dumpable;
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