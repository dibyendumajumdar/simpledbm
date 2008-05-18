package org.simpledbm.database.api;

import org.simpledbm.rss.api.st.Storable;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.TypeDescriptor;

/**
 * The IndexDefinition type holds information about a single
 * index on a table. Columns are identified by their position
 * in the table row (position starts from 0).
 * 
 * @author dibyendumajumdar
 */
public interface IndexDefinition extends Storable {

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
	 * Returns the columns, dentified by their positions, that are
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
	 * or now.
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