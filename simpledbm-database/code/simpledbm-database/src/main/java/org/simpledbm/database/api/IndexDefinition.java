package org.simpledbm.database.api;

import org.simpledbm.rss.api.st.Storable;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.TypeDescriptor;

public interface IndexDefinition extends Storable {

	public abstract TableDefinition getTable();

	public abstract int getContainerId();

	public abstract String getName();

	public abstract int[] getColumns();

	public abstract TypeDescriptor[] getRowType();

	public abstract boolean isPrimary();

	public abstract boolean isUnique();

	public abstract Row getRow();

}