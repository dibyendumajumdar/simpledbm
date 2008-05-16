package org.simpledbm.database.api;

import java.util.ArrayList;

import org.simpledbm.database.IndexDefinition;
import org.simpledbm.rss.api.st.Storable;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.TypeDescriptor;

public interface TableDefinition extends Storable {

	public abstract void addIndex(int containerId, String name, int[] columns,
			boolean primary, boolean unique);

	public abstract Database getDatabase();

	public abstract int getContainerId();

	public abstract String getName();

	public abstract TypeDescriptor[] getRowType();

	public abstract ArrayList<IndexDefinition> getIndexes();

	public abstract Row getRow();

	public abstract Row getIndexRow(IndexDefinition index, Row tableRow);

}