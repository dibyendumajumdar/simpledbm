package org.simpledbm.database;

import java.util.ArrayList;

import org.simpledbm.database.api.Database;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.TypeDescriptor;

public interface TableDefinition {

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