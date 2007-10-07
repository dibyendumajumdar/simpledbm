package org.simpledbm.database;

import org.simpledbm.rss.api.im.IndexKey;
import org.simpledbm.rss.api.im.IndexKeyFactory;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class Index {

	Table table;

	int containerId;

	String name;

	int columns[];

	TypeDescriptor[] rowType;

	boolean primary;

	boolean unique;

	public Index(Table table, int containerId, String name,
			int columns[], boolean primary, boolean unique) {
		this.table = table;
		this.containerId = containerId;
		this.name = name;
		this.columns = columns;
		this.primary = primary;
		if (primary) {
			this.unique = true;
		} else {
			this.unique = unique;
		}

		rowType = new TypeDescriptor[columns.length];
		for (int i = 0; i < columns.length; i++) {
			rowType[i] = table.getRowType()[columns[i]];
		}

		table.database.getRowFactory().registerRowType(containerId, rowType);
		table.indexes.add(this);
	}

	public Table getTable() {
		return table;
	}

	public int getContainerId() {
		return containerId;
	}

	public String getName() {
		return name;
	}

	public int[] getColumns() {
		return columns;
	}

	public TypeDescriptor[] getRowType() {
		return rowType;
	}

	public boolean isPrimary() {
		return primary;
	}

	public boolean isUnique() {
		return unique;
	}

	public Row getRow() {
		RowFactory rowFactory = table.database.getRowFactory();
		return rowFactory.newRow(containerId);
	}
	
    /**
     * Create a row with values that are less than any other row in the index.
     * 
     * @param containerId
     *            ID of the container
     * @return Appropriate row type
     */
    IndexKey makeMinRow(int containerId) {
        IndexKeyFactory rowFactory = table.database.getRowFactory();
        return rowFactory.minIndexKey(containerId);
    }
}
