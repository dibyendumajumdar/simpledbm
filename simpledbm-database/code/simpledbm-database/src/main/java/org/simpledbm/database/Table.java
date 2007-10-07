package org.simpledbm.database;

import java.util.ArrayList;

import org.simpledbm.rss.api.im.IndexContainer;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.tuple.TupleContainer;
import org.simpledbm.rss.api.tuple.TupleInserter;
import org.simpledbm.rss.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.typesystem.api.Field;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class Table {

	Database database;

	int containerId;

	String name;

	TypeDescriptor[] rowType;

	ArrayList<Index> indexes = new ArrayList<Index>();

	public Table(Database database, int containerId, String name,
			TypeDescriptor[] rowType) {
		this.database = database;
		this.containerId = containerId;
		this.name = name;
		this.rowType = rowType;

		database.getRowFactory().registerRowType(containerId, rowType);
		database.tables.add(this);
	}

	public void addIndex(int containerId, String name, int[] columns,
			boolean primary, boolean unique) {
		new Index(this, containerId, name, columns, primary, unique);
	}

	public Row getRow() {
		RowFactory rowFactory = database.getRowFactory();
		return rowFactory.newRow(containerId);
	}

	public Location addRow(Row tableRow) {

		Location location = null;
		// Start a new transaction
		Transaction trx = database.server.begin(IsolationMode.READ_COMMITTED);
		boolean success = false;
		try {
			TupleContainer table = database.server.getTupleContainer(trx,
					containerId);

			// First lets create a new row and lock the location
			TupleInserter inserter = table.insert(trx, tableRow);
			// Insert the primary key - may fail with unique constraint
			// violation

			for (Index idx : indexes) {
				IndexContainer index = database.server.getIndex(trx,
						idx.containerId);
				Row indexRow = idx.getRow();
				for (int i = 0; i < idx.columns.length; i++) {
					indexRow.set(i, (Field) tableRow.get(idx.columns[i])
							.cloneMe());
				}
				index.insert(trx, indexRow, inserter.getLocation());
			}
			inserter.completeInsert();
			location = inserter.getLocation();
			success = true;
		} finally {
			if (success) {
				trx.commit();
			} else {
				trx.abort();
			}
		}
		return location;
	}

	public void updateRow(Location location, Row newRow) {

	}

	public void deleteRow(Location location) {

	}

	public Database getDatabase() {
		return database;
	}

	public int getContainerId() {
		return containerId;
	}

	public String getName() {
		return name;
	}

	public TypeDescriptor[] getRowType() {
		return rowType;
	}

	public ArrayList<Index> getIndexes() {
		return indexes;
	}

}
