package org.simpledbm.database;

import java.util.ArrayList;

import org.simpledbm.rss.api.im.IndexContainer;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.tuple.TupleContainer;
import org.simpledbm.rss.api.tuple.TupleInserter;
import org.simpledbm.rss.api.tx.Savepoint;
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

	public Location addRow(Transaction trx, Row tableRow) {

		Location location = null;
		// Create a savepoint so that we can rollback to a consistent
		// state in case there is a problem.
		Savepoint savepoint = trx.createSavepoint(false);
		boolean success = false;
		try {
			// Get a handle to the table container.
			// Note - will be locked in shared mode.
			TupleContainer table = database.server.getTupleContainer(trx,
					containerId);

			// Lets create the new row and lock the location
			TupleInserter inserter = table.insert(trx, tableRow);

			// Insert the keys. The first key should be the primary key.
			// Insertion of primary key may fail with unique constraint
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
			// All keys inserted successfully, so now complete the insert
			inserter.completeInsert();
			location = inserter.getLocation();
			success = true;
		} finally {
			if (!success) {
				// Oops - need to rollback to the savepoint
				trx.rollback(savepoint);
			}
		}
		return location;
	}

	public void updateRow(Transaction trx, Row newRow) {

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
