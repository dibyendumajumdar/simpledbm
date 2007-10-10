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
 *    Email  : dibyendu@mazumdar.demon.co.uk
 */
package org.simpledbm.database;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.simpledbm.rss.api.im.IndexContainer;
import org.simpledbm.rss.api.im.IndexScan;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.tuple.TupleContainer;
import org.simpledbm.rss.api.tuple.TupleInserter;
import org.simpledbm.rss.api.tx.Savepoint;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.typesystem.api.Field;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;

/**
 * Encapsulates a table definition and provides methods to work with table and
 * indexes associated with the table.
 * 
 * @author dibyendumajumdar
 * @since 7 Oct 2007
 */
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
		if (primary) {
			if (indexes.size() == 0) {
				throw new IllegalArgumentException(
						"First index must be the primary");
			}
		}
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
				Row indexRow = getIndexRow(idx, tableRow);
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

	Row getIndexRow(Index index, Row tableRow) {
		Row indexRow = index.getRow();
		for (int i = 0; i < index.columns.length; i++) {
			indexRow.set(i, (Field) tableRow.get(index.columns[i]).cloneMe());
		}
		return indexRow;
	}

	public void updateRow(Transaction trx, Row tableRow) {

		// Start a new transaction
		Savepoint sp = trx.createSavepoint(false);
		boolean success = false;
		try {
			TupleContainer table = database.server.getTupleContainer(trx,
					containerId);

			Index pkey = indexes.get(0);
			// New primary key
			Row primaryKeyRow = getIndexRow(pkey, tableRow);

			IndexContainer primaryIndex = database.server.getIndex(trx,
					pkey.containerId);

			// Start a scan, with the primary key as argument
			IndexScan indexScan = primaryIndex.openScan(trx, primaryKeyRow,
					null, true);
			if (indexScan.fetchNext()) {
				// Scan always return item >= search key, so let's
				// check if we had an exact match
				boolean matched = indexScan.getCurrentKey().equals(
						primaryKeyRow);
				try {
					if (matched) {
						// Get location of the tuple
						Location location = indexScan.getCurrentLocation();
						// We need the old row data to be able to delete indexes
						// fetch tuple data
						byte[] data = table.read(location);
						// parse the data
						ByteBuffer bb = ByteBuffer.wrap(data);
						Row oldTableRow = getRow();
						oldTableRow.retrieve(bb);
						// Okay, now update the table row
						table.update(trx, location, tableRow);
						// Update secondary indexes
						// Old secondary key
						for (int i = 1; i < indexes.size(); i++) {
							Index skey = indexes.get(i);
							IndexContainer secondaryIndex = database.server
									.getIndex(trx, skey.containerId);
							// old secondary key
							Row oldSecondaryKeyRow = getIndexRow(skey, tableRow);
							// New secondary key
							Row secondaryKeyRow = getIndexRow(skey, tableRow);
							if (!oldSecondaryKeyRow.equals(secondaryKeyRow)) {
								// Delete old key
								secondaryIndex.delete(trx, oldSecondaryKeyRow,
										location);
								// Insert new key
								secondaryIndex.insert(trx, secondaryKeyRow,
										location);
							}
						}
					}
				} finally {
					indexScan.fetchCompleted(matched);
				}
			}
			success = true;
		} finally {
			if (!success) {
				trx.rollback(sp);
			}
		}
	}

	public void deleteRow(Transaction trx, Row tableRow) {

		// Start a new transaction
		Savepoint sp = trx.createSavepoint(false);
		boolean success = false;
		try {
			TupleContainer table = database.server.getTupleContainer(trx,
					containerId);

			Index pkey = indexes.get(0);
			// New primary key
			Row primaryKeyRow = getIndexRow(pkey, tableRow);

			IndexContainer primaryIndex = database.server.getIndex(trx,
					pkey.containerId);

			// Start a scan, with the primary key as argument
			IndexScan indexScan = primaryIndex.openScan(trx, primaryKeyRow,
					null, true);
			if (indexScan.fetchNext()) {
				// Scan always return item >= search key, so let's
				// check if we had an exact match
				boolean matched = indexScan.getCurrentKey().equals(
						primaryKeyRow);
				try {
					if (matched) {
						// Get location of the tuple
						Location location = indexScan.getCurrentLocation();
						// We need the old row data to be able to delete indexes
						// fetch tuple data
						byte[] data = table.read(location);
						// parse the data
						ByteBuffer bb = ByteBuffer.wrap(data);
						Row oldTableRow = getRow();
						oldTableRow.retrieve(bb);
						// Okay, now update the table row
						table.delete(trx, location);
						// Update secondary indexes
						// Old secondary key
						for (int i = 1; i < indexes.size(); i++) {
							Index skey = indexes.get(i);
							IndexContainer secondaryIndex = database.server
									.getIndex(trx, skey.containerId);
							// old secondary key
							Row oldSecondaryKeyRow = getIndexRow(skey, tableRow);
							// Delete old key
							secondaryIndex.delete(trx, oldSecondaryKeyRow,
									location);
						}
						primaryIndex.delete(trx, primaryKeyRow, location);
					}
				} finally {
					indexScan.fetchCompleted(matched);
				}
			}
			success = true;
		} finally {
			if (!success) {
				trx.rollback(sp);
			}
		}
	}

	public TableScan openScan(Transaction trx, int indexno, Row startRow) {
		return new TableScan(trx, this, indexno, startRow);
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
