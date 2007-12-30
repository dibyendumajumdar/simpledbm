package org.simpledbm.database;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.im.IndexContainer;
import org.simpledbm.rss.api.im.IndexScan;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.tuple.TupleContainer;
import org.simpledbm.rss.api.tx.Savepoint;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.typesystem.api.Row;

public class TableScan {

	final TableDefinition table;
	
	final IndexScan indexScan;
	
	final Row startRow;
	
	final TupleContainer tcont;
	
	final IndexContainer icont;
	
	final Transaction trx;
	
	Row currentRow;
	
	TableScan(Transaction trx, TableDefinition table, int indexNo, Row startRow, boolean forUpdate) {
		this.table = table;
		this.startRow = startRow;
		this.trx = trx;
        tcont = table.database.server.getTupleContainer(trx, table.containerId);
        IndexDefinition index = table.indexes.get(indexNo);
        icont = table.database.server.getIndex(trx, index.containerId);
        indexScan = icont.openScan(trx, startRow, null, forUpdate);
	}

	public boolean fetchNext() {
		boolean okay = indexScan.fetchNext();
		if (okay) {
            Location location = indexScan.getCurrentLocation();
            // fetch tuple data
            byte[] data = tcont.read(location);
            // parse the data
            ByteBuffer bb = ByteBuffer.wrap(data);
            currentRow = table.getRow();
            currentRow.retrieve(bb);			
		}
		return okay;
	}
	
	public Row getCurrentRow() {
		return currentRow;
	}
	
	public Row getCurrentIndexRow() {
		return (Row) indexScan.getCurrentKey();
	}
	
	public void fetchCompleted(boolean matched) {
		indexScan.fetchCompleted(matched);
	}
	
	public void close() {
		indexScan.close();
	}
	
	public void updateCurrentRow(Row tableRow) {
		// Start a new transaction
		Savepoint sp = trx.createSavepoint(false);
		boolean success = false;
		try {
			IndexDefinition pkey = table.indexes.get(0);
			// New secondary key
			Row newPrimaryKeyRow = table.getIndexRow(pkey, tableRow);
			if (indexScan.getCurrentKey().equals(newPrimaryKeyRow)) {
				// Get location of the tuple
				Location location = indexScan.getCurrentLocation();
				// We need the old row data to be able to delete indexes
				// fetch tuple data
				byte[] data = tcont.read(location);
				// parse the data
				ByteBuffer bb = ByteBuffer.wrap(data);
				Row oldTableRow = table.getRow();
				oldTableRow.retrieve(bb);
				// Okay, now update the table row
				tcont.update(trx, location, tableRow);
				// Update secondary indexes
				// Old secondary key
				for (int i = 1; i < table.indexes.size(); i++) {
					IndexDefinition skey = table.indexes.get(i);
					IndexContainer secondaryIndex = table.database.server
							.getIndex(trx, skey.containerId);
					// old secondary key
					Row oldSecondaryKeyRow = table.getIndexRow(skey, tableRow);
					// New secondary key
					Row secondaryKeyRow = table.getIndexRow(skey, tableRow);
					if (!oldSecondaryKeyRow.equals(secondaryKeyRow)) {
						// Delete old key
						secondaryIndex
								.delete(trx, oldSecondaryKeyRow, location);
						// Insert new key
						secondaryIndex.insert(trx, secondaryKeyRow, location);
					}
				}
			} else {
				table.addRow(trx, tableRow);
			}
			success = true;
		} finally {
			if (!success) {
				trx.rollback(sp);
			}
		}
	}	
	
	public void deleteRow() {
		// Start a new transaction
		Savepoint sp = trx.createSavepoint(false);
		boolean success = false;
		try {
			// Get location of the tuple
			Location location = indexScan.getCurrentLocation();
			// We need the old row data to be able to delete indexes
			// fetch tuple data
			byte[] data = tcont.read(location);
			// parse the data
			ByteBuffer bb = ByteBuffer.wrap(data);
			Row oldTableRow = table.getRow();
			oldTableRow.retrieve(bb);
			// Okay, now update the table row
			tcont.delete(trx, location);
			// Update secondary indexes
			// Old secondary key
			for (int i = table.indexes.size() - 1; i >= 0; i--) {
				IndexDefinition skey = table.indexes.get(i);
				IndexContainer secondaryIndex = table.database.server.getIndex(
						trx, skey.containerId);
				// old secondary key
				Row oldSecondaryKeyRow = table.getIndexRow(skey, oldTableRow);
				// Delete old key
				secondaryIndex.delete(trx, oldSecondaryKeyRow, location);
			}
			success = true;
		} finally {
			if (!success) {
				trx.rollback(sp);
			}
		}
	}
}
