package org.simpledbm.integrationtests.btree;

import java.util.ArrayList;

import org.simpledbm.rss.api.im.IndexContainer;
import org.simpledbm.rss.api.im.IndexScan;
import org.simpledbm.rss.api.loc.LocationFactory;
import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockMode;
import org.simpledbm.rss.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.typesystem.api.Row;

public class BTreeTests extends BaseTestCase {

	void insertValuesUsingMultipleTransactions(Integer[] values,
			boolean forceAbort) {
		boolean success = false;
		for (int i = 0; i < values.length; i++) {
			Transaction trx = db.server.begin(IsolationMode.READ_COMMITTED);
			try {
				IndexContainer btree = db.server.getIndex(trx, 1);
				Row row = (Row) db.keyFactory.newIndexKey(1);
				LocationFactory locationFactory = (LocationFactory) db.server
						.getObjectRegistry().getInstance(
								BTreeDatabase.LOCATION_FACTORY_TYPE);
				RowLocation location = (RowLocation) locationFactory
						.newLocation();
				row.get(0).setInt(values[i]);
				location.setInt(values[i]);
				// Note that the row must be locked exclusively prior to the
				// insert
				trx.acquireLock(location, LockMode.EXCLUSIVE,
						LockDuration.COMMIT_DURATION);
				btree.insert(trx, row, location);
				success = true;
			} finally {
				if (!success || forceAbort) {
					trx.abort();
				} else {
					trx.commit();
				}
			}
		}
	}

	void insertValuesUsingSingleTransaction(Integer[] values, boolean forceAbort) {
		boolean success = false;
		Transaction trx = db.server.begin(IsolationMode.READ_COMMITTED);
		try {
			IndexContainer btree = db.server.getIndex(trx, 1);
			LocationFactory locationFactory = (LocationFactory) db.server
					.getObjectRegistry().getInstance(
							BTreeDatabase.LOCATION_FACTORY_TYPE);
			for (int i = 0; i < values.length; i++) {
				Row row = (Row) db.keyFactory.newIndexKey(1);
				row.get(0).setInt(values[i]);
				RowLocation location = (RowLocation) locationFactory.newLocation();
				location.setInt(values[i]);
				// Note that the row must be locked exclusively prior to the
				// insert
				trx.acquireLock(location, LockMode.EXCLUSIVE,
						LockDuration.COMMIT_DURATION);
				btree.insert(trx, row, location);
			}
			success = true;
		} finally {
			if (!success || forceAbort) {
				trx.abort();
			} else {
				trx.commit();
			}
		}
	}

	void searchValues(Integer[] values, boolean expectedResult) {
		boolean success = false;
		for (int i = 0; i < values.length; i++) {
			Transaction trx = db.server.begin(IsolationMode.READ_COMMITTED);
			try {
				IndexContainer btree = db.server.getIndex(trx, 1);
				Row row = (Row) db.keyFactory.newIndexKey(1);
				row.get(0).setInt(values[i]);
				IndexScan scan = btree.openScan(trx, row, null, false);
				try {
					if (scan.fetchNext()) {
						Row indexRow = (Row) scan.getCurrentKey();
						boolean result = row.equals(indexRow);
						scan.fetchCompleted(true);
						assertEquals(expectedResult, result);
					}
				} finally {
					if (scan != null) {
						scan.close();
					}
				}
				success = true;
			} finally {
				if (!success) {
					trx.abort();
				} else {
					trx.commit();
				}
			}
		}
	}
	
	Integer[] generateValues(int start, int stop, int increment) {
		ArrayList<Integer> intArray = new ArrayList<Integer>();
		for (int i = start; i <= stop; i += increment) {
			intArray.add(i);
		}
		return intArray.toArray(new Integer[0]);
	}

	public void testInsertSingleThreadMultipleTransactionsCommit() {
		Integer[] values = generateValues(0, 200, 2);
		insertValuesUsingMultipleTransactions(values, false);
		searchValues(values, true);
	}

	public void testInsertSingleThreadMultipleTransactionsAbort() {
		Integer[] values = generateValues(0, 200, 2);
		insertValuesUsingMultipleTransactions(values, true);
		searchValues(values, false);
	}
	
	public void testInsertSingleThreadSingleTransactionCommit() {
		Integer[] values = generateValues(0, 200, 2);
		insertValuesUsingSingleTransaction(values, false);
		searchValues(values, true);
	}
	
	public void testInsertSingleThreadSingleTransactionAbort() {
		Integer[] values = generateValues(0, 200, 2);
		insertValuesUsingSingleTransaction(values, true);
		searchValues(values, false);
	}	
	
}
