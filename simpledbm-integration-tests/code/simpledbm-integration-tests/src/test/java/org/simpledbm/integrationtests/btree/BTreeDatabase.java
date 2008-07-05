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
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.integrationtests.btree;

import java.io.File;
import java.util.Properties;

import org.simpledbm.rss.api.im.IndexContainer;
import org.simpledbm.rss.api.im.IndexException;
import org.simpledbm.rss.api.im.IndexScan;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.loc.LocationFactory;
import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockMode;
import org.simpledbm.rss.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.Savepoint;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.api.tx.TransactionException;
import org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl;
import org.simpledbm.rss.main.Server;
import org.simpledbm.rss.util.logging.DiagnosticLogger;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;
import org.simpledbm.typesystem.api.TypeSystemFactory;

public class BTreeDatabase {

	Server server;

	final TypeFactory fieldFactory = TypeSystemFactory.getDefaultTypeFactory();

	final RowFactory keyFactory = TypeSystemFactory.getDefaultRowFactory(fieldFactory);

	final TypeDescriptor[] rowtype1 = new TypeDescriptor[] { fieldFactory.getIntegerType() };

	IndexContainer btree;

	static int KEY_FACTORY_TYPE = 25000;

	static int LOCATION_FACTORY_TYPE = 25001;

	/**
	 * Encapsulates all the components of the BTree database.
	 * 
	 * @throws Exception
	 */
	public BTreeDatabase(boolean create) throws Exception {

		BTreeIndexManagerImpl
				.setTestingFlag(BTreeIndexManagerImpl.TEST_MODE_LIMIT_MAX_KEYS_PER_PAGE);
		// DiagnosticLogger.setDiagnosticsLevel(1);

		Properties properties = new Properties();
		properties.setProperty("log.ctl.1", "ctl.a");
		properties.setProperty("log.ctl.2", "ctl.b");
		properties.setProperty("log.groups.1.path", ".");
		properties.setProperty("log.archive.path", ".");
		properties.setProperty("log.group.files", "3");
		properties.setProperty("log.file.size", "16384");
		properties.setProperty("log.buffer.size", "16384");
		properties.setProperty("log.buffer.limit", "4");
		properties.setProperty("log.flush.interval", "5");
		properties.setProperty("storage.basePath", "demodata/BTreeDemo");

		if (create) {
			deleteRecursively("demodata/BTreeDemo");
			Server.create(properties);
		}

		server = new Server(properties);
		keyFactory.registerRowType(1, rowtype1);

		server.registerSingleton(KEY_FACTORY_TYPE, keyFactory);
		server.registerType(LOCATION_FACTORY_TYPE, RowLocationFactory.class
				.getName());

		server.start();

		if (create) {
			createBTree();
		}
	}

	/**
	 * Creates the BTree that will be used for the demo.
	 */
	public void createBTree() throws IndexException, TransactionException {
		Transaction trx = server.begin(IsolationMode.READ_COMMITTED);
		boolean success = false;
		try {
			server.createIndex(trx, "testbtree.dat", 1, 8, KEY_FACTORY_TYPE,
					LOCATION_FACTORY_TYPE, false);
		} finally {
			if (success)
				trx.commit();
			else
				trx.abort();
		}
		// To make it more interesting, lets add 200 keys
		trx = server.begin(IsolationMode.READ_COMMITTED);
		try {
			IndexContainer btree = server.getIndex(trx, 1);
			LocationFactory locationFactory = (LocationFactory) server
					.getObjectRegistry().getInstance(LOCATION_FACTORY_TYPE);
			for (int i = 1; i <= 201; i += 2) {
				Row row = (Row) keyFactory.newIndexKey(1);
				row.getColumnValue(0).setInt(i);
				RowLocation location = (RowLocation) locationFactory
						.newLocation();
				location.setInt(i);
				// Note that the row must be locked exclusively prior to the
				// insert
				trx.acquireLock(location, LockMode.EXCLUSIVE,
						LockDuration.COMMIT_DURATION);
				btree.insert(trx, row, location);
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

	/**
	 * Adds a key/location pair to the tree.
	 */
	public boolean add(Transaction trx, String key, String sloc)
			throws IndexException, TransactionException {
		boolean success = false;
		Savepoint sp = trx.createSavepoint(false);
		try {
			IndexContainer btree = server.getIndex(trx, 1);
			Row row = (Row) keyFactory.newIndexKey(1);
			row.getColumnValue(0).setString(key);
			LocationFactory locationFactory = (LocationFactory) server
					.getObjectRegistry().getInstance(LOCATION_FACTORY_TYPE);
			Location location = locationFactory.newLocation();
			location.parseString(sloc);
			// Note that the row must be locked exclusively prior to the insert
			trx.acquireLock(location, LockMode.EXCLUSIVE,
					LockDuration.COMMIT_DURATION);
			btree.insert(trx, row, location);
			success = true;
		} finally {
			if (!success) {
				trx.rollback(sp);
			}
		}
		return success;
	}

	/**
	 * Adds a key/location pair to the tree.
	 */
	public boolean delete(Transaction trx, String key, String sloc)
			throws IndexException, TransactionException {
		boolean success = false;
		Savepoint sp = trx.createSavepoint(false);
		try {
			IndexContainer btree = server.getIndex(trx, 1);
			Row row = (Row) keyFactory.newIndexKey(1);
			row.getColumnValue(0).setString(key);
			LocationFactory locationFactory = (LocationFactory) server
					.getObjectRegistry().getInstance(LOCATION_FACTORY_TYPE);
			Location location = locationFactory.newLocation();
			location.parseString(sloc);
			// Note that the row must be locked exclusively prior to the delete
			trx.acquireLock(location, LockMode.EXCLUSIVE,
					LockDuration.COMMIT_DURATION);
			btree.delete(trx, row, location);
			success = true;
		} finally {
			if (!success) {
				trx.rollback(sp);
			}
		}
		return success;
	}

	public boolean scan(Transaction trx, String key, String sloc)
			throws IndexException, TransactionException {
		boolean success = false;
		Savepoint sp = trx.createSavepoint(false);
		try {
			IndexContainer btree = server.getIndex(trx, 1);
			Row row = (Row) keyFactory.newIndexKey(1);
			row.getColumnValue(0).setString(key);
			LocationFactory locationFactory = (LocationFactory) server
					.getObjectRegistry().getInstance(LOCATION_FACTORY_TYPE);
			Location location = locationFactory.newLocation();
			location.parseString(sloc);
			IndexScan scan = btree.openScan(trx, row, location, false);
			try {
				while (scan.fetchNext()) {
					DiagnosticLogger.log("SCAN NEXT=" + scan.getCurrentKey()
							+ "," + scan.getCurrentLocation());
					System.err.println("SCAN NEXT=" + scan.getCurrentKey()
							+ "," + scan.getCurrentLocation());
					// trx.releaseLock(scan.getCurrentLocation());
					scan.fetchCompleted(true);
				}
			} finally {
				if (scan != null) {
					scan.close();
				}
			}
			success = true;
		} finally {
			if (!success) {
				trx.rollback(sp);
			}
		}
		return success;
	}

	/**
	 * Shuts down the database components.
	 */
	public void shutdown() {
		server.shutdown();
	}

	public void destroy() {
		deleteRecursively("demodata/BTreeDemo");
	}

	void deleteRecursively(String pathname) {
		File file = new File(pathname);
		deleteRecursively(file);
	}

	void deleteRecursively(File dir) {
		if (dir.isDirectory()) {
			File[] files = dir.listFiles();
			for (File file : files) {
				if (file.isDirectory()) {
					deleteRecursively(file);
				} else {
					file.delete();
				}
			}
		}
		dir.delete();
	}

}
