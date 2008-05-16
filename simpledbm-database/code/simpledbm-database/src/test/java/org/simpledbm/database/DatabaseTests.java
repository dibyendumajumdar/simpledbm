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

import java.io.File;
import java.math.BigInteger;
import java.util.Date;
import java.util.Properties;

import junit.framework.TestCase;

import org.simpledbm.database.api.Database;
import org.simpledbm.database.api.TableDefinition;
import org.simpledbm.rss.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.typesystem.api.FieldFactory;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class DatabaseTests extends TestCase {

	public DatabaseTests() {
		super();
	}

	public DatabaseTests(String name) {
		super(name);
	}

	static Properties getServerProperties() {
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
		properties.setProperty("storage.basePath", "testdata/DatabaseTests");
		properties.setProperty("bufferpool.numbuffers", "50");
		properties.setProperty("logging.properties.type", "log4j");
		properties.setProperty("logging.propertis.file",
				"classpath:simpledbm.logging.properties");

		return properties;
	}

	public void testBasicFunctions() throws Exception {

		deleteRecursively("testdata/DatabaseTests");
		DatabaseImpl.create(getServerProperties());

		Database db = new DatabaseImpl(getServerProperties());
		db.start();
		try {
			FieldFactory ff = db.getFieldFactory();
			TypeDescriptor employee_rowtype[] = { ff.getIntegerType(), /*
																		 * primary
																		 * key
																		 */
			ff.getVarcharType(20), /* name */
			ff.getVarcharType(20), /* surname */
			ff.getVarcharType(20), /* city */
			ff.getVarcharType(45), /* email address */
			ff.getDateTimeType(), /* date of birth */
			ff.getNumberType(2) /* salary */

			};
			TableDefinition tableDefinition = db.newTableDefinition("employee", 1,
					employee_rowtype);
			tableDefinition.addIndex(2, "employee1.idx", new int[] { 0 }, true, true);
			tableDefinition
					.addIndex(3, "employee2.idx", new int[] { 2, 1 }, false,
							false);
			tableDefinition.addIndex(4, "employee3.idx", new int[] { 5 }, false, false);
			tableDefinition.addIndex(5, "employee4.idx", new int[] { 6 }, false, false);

			db.createTable(tableDefinition);
		} finally {
			db.shutdown();
		}

		db = new DatabaseImpl(getServerProperties());
		db.start();
		try {
			TableDefinition tableDefinition = db.getTableDefinition(1);
			assertNotNull(tableDefinition);
		} finally {
			db.shutdown();
		}

		// Lets add some data
		db = new DatabaseImpl(getServerProperties());
		db.start();
		try {
			TableDefinition tableDefinition = db.getTableDefinition(1);
			assertNotNull(tableDefinition);
			TableImpl table = new TableImpl(tableDefinition);
			Row tableRow = tableDefinition.getRow();
			tableRow.get(0).setInt(1);
			tableRow.get(1).setString("Joe");
			tableRow.get(2).setString("Blogg");
			tableRow.get(5).setDate(new Date(1930, 12, 31));
			tableRow.get(6).setString("500.00");

			Transaction trx = db.getServer()
					.begin(IsolationMode.READ_COMMITTED);
			boolean okay = false;
			try {
				table.addRow(trx, tableRow);
				okay = true;
			} finally {
				if (okay) {
					trx.commit();
				} else {
					trx.abort();
				}
			}
		} finally {
			db.shutdown();
		}

		db = new DatabaseImpl(getServerProperties());
		db.start();
		try {
			TableDefinition tableDefinition = db.getTableDefinition(1);
			assertNotNull(tableDefinition);
			TableImpl table = new TableImpl(tableDefinition);
			Transaction trx = db.getServer()
					.begin(IsolationMode.READ_COMMITTED);
			boolean okay = false;
			try {
				Row tableRow = tableDefinition.getRow();
				tableRow.get(2).setString("Blogg");
				TableScan scan = table.openScan(trx, 1, tableRow, true);
				try {
					if (scan.fetchNext()) {
						Row currentRow = scan.getCurrentRow();
						Row tr = tableDefinition.getRow();
						tr.get(0).setInt(1);
						tr.get(1).setString("Joe");
						tr.get(2).setString("Blogg");
						tr.get(5).setDate(new Date(1930, 12, 31));
						tr.get(6).setString("500.00");
						assertEquals(tr, currentRow);
					}
				} finally {
					scan.close();
				}
				okay = true;
			} finally {
				if (okay) {
					trx.commit();
				} else {
					trx.abort();
				}
			}
		} finally {
			db.shutdown();
		}

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
