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
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import org.simpledbm.database.api.Database;
import org.simpledbm.database.api.DatabaseFactory;
import org.simpledbm.database.api.Table;
import org.simpledbm.database.api.TableDefinition;
import org.simpledbm.database.api.TableScan;
import org.simpledbm.rss.api.locking.LockDeadlockException;
import org.simpledbm.rss.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;

public class TestDatabase extends BaseTestCase {

	public TestDatabase() {
		super();
	}

	public TestDatabase(String name) {
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
		properties.setProperty("logging.properties.file",
				"classpath:simpledbm.logging.properties");

		return properties;
	}
	
	static Properties getLargeServerProperties() {
		Properties properties = new Properties();
		properties.setProperty("log.ctl.1", "ctl.a");
		properties.setProperty("log.ctl.2", "ctl.b");
		properties.setProperty("log.groups.1.path", ".");
		properties.setProperty("log.archive.path", ".");
		properties.setProperty("log.group.files", "3");
		properties.setProperty("log.file.size", "5242880");
		properties.setProperty("log.buffer.size", "5242880");
		properties.setProperty("log.buffer.limit", "4");
		properties.setProperty("log.flush.interval", "30");
		properties.setProperty("log.disableFlushRequests", "true");
		properties.setProperty("storage.basePath", "testdata/DatabaseTests");
		properties.setProperty("bufferpool.numbuffers", "350");
		properties.setProperty("bufferpool.writerSleepInterval", "60000");
		properties.setProperty("transaction.ckpt.interval", "60000");
		properties.setProperty("logging.properties.type", "log4j");
		properties.setProperty("logging.properties.file",
				"classpath:simpledbm.logging.properties");
		properties.setProperty("lock.deadlock.detection.interval", "3");
		return properties;
	}

	static Date getDOB(int year, int month, int day) {
		Calendar c = Calendar.getInstance();
		c.clear();
		c.set(year, month-1, day);
		return c.getTime();
	}

	static Date getDOB(int year, int month, int day, int add) {
		Calendar c = Calendar.getInstance();
		c.clear();
		c.set(year, month-1, day);
		c.add(Calendar.DATE, add);
		return c.getTime();
	}
	
	public void testBasicFunctions() throws Exception {

		createTestDatabase(getServerProperties());

		Database db = DatabaseFactory.getDatabase(getServerProperties());
		db.start();
		try {
			TableDefinition tableDefinition = db.getTableDefinition(1);
			assertNotNull(tableDefinition);
		} finally {
			db.shutdown();
		}

		// Lets add some data
		db = DatabaseFactory.getDatabase(getServerProperties());
		db.start();
		try {
			Transaction trx = db.startTransaction(IsolationMode.READ_COMMITTED);
			boolean okay = false;
			try {
				Table table = db.getTable(trx, 1);
				assertNotNull(table);
				Row tableRow = table.getRow();
				tableRow.getColumnValue(0).setInt(1);
				tableRow.getColumnValue(1).setString("Joe");
				tableRow.getColumnValue(2).setString("Blogg");
				tableRow.getColumnValue(5).setDate(getDOB(1930, 12, 31));
				tableRow.getColumnValue(6).setString("500.00");

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

		db = DatabaseFactory.getDatabase(getServerProperties());
		db.start();
		try {
			Transaction trx = db.startTransaction(IsolationMode.READ_COMMITTED);
			boolean okay = false;
			try {
				Table table = db.getTable(trx, 1);
				assertNotNull(table);
				Row tableRow = table.getRow();
				tableRow.getColumnValue(2).setString("Blogg");
				TableScan scan = table.openScan(trx, 1, tableRow, true);
				try {
					if (scan.fetchNext()) {
						Row currentRow = scan.getCurrentRow();
						Row tr = table.getRow();
						tr.getColumnValue(0).setInt(1);
						tr.getColumnValue(1).setString("Joe");
						tr.getColumnValue(2).setString("Blogg");
						tr.getColumnValue(5).setDate(getDOB(1930, 12, 31));
						tr.getColumnValue(6).setString("500.00");
						
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

	static class EmpRecord {
		int Id;
		String name;
		String surname;
		String city;
		String email;
		Date dob;
		String salary = "0.0";
		
		public EmpRecord(int id, String name, String surname, String city, Date dob, String email,
				String salary) {
			super();
			Id = id;
			this.city = city;
			this.dob = dob;
			this.email = email;
			this.name = name;
			this.salary = salary;
			this.surname = surname;
		}
	}

	private void createTestDatabase(Properties properties) {
		deleteRecursively("testdata/DatabaseTests");
		DatabaseFactory.create(properties);

		Database db = DatabaseFactory.getDatabase(properties);
		db.start();
		try {
			TypeFactory ff = db.getTypeFactory();
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
	}
	
	private void createTestData(int startno, int range, boolean doCommit) {
		// Lets add some data
		Database db = DatabaseFactory.getDatabase(getServerProperties());
		db.start();
		try {
			boolean okay = false;
			Transaction trx = db.startTransaction(IsolationMode.READ_COMMITTED);
			try {
				Table table = db.getTable(trx, 1);
				assertNotNull(table);
				for (int i = startno; i < (startno + range); i++) {
					Row tableRow = table.getRow();
					tableRow.getColumnValue(0).setInt(i);
					tableRow.getColumnValue(1).setString("Joe" + i);
					tableRow.getColumnValue(2).setString("Blogg" + i);
					tableRow.getColumnValue(5).setDate(getDOB(1930, 12, 31));
					tableRow.getColumnValue(6).setInt(1000 + i);
					System.out.println("Adding " + tableRow);
					table.addRow(trx, tableRow);
				}
				okay = true;
			} finally {
				if (doCommit) {
					if (okay) {
						trx.commit();
					} else {
						trx.abort();
					}
				}
			}
		} finally {
			db.shutdown();
		}
	}

	private void listData(int startno, int range) {
		Database db = DatabaseFactory.getDatabase(getServerProperties());
		db.start();
		try {
			Transaction trx = db.startTransaction(IsolationMode.READ_COMMITTED);
			boolean okay = false;
			try {
				Table table = db.getTable(trx, 1);
				assertNotNull(table);
				Row tableRow = table.getRow();
				tableRow.getColumnValue(2).setString("Blogg");
				TableScan scan = table.openScan(trx, 0, tableRow, false);
				try {
					int i = startno;
					while (scan.fetchNext() && i < (startno + range)) {
						Row currentRow = scan.getCurrentRow();
						Row tr = table.getRow();
						tr.getColumnValue(0).setInt(i);
						tr.getColumnValue(1).setString("Joe" + i);
						tr.getColumnValue(2).setString("Blogg" + i);
						tr.getColumnValue(5).setDate(getDOB(1930, 12, 31));
						tr.getColumnValue(6).setInt(1000 + i);
						assertEquals(tr, currentRow);
						scan.fetchCompleted(true);
						i++;
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

	private void dumpData() {
		Database db = DatabaseFactory.getDatabase(getServerProperties());
		db.start();
		try {
			Transaction trx = db.startTransaction(IsolationMode.READ_COMMITTED);
			boolean okay = false;
			try {
				Table table = db.getTable(trx, 1);
				TableScan scan = table.openScan(trx, 0, null, false);
				try {
					while (scan.fetchNext()) {
						Row currentRow = scan.getCurrentRow();
						System.out.println("Row = " + currentRow);
						scan.fetchCompleted(true);
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
	
	
	private int countData() {
		int i = 0;
		Database db = DatabaseFactory.getDatabase(getServerProperties());
		db.start();
		try {
			Transaction trx = db.startTransaction(IsolationMode.READ_COMMITTED);
			boolean okay = false;
			try {
				Table table = db.getTable(trx, 1);
				assertNotNull(table);
				TableScan scan = table.openScan(trx, 0, null, false);
				try {
					while (scan.fetchNext()) {
						scan.fetchCompleted(true);
						i++;
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
		return i;
	}	
	
	private void updateData() {
		/*
		 * Update all rows - city will be set to London.
		 */
		Database db = DatabaseFactory.getDatabase(getServerProperties());
		db.start();
		try {
			Transaction trx = db.startTransaction(IsolationMode.READ_COMMITTED);
			boolean okay = false;
			try {
				Table table = db.getTable(trx, 1);
				assertNotNull(table);
				/* start an update mode scan */
				TableScan scan = table.openScan(trx, 0, null, true);
				try {
					while (scan.fetchNext()) {
						Row tr = scan.getCurrentRow();
						tr.getColumnValue(3).setString("London");
						tr.getColumnValue(4).setString(tr.getColumnValue(1).getString() + "." + tr.getColumnValue(2).getString() + "@gmail.com");
						tr.getColumnValue(6).setInt(50000);
						scan.updateCurrentRow(tr);
						scan.fetchCompleted(true);
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

	private int deleteData() {
		/*
		 * Update all rows - city will be set to London.
		 */
		int deleteCount = 0;
		Database db = DatabaseFactory.getDatabase(getServerProperties());
		db.start();
		try {
			Transaction trx = db.startTransaction(IsolationMode.READ_COMMITTED);
			boolean okay = false;
			try {
				Table table = db.getTable(trx, 1);
				assertNotNull(table);
				/* start an update mode scan */
				TableScan scan = table.openScan(trx, 0, null, true);
				int i = 0;
				try {
					while (scan.fetchNext()) {
						if (i % 2 == 0) {
							scan.deleteRow();
							deleteCount++;
						}
						scan.fetchCompleted(true);
						i++;
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
		return deleteCount;
	}
	
	
	public void testRecovery() {
		createTestDatabase(getServerProperties());
		createTestData(1, 100, true);
		assertEquals(100, countData());	
		createTestData(101, 100, false);
		assertEquals(100, countData());	
		createTestData(101, 100, true);
		assertEquals(200, countData());	
	}
	
	public void testUpdates() {
		createTestDatabase(getServerProperties());
		createTestData(1, 100, true);
		assertEquals(100, countData());	
		updateData();
		assertEquals(100, countData());	
		dumpData();
		int deleteCount = deleteData();
		assertEquals(100-deleteCount, countData());	
	}
	
	static class TesterThread implements Runnable {

		Database db;
		int startno;
		int range;
		int iterations;
		BaseTestCase testCase;
		
		TesterThread(BaseTestCase testCase, Database db, int start, int range, int iterations) {
			this.db = db;
			this.startno = start;
			this.range = range;
			this.iterations = iterations;
			this.testCase = testCase;
		}
		
		void testInsert() {
			System.out.println(Thread.currentThread().getName() + " Starting inserts");
			for (int i = startno; i < (startno + range); i++) {
				boolean okay = false;
				while (!okay) {
					if (testCase.threadFailed()) {
						return;
					}
					Transaction trx = db
							.startTransaction(IsolationMode.READ_COMMITTED);
					try {
						Table table = db.getTable(trx, 1);
						assertNotNull(table);
						Row tableRow = table.getRow();
						tableRow.getColumnValue(0).setInt(i);
						tableRow.getColumnValue(1).setString("Joe" + i);
						tableRow.getColumnValue(2).setString("Blogg" + i);
						tableRow.getColumnValue(5).setDate(
								getDOB(1930, 12, 31, i));
						tableRow.getColumnValue(6).setInt(i);
						// System.err.println("Adding " + tableRow);
						table.addRow(trx, tableRow);
						okay = true;
					} catch (LockDeadlockException e) {
						// deadlock
						e.printStackTrace();
					} catch (RuntimeException e) {
						e.printStackTrace();
						throw e;
					} finally {
						if (okay) {
							trx.commit();
						} else {
							trx.abort();
						}
					}
				}
			}

			for (int i = startno; i < (startno + range); i++) {
				boolean okay = false;
				while (!okay) {
					if (testCase.threadFailed()) {
						return;
					}
					Transaction trx = db
							.startTransaction(IsolationMode.READ_COMMITTED);
					try {
						Table table = db.getTable(trx, 1);
						assertNotNull(table);
						Row tableRow = table.getRow();
						tableRow.getColumnValue(0).setInt(i);
						tableRow.getColumnValue(1).setString("Joe" + i);
						tableRow.getColumnValue(2).setString("Blogg" + i);
						tableRow.getColumnValue(5).setDate(
								getDOB(1930, 12, 31, i));
						tableRow.getColumnValue(6).setInt(i);

						for (int j = 0; j < table.getDefinition()
								.getNumberOfIndexes(); j++) {
							TableScan scan = table.openScan(trx, j, tableRow,
									false);
							try {
								if (scan.fetchNext()) {
									Row scanRow = scan.getCurrentRow();
									// System.err.println("Search by index " + j
									// + " found " + scanRow + ", expected " +
									// tableRow);
									assertEquals(tableRow, scanRow);
								} else {
									fail();
								}
								scan.fetchCompleted(false);
							} finally {
								scan.close();
							}
						}
						okay = true;
					} catch (LockDeadlockException e) {
						e.printStackTrace();
					} catch (RuntimeException e) {
						e.printStackTrace();
						throw e;
					} finally {
						if (okay) {
							trx.commit();
						} else {
							trx.abort();
						}
					}
				}
			}
		}
		
		void testUpdate() {
			System.out.println(Thread.currentThread().getName() + " Starting updates");
			for (int i = startno; i < (startno + range); i++) {
				boolean okay = false;
				while (!okay) {
					if (testCase.threadFailed()) {
						return;
					}
					Transaction trx = db
							.startTransaction(IsolationMode.READ_COMMITTED);
					Row tableRow = null;
					try {
						Table table = db.getTable(trx, 1);
						assertNotNull(table);
						tableRow = table.getRow();
						tableRow.getColumnValue(0).setInt(i);
						tableRow.getColumnValue(1).setString("Joe" + i);
						tableRow.getColumnValue(2).setString("Blogg" + i);
						tableRow.getColumnValue(5).setDate(
								getDOB(1930, 12, 31, i));
						tableRow.getColumnValue(6).setInt(i);

						TableScan scan = table.openScan(trx, 0, tableRow, true);
						try {
							if (scan.fetchNext()) {
								Row tr = scan.getCurrentRow();
								tr.getColumnValue(3).setString("London");
								tr.getColumnValue(4).setString(
										tr.getColumnValue(1).getString()
												+ "."
												+ tr.getColumnValue(2)
														.getString()
												+ "@gmail.com");
								tr.getColumnValue(6).setInt(-i);
								scan.updateCurrentRow(tr);
							} else {
								fail();
							}
							scan.fetchCompleted(false);
						} finally {
							scan.close();
						}
						okay = true;
					} catch (LockDeadlockException e) {
						System.err.println("Deadlock occurred while deleting " + tableRow);
						e.printStackTrace();
					} catch (RuntimeException e) {
						System.err.println("Unexpected error occurred while deleting " + tableRow);
						e.printStackTrace();
						throw e;
					} finally {
						if (okay) {
							trx.commit();
						} else {
							trx.abort();
						}
					}
				}
			}

			for (int i = startno; i < (startno + range); i++) {
				boolean okay = false;
				while (!okay) {
					if (testCase.threadFailed()) {
						return;
					}
					Transaction trx = db
							.startTransaction(IsolationMode.READ_COMMITTED);
					try {
						Table table = db.getTable(trx, 1);
						assertNotNull(table);
						Row tableRow = table.getRow();
						tableRow.getColumnValue(0).setInt(i);
						tableRow.getColumnValue(1).setString("Joe" + i);
						tableRow.getColumnValue(2).setString("Blogg" + i);
						tableRow.getColumnValue(3).setString("London");
						tableRow.getColumnValue(4).setString(
								tableRow.getColumnValue(1).getString()
										+ "."
										+ tableRow.getColumnValue(2)
												.getString() + "@gmail.com");
						tableRow.getColumnValue(5).setDate(
								getDOB(1930, 12, 31, i));
						tableRow.getColumnValue(6).setInt(-i);

						for (int j = 0; j < table.getDefinition()
								.getNumberOfIndexes(); j++) {
							TableScan scan = table.openScan(trx, j, tableRow,
									false);
							try {
								if (scan.fetchNext()) {
									Row scanRow = scan.getCurrentRow();
									// System.err.println("Search by index " + j
									// +
									// " found " + scanRow + ", expected " +
									// tableRow);
									assertEquals(tableRow, scanRow);
								} else {
									fail();
								}
								scan.fetchCompleted(false);
							} finally {
								scan.close();
							}
						}
						okay = true;
					} catch (LockDeadlockException e) {
						e.printStackTrace();
					} catch (RuntimeException e) {
						e.printStackTrace();
						throw e;
					} finally {
						if (okay) {
							trx.commit();
						} else {
							trx.abort();
						}
					}
				}
			}
		}
		
		void testDelete() {
			System.out.println(Thread.currentThread().getName()
					+ " Starting deletes");
			for (int i = startno; i < (startno + range); i++) {
				boolean okay = false;
				while (!okay) {
					if (testCase.threadFailed()) {
						return;
					}
					Transaction trx = db
							.startTransaction(IsolationMode.READ_COMMITTED);
					Row tableRow = null;
					try {
						// System.out.println("Deleting row " + i);
						Table table = db.getTable(trx, 1);
						assertNotNull(table);
						tableRow = table.getRow();
						tableRow.getColumnValue(0).setInt(i);

						TableScan scan = table.openScan(trx, 0, tableRow, true);
						try {
							if (scan.fetchNext()) {
								scan.deleteRow();
							} else {
								fail();
							}
							scan.fetchCompleted(false);
						} finally {
							scan.close();
						}
						okay = true;
					} catch (LockDeadlockException e) {
						System.err.println("Deadlock occurred while deleting "
								+ tableRow);
						e.printStackTrace();
					} catch (RuntimeException e) {
						System.err
								.println("Unexpected error occurred while deleting "
										+ tableRow);
						e.printStackTrace();
						throw e;
					} finally {
						if (okay) {
							trx.commit();
						} else {
							trx.abort();
						}
					}
				}
			}

			for (int i = startno; i < (startno + range); i++) {
				boolean okay = false;
				int deadlockcount = 0;
				while (!okay) {
					if (testCase.threadFailed()) {
						return;
					}
					Transaction trx = db
							.startTransaction(IsolationMode.READ_COMMITTED);
					try {
						Table table = db.getTable(trx, 1);
						assertNotNull(table);
						Row tableRow = table.getRow();
						tableRow.getColumnValue(0).setInt(i);
						tableRow.getColumnValue(1).setString("Joe" + i);
						tableRow.getColumnValue(2).setString("Blogg" + i);
						tableRow.getColumnValue(3).setString("London");
						tableRow.getColumnValue(4).setString(
								tableRow.getColumnValue(1).getString()
										+ "."
										+ tableRow.getColumnValue(2)
												.getString() + "@gmail.com");
						tableRow.getColumnValue(5).setDate(
								getDOB(1930, 12, 31, i));
						tableRow.getColumnValue(6).setInt(-i);

						for (int j = 0; j < table.getDefinition()
								.getNumberOfIndexes(); j++) {
							Row indexSearchRow = table.getIndexRow(j, tableRow);
							TableScan scan = table.openScan(trx, j, tableRow,
									false);
							try {
								if (scan.fetchNext()) {
									Row scanRow = scan.getCurrentRow();
									Row scannedIndexRow = scan.getCurrentIndexRow();
									// System.err.println("Search by index " + j
									// +
									// " found " + scanRow + ", expected " +
									// tableRow);
									if (scannedIndexRow.compareTo(indexSearchRow) <= 0) {
										System.err.println("=========================================");
										System.err.println("Index = " + j);
										System.err.println("Deadlockcount = " + deadlockcount);
										System.err.println("ScannedRow = " + scanRow);
										System.err.println("Search criteria = " + tableRow);
										System.err.println("=========================================");
									}
									assertTrue(scannedIndexRow.compareTo(indexSearchRow) > 0);
								} else {
								}
								scan.fetchCompleted(false);
							} finally {
								scan.close();
							}
						}
						okay = true;
					} catch (LockDeadlockException e) {
						e.printStackTrace();
						deadlockcount++;
					} catch (RuntimeException e) {
						e.printStackTrace();
						throw e;
					} finally {
						if (okay) {
							trx.commit();
						} else {
							trx.abort();
						}
					}
				}
			}
		}
		
		public void run() {
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
			}
			System.out.println(Thread.currentThread().getName() + ": start=" + startno + ", range=" + range);
			try {
				for (int i = 0; i < iterations; i++) {
					System.out.println(Thread.currentThread().getName()
							+ ": starting iteration " + i);
					testInsert();
					testUpdate();
					testDelete();
				}
			} catch (Throwable e) {
				testCase.setThreadFailed(Thread.currentThread(), e);
			}
		}
	}
	
	public void testStress() throws Exception {

		int numThreads = 3;
		int range = 10000;
		int iterations = 1;
		
		createTestDatabase(getLargeServerProperties());
		final Database db = DatabaseFactory.getDatabase(getLargeServerProperties());
		db.start();
		try {
			Thread threads[] = new Thread[numThreads];
			for (int i = 0; i < numThreads; i++) {
				threads[i] = new Thread(new TesterThread(this, db, i * range,
						range, iterations));
			}
			for (int i = 0; i < numThreads; i++) {
				threads[i].start();
			}
			for (int i = 0; i < numThreads; i++) {
				try {
					threads[i].join(600*1000);
				} catch (InterruptedException e) {
				}
			}
			for (int i = 0; i < numThreads; i++) {
				if (threads[i].isAlive()) {
					throw new Exception();
				}
			}
			checkThreadFailures();
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
