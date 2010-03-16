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
 *    Linking this library statically or dynamically with other modules 
 *    is making a combined work based on this library. Thus, the terms and
 *    conditions of the GNU General Public License cover the whole
 *    combination.
 *
 *    As a special exception, the copyright holders of this library give 
 *    you permission to link this library with independent modules to 
 *    produce an executable, regardless of the license terms of these 
 *    independent modules, and to copy and distribute the resulting 
 *    executable under terms of your choice, provided that you also meet, 
 *    for each linked independent module, the terms and conditions of the 
 *    license of that module.  An independent module is a module which 
 *    is not derived from or based on this library.  If you modify this 
 *    library, you may extend this exception to your version of the 
 *    library, but you are not obligated to do so.  If you do not wish 
 *    to do so, delete this exception statement from your version.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.network.client;

import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.simpledbm.common.api.exception.SimpleDBMException;
import org.simpledbm.common.api.tx.IsolationMode;
import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.network.client.api.Session;
import org.simpledbm.network.client.api.SessionManager;
import org.simpledbm.network.client.api.Table;
import org.simpledbm.network.client.api.TableScan;
import org.simpledbm.network.server.SimpleDBMServer;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.TableDefinition;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;

public class ClientTest extends BaseTestCase {

	public ClientTest(String name) {
		super(name);
	}

	protected void setUp() throws Exception {
		super.setUp();
		SimpleDBMServer server = new SimpleDBMServer();
		server.run(new String[] { "create", "test.properties" });
	}

	protected void tearDown() throws Exception {
		super.tearDown();
	}

	static class MyServer implements Runnable {

		volatile boolean stop = false;
		SimpleDBMServer server = new SimpleDBMServer();

		MyServer() {
		}

		public void run() {
			System.err.println("starting server");
			try {
				server.run(new String[] { "open", "test.properties" });
				while (!stop) {
					server.select();
				}
				server.shutdown();
			} catch (Exception e) {
				System.err.println("failed to start server");
				e.printStackTrace();
			}
		}

		void shutdown() {
			stop = true;
		}
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
	
    private Properties parseProperties(String arg) {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(arg);
        if (null == in) {
            System.out.println("Unable to access resource [" + arg + "]");
            return null;
        }
        Properties properties = new Properties();
        try {
            properties.load(in);
        }
        catch (IOException e) {
            System.err.println("Error loading from resource [" + arg + "] :" + e.getMessage());
            return null;
        }
        finally {
            try {
                in.close();
            } catch (IOException ignored) {
            }
        }
        System.out.println(properties);
        return properties;
    }
	
	public void testConnection() {
		MyServer server = new MyServer();
		Thread t = new Thread(server);
		t.start();

		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
		}

		Properties properties = parseProperties("test.properties");
		SessionManager sessionManager = SessionManager.getSessionManager(properties, "localhost", 8000,
				(int) TimeUnit.MILLISECONDS.convert(5*60, TimeUnit.SECONDS));
		TypeFactory ff = sessionManager.getTypeFactory();		
		Session session = sessionManager.openSession();
		try {
			TypeDescriptor employee_rowtype[] = { 
					ff.getIntegerType(), /* primary key */
					ff.getVarcharType(20), /* name */
					ff.getVarcharType(20), /* surname */
					ff.getVarcharType(20), /* city */
					ff.getVarcharType(45), /* email address */
					ff.getDateTimeType(), /* date of birth */
					ff.getNumberType(2) /* salary */
			};
			TableDefinition tableDefinition = sessionManager.newTableDefinition(
					"employee", 1, employee_rowtype);
			tableDefinition.addIndex(2, "employee1.idx", new int[] { 0 }, true,
					true);
			tableDefinition.addIndex(3, "employee2.idx", new int[] { 2, 1 },
					false, false);
			tableDefinition.addIndex(4, "employee3.idx", new int[] { 5 },
					false, false);
			tableDefinition.addIndex(5, "employee4.idx", new int[] { 6 },
					false, false);
			session.createTable(tableDefinition);
			session.startTransaction(IsolationMode.READ_COMMITTED);
			boolean success = false;
			try {
				Table table = session.getTable(1);
				System.out.println(table);	
				Row tableRow = table.getRow();
				tableRow.setInt(0, 1);
				tableRow.setString(1, "Joe");
				tableRow.setString(2, "Blogg");
				tableRow.setDate(5, getDOB(1930, 12, 31));
				tableRow.setString(6, "500.00");
				table.addRow(tableRow);
				
				try {
					// following should fail due to unique constraint violation
					table.addRow(tableRow);
					fail("Unique constraint failed");
				} catch (SimpleDBMException e) {
					assertEquals("WRB00003", e.getMessageKey());
					System.err.println("Error: " + e.getMessage());
				}
				TableScan scan = table.openScan(0, null, false);
				try {
					Row row = scan.fetchNext();
					while (row != null) {
						System.err.println("Fetched row " + row);
						row.setString(6, "501.00");
						scan.updateCurrentRow(row);
						row = scan.fetchNext();
					}
				}
				finally {
					scan.close();
				}
				success = true;
			}
			finally {
				if (success) {
					session.commit();
				}
				else {
					session.rollback();
				}
			}
			session.startTransaction(IsolationMode.READ_COMMITTED);
			success = false;
			try {
				Table table = session.getTable(1);
				TableScan scan = table.openScan(0, null, false);
				try {
					Row row = scan.fetchNext();
					while (row != null) {
						System.err.println("Deleting row " + row);
						scan.deleteRow();
						row = scan.fetchNext();
					}
				}
				finally {
					scan.close();
				}
				success = true;
			}
			finally {
				if (success) {
					session.commit();
				}
				else {
					session.rollback();
				}
			}
			
			System.out.println("Sleeping for 30 secs");
			Thread.sleep(30*1000);
			session.close();
		}
		catch (Exception e) {
			e.printStackTrace();
		} finally {
		}
//		try {
//			TypeDescriptor[] td = sessionManager.getRowType(1);
//			for (int i = 0; i < td.length; i++) {
//				System.out.println(td[i]);				
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		server.shutdown();

		try {
			t.join();
		} catch (InterruptedException e) {
		}
	}
}
