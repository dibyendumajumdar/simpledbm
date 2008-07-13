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
package org.simpledbm.rss.impl.st;

import java.io.File;
import java.util.Properties;

import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageContainerInfo;
import org.simpledbm.rss.api.st.StorageException;
import org.simpledbm.rss.api.st.StorageManager;

/**
 * Test cases for Storage Container (IO) module.
 * 
 * @author Dibyendu Majumdar
 * @since 21-Aug-2005
 */
public class TestStorageContainer extends BaseTestCase {

	public TestStorageContainer(String arg0) {
		super(arg0);
	}

	public void testCreate() throws Exception {
		String name = "testfile";
		File file = new File("testdata/TestStorageContainer/" + name);
		file.delete();
		assertTrue(!file.exists());
		Properties properties = new Properties();
		properties.setProperty("storage.basePath",
				"testdata/TestStorageContainer");
		final StorageContainerFactory factory = new FileStorageContainerFactory(
				properties);
		StorageContainer sc = factory.create(name);
		sc.write(0, new byte[10], 0, 10);
		sc.flush();
		assertTrue(file.exists());
		assertTrue(file.length() == 10);
		sc.close();
		assertTrue(file.exists());
		assertTrue(file.length() == 10);
		sc = factory.open(name);
		sc.close();
		assertTrue(file.exists());
		assertTrue(file.length() == 10);
                try {
                    sc = factory.createIfNotExisting(name);
                    fail("Error: should fail to create a container if it already exists");
                }
                catch (StorageException e) {
                    assertTrue(e.getMessage().contains("ES0017"));
                }
//		sc.close();
		assertTrue(file.exists());
		assertTrue(file.length() == 10);
		file.delete();
		boolean caughtException = false;
		try {
			sc = factory.open(name);
                        fail("Error: should fail as the container has been deleted");
//			sc.close();
		} catch (Exception e) {
			// e.printStackTrace();
//			caughtException = true;
		}
//		assertTrue(caughtException);
	}

	public void testCase2() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("storage.basePath",
				"testdata/TestStorageContainer");
		final StorageContainerFactory factory = new FileStorageContainerFactory(
				properties);
		StorageManager storageManager = new StorageManagerImpl(properties);
		String name = "testfile";
		File file = new File("testdata/TestStorageContainer/" + name);
		file.delete();
		assertTrue(!file.exists());
		StorageContainerInfo[] activeContainers = storageManager
				.getActiveContainers();
		assertEquals(activeContainers.length, 0);
		StorageContainer sc = factory.create(name);
		storageManager.register(1, sc);
		activeContainers = storageManager.getActiveContainers();
		assertEquals(activeContainers.length, 1);
		assertEquals(activeContainers[0].getContainerId(), 1);
		assertEquals(activeContainers[0].getName(), "testfile");
		storageManager.shutdown();
		activeContainers = storageManager.getActiveContainers();
		assertEquals(activeContainers.length, 0);
		assertTrue(file.exists());
		factory.delete("testfile");
		assertFalse(file.exists());
	}

	public void testCase3() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("storage.basePath",
				"testdata/TestStorageContainer");
		final StorageContainerFactory factory = new FileStorageContainerFactory(
				properties);
		factory.create("testfile1").close();
		factory.create("./testfile2").close();
		factory.create("./mypath/testfile2").close();
		factory.delete("./mypath/testfile2");
		factory.delete("./testfile2");
		factory.delete("testfile1");
	}

	// test case disabled
	public void _testCase4() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("storage.basePath",
				"testdata/TestStorageContainer");
		final StorageContainerFactory factory = new FileStorageContainerFactory(
				properties);
		factory.create("lockfile").close();
		try {
			StorageContainer sc = factory.open("lockfile");
			try {
				sc.lock();

				Thread t = new Thread(new Runnable() {
					public void run() {
						StorageContainer sc2 = factory.open("lockfile");
						try {
							sc2.lock();
							// THIS TEST DOES NOT WORK ON MAC OS X
						
							sc2.unlock();
						} catch (Exception e) {
							setThreadFailed(Thread.currentThread(), e);
						} finally {
							sc2.close();
						}
					}
				});
				sc.unlock();
			} finally {
				sc.close();
			}
		} catch (Exception e) {
			fail(e.getMessage());
		} finally {
			factory.delete("lockfile");
		}
		checkThreadFailures();
	}

}
