/**
 * DO NOT REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Contributor(s):
 *
 * The Original Software is SimpleDBM (www.simpledbm.org).
 * The Initial Developer of the Original Software is Dibyendu Majumdar.
 *
 * Portions Copyright 2005-2014 Dibyendu Majumdar. All Rights Reserved.
 *
 * The contents of this file are subject to the terms of the
 * Apache License Version 2 (the "APL"). You may not use this
 * file except in compliance with the License. A copy of the
 * APL may be obtained from:
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the APL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the APL, the GPL or the LGPL.
 *
 * Copies of GPL and LGPL may be obtained from:
 * http://www.gnu.org/licenses/license-list.html
 */
package org.simpledbm.rss.impl.st;

import java.io.File;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;
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
                platform, properties);
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
        } catch (StorageException e) {
            assertTrue(e.getErrorCode() == 17);
        }
        assertTrue(file.exists());
        assertTrue(file.length() == 10);
        file.delete();
        try {
            sc = factory.open(name);
            fail("Error: should fail as the container has been deleted");
        } catch (Exception e) {
        }
    }

    public void testCase2() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("storage.basePath",
                "testdata/TestStorageContainer");
        final StorageContainerFactory factory = new FileStorageContainerFactory(
                platform, properties);
        StorageManager storageManager = new StorageManagerImpl(platform,
                properties);
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
                platform, properties);
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
                platform, properties);
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

    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTest(new TestStorageContainer("testCreate"));
        suite.addTest(new TestStorageContainer("testCase2"));
        suite.addTest(new TestStorageContainer("testCase3"));
        return suite;
    }


}
