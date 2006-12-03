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
package org.simpledbm.rss.impl.st;

import java.io.File;
import java.util.Properties;

import junit.framework.TestCase;

import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageContainerInfo;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.impl.st.FileStorageContainerFactory;
import org.simpledbm.rss.impl.st.StorageManagerImpl;

/**
 * Test cases for Storage Container (IO) module.
 * 
 * @author Dibyendu Majumdar
 * @since 21-Aug-2005
 */
public class TestStorageContainer extends TestCase {

    public TestStorageContainer(String arg0) {
        super(arg0);
    }

    public void testCreate() throws Exception {
        String name = "testfile";
        File file = new File("testdata/TestStorageContainer/" + name);
        file.delete();
        assertTrue(!file.exists());
		Properties properties = new Properties();
		properties.setProperty("storage.basePath", "testdata/TestStorageContainer");
        final StorageContainerFactory factory = new FileStorageContainerFactory(properties);
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
        sc = factory.create(name);
        sc.close();
        assertTrue(file.exists());
        assertTrue(file.length() == 0);
        file.delete();
        boolean caughtException = false;
        try {
            sc = factory.open(name);
            sc.close();
        }
        catch (Exception e) {
            // e.printStackTrace();
            caughtException = true;
        }
        assertTrue(caughtException);
    }

    public void testCase2() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("storage.basePath", "testdata/TestStorageContainer");
        final StorageContainerFactory factory = new FileStorageContainerFactory(properties);
        StorageManager storageManager = new StorageManagerImpl();
        String name = "testfile";
        File file = new File("testdata/TestStorageContainer/" + name);
        file.delete();
        assertTrue(!file.exists());
        StorageContainerInfo[] activeContainers = storageManager.getActiveContainers();
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
		properties.setProperty("storage.basePath", "testdata/TestStorageContainer");
        final StorageContainerFactory factory = new FileStorageContainerFactory(properties);
        factory.create("testfile1").close();
        factory.create("./testfile2").close();
        factory.create("./mypath/testfile2").close();
        factory.delete("./mypath/testfile2");
        factory.delete("./testfile2");
        factory.delete("testfile1");
    }
    
}
