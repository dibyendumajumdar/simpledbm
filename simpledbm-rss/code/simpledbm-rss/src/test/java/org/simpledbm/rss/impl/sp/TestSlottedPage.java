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
package org.simpledbm.rss.impl.sp;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.registry.ObjectRegistry;
import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.common.api.registry.StorableFactory;
import org.simpledbm.common.impl.platform.PlatformImpl;
import org.simpledbm.common.impl.registry.ObjectRegistryImpl;
import org.simpledbm.common.util.ByteString;
import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.sp.SlottedPageManager;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.impl.latch.LatchFactoryImpl;
import org.simpledbm.rss.impl.pm.PageManagerImpl;
import org.simpledbm.rss.impl.st.StorageManagerImpl;

public class TestSlottedPage extends BaseTestCase {

    public TestSlottedPage(String arg0) {
        super(arg0);
    }

    public static class StringItem implements Storable {

        private final ByteString string;
        
        public StringItem(ByteBuffer buf) {
        	string = new ByteString(buf);
        }

        public StringItem(String s) {
			string = new ByteString(s);
		}
        
        @Override
        public String toString() {
            return string.toString();
        }

        public int getStoredLength() {
            return string.getStoredLength();
        }

        public void store(ByteBuffer bb) {
            string.store(bb);
        }
    }
    
    static class StringItemFactory implements StorableFactory {
		public Storable getStorable(ByteBuffer buf) {
			return new StringItem(buf);
		}
    }
    
    static StringItemFactory stringItemFactory = new StringItemFactory();

    void printItems(SlottedPageImpl page) {
        page.dump();
        for (int i = 0; i < page.getNumberOfSlots(); i++) {
            if (!page.isSlotDeleted(i)) {
//                System.err.println("Item=[" + i + "]={"
//                        + page.get(i, new StringItem()) + "}");
                System.err.println("Item=[" + i + "]={"
                        + page.get(i, stringItemFactory) + "}");
            } else {
                System.err.println("Item=[" + i + "]=*deleted*");
            }
        }
    }

    public void testInsertItems() throws Exception {
        SlottedPageImpl.TESTING = true;
    	Properties properties = new Properties();
        properties.setProperty("logging.properties.file", "classpath:simpledbm.logging.properties");
        properties.setProperty("logging.properties.type", "log4j");
        final Platform platform = new PlatformImpl(properties);
        final ObjectRegistry objectFactory = new ObjectRegistryImpl(platform, properties);
//		final StorageContainerFactory storageFactory = new FileStorageContainerFactory();
        final StorageManager storageManager = new StorageManagerImpl(platform, properties);
        final LatchFactory latchFactory = new LatchFactoryImpl(platform, properties);
        final PageManager pageFactory = new PageManagerImpl(
        	platform,
            objectFactory,
            storageManager,
            latchFactory,
            properties);
        final SlottedPageManager spmgr = new SlottedPageManagerImpl(platform, 
            objectFactory, pageFactory, properties);
        SlottedPageImpl page = (SlottedPageImpl) pageFactory.getInstance(spmgr
            .getPageType(), new PageId());
        page.latchExclusive();
        StringItem item = new StringItem("Dibyendu Majumdar, This is pretty cool");
        assertEquals(page.getFreeSpace(), page.getSpace());
        page.insert(item);
        assertEquals(page.getFreeSpace(), page.getSpace()
                - (item.getStoredLength() + SlottedPageImpl.Slot.SIZE));
        assertEquals(page.getNumberOfSlots(), 1);
        assertEquals(page.getDeletedSlots(), 0);
        printItems(page);
//        StringItem item2 = new StringItem();
//        item2.setString("Dibyendu Majumdar, This is pretty foolish");
        StringItem item2 = new StringItem("Dibyendu Majumdar, This is pretty foolish");
        page.insertAt(1, item2, false);
        assertEquals(
            page.getFreeSpace(),
            page.getSpace()
                    - (item.getStoredLength() + item2.getStoredLength() + SlottedPageImpl.Slot.SIZE * 2));
        assertEquals(page.getNumberOfSlots(), 2);
        assertEquals(page.getDeletedSlots(), 0);
        printItems(page);
        page.delete(0);
        assertEquals(page.getFreeSpace(), page.getSpace()
                - (item2.getStoredLength() + SlottedPageImpl.Slot.SIZE * 2));
        assertEquals(page.getNumberOfSlots(), 2);
        assertEquals(page.getDeletedSlots(), 1);
        printItems(page);
        StringItem item3 = new StringItem("Dibyendu Majumdar, SimpleDBM will succeed");
        page.insertAt(2, item3, false);
        assertEquals(
            page.getFreeSpace(),
            page.getSpace()
                    - (item2.getStoredLength() + item3.getStoredLength() + SlottedPageImpl.Slot.SIZE * 3));
        assertEquals(page.getNumberOfSlots(), 3);
        assertEquals(page.getDeletedSlots(), 1);
        printItems(page);
        StringItem item4 = new StringItem("This is a long item, and should force the page to be compacted - by removing holes");
        page.insertAt(3, item4, false);
        assertEquals(
            page.getFreeSpace(),
            page.getSpace()
                    - (item2.getStoredLength() + item3.getStoredLength()
                            + item4.getStoredLength() + SlottedPageImpl.Slot.SIZE * 4));
        assertEquals(page.getNumberOfSlots(), 4);
        assertEquals(page.getDeletedSlots(), 1);
        printItems(page);
        page.purge(0);
        assertEquals(
            page.getFreeSpace(),
            page.getSpace()
                    - (item2.getStoredLength() + item3.getStoredLength()
                            + item4.getStoredLength() + SlottedPageImpl.Slot.SIZE * 3));
        assertEquals(page.getNumberOfSlots(), 3);
        assertEquals(page.getDeletedSlots(), 0);
        printItems(page);
        page.setFlags(2, (short) 6);
        page.insertAt(2, item4, true);
        assertEquals(
            page.getFreeSpace(),
            page.getSpace()
                    - (item2.getStoredLength() + item3.getStoredLength()
                            + item4.getStoredLength() + SlottedPageImpl.Slot.SIZE * 3));
        assertEquals(page.getNumberOfSlots(), 3);
        assertEquals(page.getDeletedSlots(), 0);
        assertEquals(page.getFlags(2), 6);
        printItems(page);
        System.out.println(page.toString());
        SlottedPageImpl.TESTING = false;
    }
}
