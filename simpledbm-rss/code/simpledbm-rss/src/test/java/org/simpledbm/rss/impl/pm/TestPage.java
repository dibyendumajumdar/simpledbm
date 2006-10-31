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
package org.simpledbm.rss.impl.pm;

import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.api.wal.Lsn;
import org.simpledbm.rss.impl.latch.LatchFactoryImpl;
import org.simpledbm.rss.impl.pm.PageFactoryImpl;
import org.simpledbm.rss.impl.registry.ObjectRegistryImpl;
import org.simpledbm.rss.impl.st.FileStorageContainerFactory;
import org.simpledbm.rss.impl.st.StorageManagerImpl;

/**
 * Test cases for Page Management module.
 * 
 * @author Dibyendu Majumdar
 * @since 21-Aug-2005
 */
public class TestPage extends TestCase {

	static final short TYPE_MYPAGE = 25000;
	
    public TestPage(String arg0) {
        super(arg0);
    }

    public void testCase1() throws Exception {
        StorageContainerFactory storageFactory = new FileStorageContainerFactory();
        ObjectRegistry objectFactory = new ObjectRegistryImpl();
        StorageManager storageManager = new StorageManagerImpl();
        LatchFactory latchFactory = new LatchFactoryImpl();
        PageFactory pageFactory = new PageFactoryImpl(objectFactory,
                storageManager, latchFactory);

        String name = "testfile.dat";
        StorageContainer sc = storageFactory.create(name);
        storageManager.register(1, sc);

        Page page = pageFactory.getInstance(pageFactory.getRawPageType(), new PageId(1, 0));
        page.setPageLsn(new Lsn(91, 33));
        pageFactory.store(page);
        page = pageFactory.retrieve(new PageId(1, 0));
        System.out.println("Retrieved page contents = " + page);
        assertEquals(page.getPageId(), new PageId(1, 0));
        assertEquals(page.getPageLsn(), new Lsn(91, 33));
        storageManager.shutdown();
        storageFactory.delete("testfile.dat");
    }

    static public class MyPage extends Page {

        int i = 0;

        public MyPage() {
            super();
        }

        /**
         * @see org.simpledbm.rss.api.pm.Page#retrieve(java.nio.ByteBuffer)
         */
        @Override
        public void retrieve(ByteBuffer bb) {
            super.retrieve(bb);
            i = bb.getInt();
        }

        /**
         * @see org.simpledbm.rss.api.pm.Page#store(java.nio.ByteBuffer)
         */
        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(i);
        }

        @Override
        public void init() {
        }

        @Override
		public String toString() {
            return super.toString() + ".MyPage(i = " + i + ")";
        }
    }

    public void testCase2() throws Exception {
        StorageContainerFactory storageFactory = new FileStorageContainerFactory();
        ObjectRegistry objectFactory = new ObjectRegistryImpl();
        StorageManager storageManager = new StorageManagerImpl();
        LatchFactory latchFactory = new LatchFactoryImpl();
        PageFactory pageFactory = new PageFactoryImpl(objectFactory,
                storageManager, latchFactory);

        String name = "testfile.dat";
        StorageContainer sc = storageFactory.create(name);
        storageManager.register(1, sc);
        objectFactory.register(TYPE_MYPAGE, MyPage.class.getName());

        MyPage page = (MyPage) pageFactory.getInstance(TYPE_MYPAGE, new PageId(1,
                0));
        page.i = 9745;
        page.setPageLsn(new Lsn(97, 45));
        pageFactory.store(page);
        page = (MyPage) pageFactory.retrieve(new PageId(1, 0));
        System.out.println("Retrieved page contents = " + page);
        assertEquals(page.i, 9745);
        assertEquals(page.getPageLsn(), new Lsn(97, 45));
        assertEquals(page.getPageId(), new PageId(1, 0));
        assertEquals(page.getType(), TYPE_MYPAGE);
        storageManager.shutdown();
        storageFactory.delete("testfile.dat");
    }

}
