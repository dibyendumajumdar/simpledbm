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
package org.simpledbm.rss.impl.pm;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.api.wal.Lsn;
import org.simpledbm.rss.impl.latch.LatchFactoryImpl;
import org.simpledbm.rss.impl.registry.ObjectRegistryImpl;
import org.simpledbm.rss.impl.st.FileStorageContainerFactory;
import org.simpledbm.rss.impl.st.StorageManagerImpl;

/**
 * Test cases for Page Management module.
 * 
 * @author Dibyendu Majumdar
 * @since 21-Aug-2005
 */
public class TestPage extends BaseTestCase {

    static final short TYPE_MYPAGE = 25000;

    public TestPage(String arg0) {
        super(arg0);
    }

    public void testCase1() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("storage.basePath", "testdata/TestPage");
        final StorageContainerFactory storageFactory = new FileStorageContainerFactory(
            properties);
        ObjectRegistry objectFactory = new ObjectRegistryImpl(properties);
        StorageManager storageManager = new StorageManagerImpl(properties);
        LatchFactory latchFactory = new LatchFactoryImpl(properties);
        PageManager pageFactory = new PageManagerImpl(
            objectFactory,
            storageManager,
            latchFactory,
            properties);

        String name = "testfile.dat";
        StorageContainer sc = storageFactory.create(name);
        storageManager.register(1, sc);

        Page page = pageFactory.getInstance(
            pageFactory.getRawPageType(),
            new PageId(1, 0));
        page.setPageLsn(new Lsn(91, 33));
        pageFactory.store(page);
        page = pageFactory.retrieve(new PageId(1, 0));
        System.out.println("Retrieved page contents = " + page);
        assertEquals(new PageId(1, 0), page.getPageId());
        assertEquals(new Lsn(91, 33), page.getPageLsn());
        storageManager.shutdown();
        storageFactory.delete("testfile.dat");
    }

    static public class MyPage extends Page {

        int i = 0;
        
        MyPage(PageManager pageFactory, int type, PageId pageId) {
			super(pageFactory, type, pageId);
		}

		MyPage(PageManager pageFactory, PageId pageId, ByteBuffer bb) {
			super(pageFactory, pageId, bb);
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
        
        static class MyPageFactory implements PageFactory {

        	final PageManager pageManager;
        	
        	public MyPageFactory(PageManager pageManager) {
        		this.pageManager = pageManager;
        	}
			public Page getInstance(int type, PageId pageId) {
				return new MyPage(pageManager, type, pageId);
			}

			public Page getInstance(PageId pageId, ByteBuffer bb) {
				return new MyPage(pageManager, pageId, bb);
			}

			public int getPageType() {
				return TYPE_MYPAGE;
			}
        	
        }
    }

    public void testCase2() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("storage.basePath", "testdata/TestPage");
        final StorageContainerFactory storageFactory = new FileStorageContainerFactory(
            properties);
        ObjectRegistry objectFactory = new ObjectRegistryImpl(properties);
        StorageManager storageManager = new StorageManagerImpl(properties);
        LatchFactory latchFactory = new LatchFactoryImpl(properties);
        PageManager pageFactory = new PageManagerImpl(
            objectFactory,
            storageManager,
            latchFactory,
            properties);

        String name = "testfile.dat";
        StorageContainer sc = storageFactory.create(name);
        storageManager.register(1, sc);
        objectFactory.registerSingleton(TYPE_MYPAGE, new MyPage.MyPageFactory(pageFactory));

        MyPage page = (MyPage) pageFactory.getInstance(TYPE_MYPAGE, new PageId(
            1,
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

    public void testCase3() {
        Properties properties = new Properties();
        properties.setProperty("storage.basePath", "testdata/TestPage");
        final StorageContainerFactory storageFactory = new FileStorageContainerFactory(
            properties);
        ObjectRegistry objectFactory = new ObjectRegistryImpl(properties);
        StorageManager storageManager = new StorageManagerImpl(properties);
        LatchFactory latchFactory = new LatchFactoryImpl(properties);
        PageManager pageFactory = new PageManagerImpl(
            objectFactory,
            storageManager,
            latchFactory,
            properties);
        objectFactory.registerSingleton(TYPE_MYPAGE, new MyPage.MyPageFactory(pageFactory));
        MyPage page = (MyPage) pageFactory.getInstance(TYPE_MYPAGE, new PageId(
            1,
            0));
        
        assertEquals(pageFactory.getUsablePageSize() - Page.SIZE, page.getAvailableLength());
        assertEquals(pageFactory.getPageSize(), page.getStoredLength());
    }
    
}
