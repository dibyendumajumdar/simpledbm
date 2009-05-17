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
package org.simpledbm.rss.impl.pm;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.platform.Platform;
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
import org.simpledbm.rss.impl.platform.PlatformImpl;
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
    
    Platform platform;
    StorageContainerFactory storageFactory;
    ObjectRegistry objectRegistry;
    StorageManager storageManager;
    LatchFactory latchFactory;
    PageManager pageManager;

    public TestPage(String arg0) {
        super(arg0);
    }
    
    @Override
	protected void setUp() throws Exception {
		super.setUp();
        Properties properties = new Properties();
        properties.setProperty("storage.basePath", "testdata/TestPage");
        properties.setProperty("logging.properties.file", "classpath:simpledbm.logging.properties");
        properties.setProperty("logging.properties.type", "log4j");
        platform = new PlatformImpl(properties);
        storageFactory = new FileStorageContainerFactory(platform, 
            properties);
        objectRegistry = new ObjectRegistryImpl(platform, properties);
        storageManager = new StorageManagerImpl(platform, properties);
        latchFactory = new LatchFactoryImpl(platform, properties);
        pageManager = new PageManagerImpl(
        	platform,
            objectRegistry,
            storageManager,
            latchFactory,
            properties);
        objectRegistry.registerSingleton(TYPE_MYPAGE, new MyPage.MyPageFactory(pageManager));
    }

	@Override
	protected void tearDown() throws Exception {
		super.tearDown();
        storageManager.shutdown();
        storageFactory.delete("testfile.dat");
	}

	public void testCase1() throws Exception {
        String name = "testfile.dat";
        StorageContainer sc = storageFactory.create(name);
        storageManager.register(1, sc);

        Page page = pageManager.getInstance(
            pageManager.getRawPageType(),
            new PageId(1, 0));
        page.setPageLsn(new Lsn(91, 33));
        pageManager.store(page);
        page = pageManager.retrieve(new PageId(1, 0));
        System.out.println("Retrieved page contents = " + page);
        assertEquals(new PageId(1, 0), page.getPageId());
        assertEquals(new Lsn(91, 33), page.getPageLsn());
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
        String name = "testfile.dat";
        StorageContainer sc = storageFactory.create(name);
        storageManager.register(1, sc);
        MyPage page = (MyPage) pageManager.getInstance(TYPE_MYPAGE, new PageId(
            1,
            0));
        page.i = 9745;
        page.setPageLsn(new Lsn(97, 45));
        pageManager.store(page);
        page = (MyPage) pageManager.retrieve(new PageId(1, 0));
        System.out.println("Retrieved page contents = " + page);
        assertEquals(page.i, 9745);
        assertEquals(page.getPageLsn(), new Lsn(97, 45));
        assertEquals(page.getPageId(), new PageId(1, 0));
        assertEquals(page.getType(), TYPE_MYPAGE);
    }

    public void testCase3() {
        MyPage page = (MyPage) pageManager.getInstance(TYPE_MYPAGE, new PageId(
            1,
            0));
        assertEquals(pageManager.getUsablePageSize() - Page.SIZE, page.getAvailableLength());
        assertEquals(pageManager.getPageSize(), page.getStoredLength());
    }
    
}
