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
package org.simpledbm.rss.impl.pm;

import java.nio.ByteBuffer;
import java.util.Properties;

import org.simpledbm.common.api.registry.ObjectRegistry;
import org.simpledbm.common.impl.registry.ObjectRegistryImpl;
import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.api.wal.Lsn;
import org.simpledbm.rss.impl.latch.LatchFactoryImpl;
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

//    Platform platform;
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
        properties.setProperty("logging.properties.file",
                "classpath:simpledbm.logging.properties");
        properties.setProperty("logging.properties.type", "log4j");
//        platform = new PlatformImpl(properties);
        storageFactory = new FileStorageContainerFactory(platform, properties);
        objectRegistry = new ObjectRegistryImpl(platform, properties);
        storageManager = new StorageManagerImpl(platform, properties);
        latchFactory = new LatchFactoryImpl(platform, properties);
        pageManager = new PageManagerImpl(platform, objectRegistry,
                storageManager, latchFactory, properties);
        objectRegistry.registerSingleton(TYPE_MYPAGE, new MyPage.MyPageFactory(
                pageManager));
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

        Page page = pageManager.getInstance(pageManager.getRawPageType(),
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
                1, 0));
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
                1, 0));
        assertEquals(pageManager.getUsablePageSize() - Page.SIZE, page
                .getAvailableLength());
        assertEquals(pageManager.getPageSize(), page.getStoredLength());
    }

}
