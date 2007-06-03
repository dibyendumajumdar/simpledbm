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
package org.simpledbm.rss.impl.bm;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.simpledbm.rss.api.bm.BufferManager;
import org.simpledbm.rss.api.bm.BufferAccessBlock;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.api.wal.Lsn;
import org.simpledbm.rss.impl.bm.BufferManagerImpl;
import org.simpledbm.rss.impl.latch.LatchFactoryImpl;
import org.simpledbm.rss.impl.pm.PageFactoryImpl;
import org.simpledbm.rss.impl.registry.ObjectRegistryImpl;
import org.simpledbm.rss.impl.st.FileStorageContainerFactory;
import org.simpledbm.rss.impl.st.StorageManagerImpl;

/**
 * Contains test cases for Buffer Manager module.
 * 
 * <p>
 * TODO: Test accessing page from non-existent container.
 * <p>
 * TODO: Test case when page is written to non-existent container.
 * First fix a new page. And then attempt to write it.
 * <p>
 * TODO: Test interaction between read/write access to pages.
 * <p>
 * TODO: Test that read access is possible while buffer write is
 * going on, but not write access.
 * 
 * @author Dibyendu Majumdar
 * @since 18-Aug-2005
 */
public class TestBufferManager extends TestCase {

	static final short TYPE_MYPAGE = 25000;
	
    public TestBufferManager(String arg0) {
        super(arg0);
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

    }

    public void testCase1() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("storage.basePath", "testdata/TestBufferManager");
        final StorageContainerFactory storageFactory = new FileStorageContainerFactory(properties);
        ObjectRegistry objectFactory = new ObjectRegistryImpl();
        StorageManager storageManager = new StorageManagerImpl();
        LatchFactory latchFactory = new LatchFactoryImpl();
        PageFactory pageFactory = new PageFactoryImpl(objectFactory,
                storageManager, latchFactory);
        BufferManagerImpl bufmgr = new BufferManagerImpl(null, pageFactory, 3, 11);

        String name = "testfile.dat";
        File file = new File(name);
        file.delete();
        
        StorageContainer sc = storageFactory.create(name);
        storageManager.register(1, sc);
        objectFactory.registerType(TYPE_MYPAGE, MyPage.class.getName());

        bufmgr.start();
        
        BufferAccessBlock bab = bufmgr.fixExclusive(new PageId(1, 0), true, TYPE_MYPAGE, 0);
        MyPage page = (MyPage) bab.getPage();
        page.i = 534;
        bab.setDirty(new Lsn(97, 45));
        bab.unfix();
        bufmgr.signalBufferWriter();
        bab = bufmgr.fixShared(new PageId(1, 0), 0);
        page = (MyPage) bab.getPage();
        // System.out.println("Retrieved page contents = " + page);
        assertEquals(page.i, 534);
        assertEquals(page.getPageLsn(), new Lsn(97, 45));
        assertEquals(page.getPageId(), new PageId(1, 0));
        assertEquals(page.getType(), TYPE_MYPAGE);
        bab.unfix();
        bufmgr.shutdown();
        storageManager.shutdown();

        sc = storageFactory.open(name);
        storageManager.register(1, sc);
        bufmgr = new BufferManagerImpl(null, pageFactory, 3, 11);
        bufmgr.start();
        bab = bufmgr.fixShared(new PageId(1, 0), 0);
        page = (MyPage) bab.getPage();
        // System.out.println("Retrieved page contents = " + page);
        assertEquals(page.i, 534);
        assertEquals(page.getPageLsn(), new Lsn(97, 45));
        assertEquals(page.getPageId(), new PageId(1, 0));
        assertEquals(page.getType(), TYPE_MYPAGE);
        bab.unfix();
        bufmgr.shutdown();
        storageManager.shutdown();
        storageFactory.delete("testfile.dat");
    }


    public void testCase2() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("storage.basePath", "testdata/TestBufferManager");
        final StorageContainerFactory storageFactory = new FileStorageContainerFactory(properties);
        final ObjectRegistry objectFactory = new ObjectRegistryImpl();
        final StorageManager storageManager = new StorageManagerImpl();
        final LatchFactory latchFactory = new LatchFactoryImpl();
        final PageFactory pageFactory = new PageFactoryImpl(objectFactory,
                storageManager, latchFactory);
        final BufferManager bufmgr = new BufferManagerImpl(null, pageFactory, 3, 11);
        final AtomicInteger errCount = new AtomicInteger(0);
        String name = "testfile.dat";
        StorageContainer sc = storageFactory.create(name);
        storageManager.register(1, sc);
        objectFactory.registerType(TYPE_MYPAGE, MyPage.class.getName());

        bufmgr.start();
        
        Runnable r1 = new Runnable() {
            public void run() {
                PageId pageId = new PageId(1, 1);
                BufferAccessBlock bab = null;
                try {
                    bab = bufmgr.fixExclusive(pageId, false, TYPE_MYPAGE, 0);
                    // System.err.println(Thread.currentThread().getName()
                    //        + ": Fixed page " + pageId);
                    Thread.sleep(300);
                    bab.setDirty(new Lsn());
                    // System.err.println(Thread.currentThread().getName()
                    //        + ": Unfixing page " + pageId);
                    bab.unfix();
                } catch (Exception e) {
                    e.printStackTrace();
                    errCount.incrementAndGet();
                }
            }
        };

        Runnable r2 = new Runnable() {
            public void run() {
                PageId pageId = new PageId(1, 1);
                BufferAccessBlock bab = null;
                try {
                    bab = bufmgr.fixExclusive(pageId, false, TYPE_MYPAGE, 0);
                    // System.err.println(Thread.currentThread().getName()
                    //        + ": Fixed page " + pageId);
                    bab.setDirty(new Lsn());
                    //System.err.println(Thread.currentThread().getName()
                    //        + ": Unfixing page " + pageId);
                    bab.unfix();
                } catch (Exception e) {
                    e.printStackTrace();
                    errCount.incrementAndGet();
                }
            }
        };

        BufferAccessBlock bufab;
        bufab = bufmgr.fixExclusive(new PageId(1, 1), true, TYPE_MYPAGE, 0);
        bufab.setDirty(new Lsn());
        bufab.unfix();

        Thread t1;
        Thread t2;
        Thread t3;

        t1 = new Thread(r1, "Thread 1");
        t2 = new Thread(r1, "Thread 2");
        t2.start();
        t1.start();
        Thread.sleep(100);

        t3 = new Thread(r2, "Thread 3");
        t3.start();
        t1.join();
        t2.join();
        t3.join();

        bufmgr.shutdown();
        storageManager.shutdown();
        storageFactory.delete("testfile.dat");
    }

    /**
     * Multiple thread concurrent read/write test, supporting 3 different scenarios:
     * <p>
     * <ul>
     * <li>
     * If (scenario == 3), 3 threads update the same page.
     * </li><li>
     * If (scenario == 2), 3 threads update 5 pages.
     * </li><li>
     * If (scenario == 1), 3 threads update 2 pages.
     * </li>
     * </ul>
     * <p>
     * BufMgr can only have 3 pages in memory, therefore scenario == 2 results in IO.
     */
    public void runtests(int scenario) throws Exception {

		Properties properties = new Properties();
		properties.setProperty("storage.basePath", "testdata/TestBufferManager");
        final StorageContainerFactory storageFactory = new FileStorageContainerFactory(properties);
        final ObjectRegistry objectFactory = new ObjectRegistryImpl();
        final StorageManager storageManager = new StorageManagerImpl();
        final LatchFactory latchFactory = new LatchFactoryImpl();
        final PageFactory pageFactory = new PageFactoryImpl(objectFactory,
                storageManager, latchFactory);
        final BufferManager bufmgr = new BufferManagerImpl(null, pageFactory, 3, 11);
        final AtomicInteger errCount = new AtomicInteger(0);
        final int testing = scenario;
        final int ITERATIONS = 10;
        final int UPDATES_PER_ITERATION = 100;
        final int UPDATES_PER_THREAD = ITERATIONS * UPDATES_PER_ITERATION;
        final int NUM_THREADS = 3;
        final int TOTAL_UPDATES = UPDATES_PER_THREAD * NUM_THREADS;
        final int page_count[] = new int[] { 0, 2, 5, 1 };
        final int expected_values[] = new int[] { 0,
                TOTAL_UPDATES / page_count[1], TOTAL_UPDATES / page_count[2],
                TOTAL_UPDATES / page_count[3] };

        final AtomicInteger calculated_values[] = new AtomicInteger[6];
        
        for (int i = 0; i < calculated_values.length; i++) {
        	calculated_values[i] = new AtomicInteger(0);
        }
        
        String name = "testfile.dat";
        StorageContainer sc = storageFactory.create(name);
        storageManager.register(1, sc);
        objectFactory.registerType(TYPE_MYPAGE, MyPage.class.getName());

        bufmgr.start();
        
        Runnable r3 = new Runnable() {
            public void run() {
                int pageno = 1;
                try {
                    for (int y = 1; y <= ITERATIONS; y++) {
                        for (int z = 1; z <= UPDATES_PER_ITERATION; z++) {
                            PageId pageId = new PageId(1, pageno);
                            BufferAccessBlock bab = null;
                            bab = bufmgr.fixExclusive(pageId, false, TYPE_MYPAGE, 0);
                            MyPage page = (MyPage) bab.getPage();
                            int i = page.i;
                            i = i + 1;
                            page.i = i;
//                            System.err.println(Thread.currentThread().getName()
//                                    + ": Value of i in page " + pageId
//                                    + " has been set to " + i);
                            bab.setDirty(new Lsn());
                            bab.unfix();
                            calculated_values[pageno].incrementAndGet();
                            pageno++;
                            if (pageno > page_count[testing]) {
                                pageno = 1;
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    errCount.incrementAndGet();
                }
            }
        };

        /* Initialize the pages */
        BufferAccessBlock bufab;
        for (int y = 1; y <= page_count[testing]; y++) {
            bufab = bufmgr.fixExclusive(new PageId(1, y), true, TYPE_MYPAGE, 0);
            bufab.setDirty(new Lsn());
            bufab.unfix();
        }
        
        /* Run the test scenario */
        Thread threads[] = new Thread[NUM_THREADS];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(r3, "Thread " + i);
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        assertEquals(0, errCount.get());
        /* Verify that the test was successful */
        for (int z = 1; z <= page_count[testing]; z++) {
            PageId pageId = new PageId(1, z);
            BufferAccessBlock bab = bufmgr.fixShared(pageId, 0);
            MyPage page = (MyPage) bab.getPage();
            int i = page.i;
            bab.unfix();
//            System.err.println("Expected value = " + expected_values[testing]);
//            System.err.println("Calculated value = " + calculated_values[z]);
            assertEquals(expected_values[testing], i);
            System.err.println(Thread.currentThread().getName()
                    + ": Validated that value of i in page " + pageId
                    + " is " + expected_values[testing]);
        }

        bufmgr.shutdown();
        storageManager.shutdown();
        storageFactory.delete("testfile.dat");
    }
    
    public void testCase3() throws Exception {
        runtests(1);
    }

    public void testCase4() throws Exception {
        runtests(2);
    }

    public void testCase5() throws Exception {
        runtests(3);
    }
    
    /**
     * Test that update mode latch blocks readers.
     */
    public void testCase6() throws Exception {
		Properties properties = new Properties();
		properties.setProperty("storage.basePath", "testdata/TestBufferManager");
        final StorageContainerFactory storageFactory = new FileStorageContainerFactory(properties);
        final ObjectRegistry objectFactory = new ObjectRegistryImpl();
        final StorageManager storageManager = new StorageManagerImpl();
        final LatchFactory latchFactory = new LatchFactoryImpl();
        final PageFactory pageFactory = new PageFactoryImpl(objectFactory,
                storageManager, latchFactory);
        final BufferManager bufmgr = new BufferManagerImpl(null, pageFactory, 3, 11);
        final AtomicInteger sync = new AtomicInteger(0);
    	
        Thread t1 = new Thread() {
        	@Override
			public void run() {
        		try {
					PageId pageId = new PageId(1, 0);
					BufferAccessBlock bab = null;
					assertTrue(sync.compareAndSet(1, 2));
			        System.err.println("T2 (2) Trying to obtain shared latch on page");
					bab = bufmgr.fixShared(pageId, 0);
					assertTrue(sync.compareAndSet(5, 6));
			        System.err.println("T2 (6) Obtained shared latch on page");
					bab.unfix();
				} catch (Exception e) {
					e.printStackTrace();
				}
        	}
        };

        String name = "testfile.dat";
        StorageContainer sc = storageFactory.create(name);
        storageManager.register(1, sc);
        objectFactory.registerType(TYPE_MYPAGE, MyPage.class.getName());

        bufmgr.start();
        
        try {
			BufferAccessBlock bufab;
			bufab = bufmgr.fixExclusive(new PageId(1, 0), true, TYPE_MYPAGE, 0);
			bufab.setDirty(new Lsn());
			bufab.unfix();

			System.err.println("T1 (1) Locking page in UPDATE mode");
			assertTrue(sync.compareAndSet(0, 1));
			bufab = bufmgr.fixForUpdate(new PageId(1, 0), 0);
			t1.start();
			Thread.sleep(100);
			assertTrue(sync.compareAndSet(2, 3));
			System.err.println("T1 (3) Upgrading lock to Exclusive");
			bufab.upgradeUpdateLatch();
			assertTrue(bufab.isLatchedExclusively());
			assertTrue(sync.compareAndSet(3, 4));
			System.err.println("T1 (4) Downgrading lock to Update");
			bufab.downgradeExclusiveLatch();
			assertTrue(bufab.isLatchedForUpdate());
			assertTrue(sync.compareAndSet(4, 5));
			System.err.println("T1 (5) Releasing Update latch on page");
			bufab.unfix();
			t1.join(2000);
			assertTrue(!t1.isAlive());
		} finally {
			bufmgr.shutdown();
			storageManager.shutdown();
			storageFactory.delete("testfile.dat");
		}
    }

    /*
     * Test that Update mode access is possible when page is
     * held in shared mode. Also test that update mode conflicts with
     * update mode.
     */
    public void testCase7() throws Exception {
    	    	
		Properties properties = new Properties();
		properties.setProperty("storage.basePath", "testdata/TestBufferManager");
        final StorageContainerFactory storageFactory = new FileStorageContainerFactory(properties);
        final ObjectRegistry objectFactory = new ObjectRegistryImpl();
        final StorageManager storageManager = new StorageManagerImpl();
        final LatchFactory latchFactory = new LatchFactoryImpl();
        final PageFactory pageFactory = new PageFactoryImpl(objectFactory,
                storageManager, latchFactory);
        final BufferManager bufmgr = new BufferManagerImpl(null, pageFactory, 3, 11);
        final AtomicInteger sync = new AtomicInteger(0);
    	
        Thread t1 = new Thread() {
        	@Override
			public void run() {
        		try {
					PageId pageId = new PageId(1, 0);
					BufferAccessBlock bab = null;
					assertTrue(sync.compareAndSet(1, 2));
			        System.err.println("T2 (2) Trying to obtain UPDATE latch on page");
					bab = bufmgr.fixForUpdate(pageId, 0);
					assertTrue(sync.compareAndSet(2, 3));
			        System.err.println("T2 (3) Obtained UPDATE latch on page, waiting");
			        Thread.sleep(1000);
					assertTrue(sync.compareAndSet(5, 6));
					System.err.println("T2 (6) Releasing Update latch on page");
					bab.unfix();
				} catch (Exception e) {
					e.printStackTrace();
				}
        	}
        };

        String name = "testfile.dat";
        StorageContainer sc = storageFactory.create(name);
        storageManager.register(1, sc);
        objectFactory.registerType(TYPE_MYPAGE, MyPage.class.getName());

        bufmgr.start();
        
        try {
			BufferAccessBlock bufab;
			bufab = bufmgr.fixExclusive(new PageId(1, 0), true, TYPE_MYPAGE, 0);
			bufab.setDirty(new Lsn());
			bufab.unfix();

			assertTrue(sync.compareAndSet(0, 1));
			System.err.println("T1 (1) Locking page in SHARED mode");
			bufab = bufmgr.fixShared(new PageId(1, 0), 0);
			t1.start();
			Thread.sleep(100);
			assertTrue(sync.compareAndSet(3, 4));
			System.err.println("T1 (4) Releasing SHARED latch on page");
			bufab.unfix();
			assertTrue(sync.compareAndSet(4, 5));
			System.err.println("T1 (5) Obtaining UPDATE mode latch");
			bufab = bufmgr.fixForUpdate(new PageId(1, 0), 0);
			assertTrue(bufab.isLatchedForUpdate());
			assertTrue(sync.compareAndSet(6, 7));
			System.err.println("T1 (7) Obtained Update latch");
			assertTrue(sync.compareAndSet(7, 8));
			System.err.println("T1 (8) Releasing Update latch on page");
			bufab.unfix();
			
			t1.join(2000);
			assertTrue(!t1.isAlive());
		} finally {
			bufmgr.shutdown();
			storageManager.shutdown();
			storageFactory.delete("testfile.dat");
		}
    }
}
