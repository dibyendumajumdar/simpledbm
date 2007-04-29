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
package org.simpledbm.rss.impl.im.btree;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.simpledbm.junit.BaseTestCase;
import org.simpledbm.rss.api.bm.BufferAccessBlock;
import org.simpledbm.rss.api.fsm.FreeSpaceManager;
import org.simpledbm.rss.api.im.Index;
import org.simpledbm.rss.api.im.IndexKey;
import org.simpledbm.rss.api.im.IndexKeyFactory;
import org.simpledbm.rss.api.im.IndexScan;
import org.simpledbm.rss.api.im.UniqueConstraintViolationException;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.loc.LocationFactory;
import org.simpledbm.rss.api.locking.LockDeadlockException;
import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockMgrFactory;
import org.simpledbm.rss.api.locking.LockMode;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.sp.SlottedPage;
import org.simpledbm.rss.api.sp.SlottedPageManager;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.api.tx.BaseLockable;
import org.simpledbm.rss.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.LoggableFactory;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.api.tx.TransactionalModuleRegistry;
import org.simpledbm.rss.api.wal.Lsn;
import org.simpledbm.rss.impl.bm.BufferManagerImpl;
import org.simpledbm.rss.impl.fsm.FreeSpaceManagerImpl;
import org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.BTreeCursor;
import org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.BTreeImpl;
import org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.BTreeNode;
import org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.IndexItem;
import org.simpledbm.rss.impl.im.btree.BTreeIndexManagerImpl.LoadPageOperation;
import org.simpledbm.rss.impl.latch.LatchFactoryImpl;
import org.simpledbm.rss.impl.locking.LockEventListener;
import org.simpledbm.rss.impl.locking.LockManagerFactoryImpl;
import org.simpledbm.rss.impl.locking.LockManagerImpl;
import org.simpledbm.rss.impl.pm.PageFactoryImpl;
import org.simpledbm.rss.impl.registry.ObjectRegistryImpl;
import org.simpledbm.rss.impl.sp.SlottedPageManagerImpl;
import org.simpledbm.rss.impl.st.FileStorageContainerFactory;
import org.simpledbm.rss.impl.st.StorageManagerImpl;
import org.simpledbm.rss.impl.tx.LoggableFactoryImpl;
import org.simpledbm.rss.impl.tx.TransactionManagerImpl;
import org.simpledbm.rss.impl.tx.TransactionalModuleRegistryImpl;
import org.simpledbm.rss.impl.wal.LogFactoryImpl;
import org.simpledbm.rss.impl.wal.LogManagerImpl;
import org.simpledbm.rss.util.ByteString;
import org.simpledbm.rss.util.ClassUtils;

public class TestBTreeManager extends BaseTestCase {

	static final short TYPE_STRINGKEYFACTORY = 25000;
	static final short TYPE_ROWLOCATIONFACTORY = 25001;
	
    boolean doCrashTesting = false;

    public TestBTreeManager() {
        super();
    }

    public TestBTreeManager(String arg0) {
        super(arg0);
    }    
    
    public TestBTreeManager(String arg0, boolean crashTesting) {
        super(arg0);
        doCrashTesting = crashTesting;
    }    
    
    static boolean compressKeys = false;
    static boolean largeBM = false;
    
	/**
	 * A simple string key.
	 */
	public static class StringKey implements IndexKey {

		static final String MAX_KEY = "<INFINITY>";
		
		ByteString string = new ByteString();
		
		public void setString(String s) {
			parseString(s);
		}
	
		public void setBytes(byte[] bytes) {
			string = new ByteString(bytes);
		}
		
		@Override
		public String toString() {
			if (isMaxKey()) {
				return MAX_KEY;
			}
			return string.toString().trim();
		}
		
		public void parseString(String string) {
			if (MAX_KEY.equals(string)) {
				this.string = new ByteString(new byte[1]);
			}
			else {
				byte data[];
				if (!compressKeys) {
					data = new byte[1024];
					Arrays.fill(data, (byte)' ');
					byte[] srcdata = string.getBytes();
					System.arraycopy(srcdata, 0, data, 0, srcdata.length);
				}
				else {
					data = string.getBytes();
				}
				this.string = new ByteString(data);
			}
		}

		public int getStoredLength() {
			return string.getStoredLength();
		}

		public void retrieve(ByteBuffer bb) {
			string = new ByteString();
			string.retrieve(bb);
		}

		public void store(ByteBuffer bb) {
			string.store(bb);
		}
		
		public boolean isMaxKey() {
			return string.length() == 1 && string.get(0) == 0;
		}
		
		public int compareTo(IndexKey o) {
			StringKey sk = (StringKey) o;
			if (isMaxKey() || sk.isMaxKey()) {
				if (isMaxKey() && sk.isMaxKey()) {
					return 0;
				}
				else if (isMaxKey()) {
					return 1;
				}
				else {
					return -1;
				}
			}
			return string.compareTo(sk.string);
		}

        @Override
        public boolean equals(Object arg0) {
            return compareTo((IndexKey)arg0) == 0;
        }	
	}
	
	public static class StringKeyFactory implements IndexKeyFactory {

		public IndexKey newIndexKey(int id) {
			return new StringKey();
		}

		public IndexKey maxIndexKey(int id) {
			StringKey s = new StringKey();
			s.setBytes(new byte[1]);
			return s;
		}
	}
	
	/**
	 * A simple location.
	 */
	public static class RowLocation extends BaseLockable implements Location {

		protected RowLocation() {
			super((byte)'R');
		}

		int loc;

		public void parseString(String string) {
			loc = Integer.parseInt(string);
		}

		public void retrieve(ByteBuffer bb) {
			loc = bb.getInt();
		}

		public void store(ByteBuffer bb) {
			bb.putInt(loc);
		}

		public int getStoredLength() {
			return Integer.SIZE/Byte.SIZE;
		}

		public int compareTo(Location o) {
			RowLocation rl = (RowLocation) o;
			return loc - rl.loc;
		}

		@Override
		public boolean equals(Object o) {
			return compareTo((Location)o) == 0;
		}
		
		@Override
		public int hashCode() {
			return loc;
		}

		@Override
		public String toString() {
			return Integer.toString(loc);
		}
		
		public int getContainerId() {
			return loc;
		}
	}
	
	public static class RowLocationFactory implements LocationFactory {

		public Location newLocation() {
			return new RowLocation();
		}
		
	}

	private IndexItem generateKey(BTreeImpl btree, String s, int location, int childpage, boolean isLeaf) {
		StringKey key = (StringKey) btree.getNewIndexKey();
		key.setString(s);
		RowLocation loc = (RowLocation) btree.getNewLocation();
		loc.loc = location;
		IndexItem item = new IndexItem(key, loc, childpage, isLeaf, btree.isUnique());
		return item;
	}

	/**
	 * Initialize the test harness. New log is created, and the
	 * test container initialized. The container is allocated an extent of
	 * 64 pages which ought to be large enough for all the test cases.
	 */
	void doInitContainer() throws Exception {

		final BTreeDB db = new BTreeDB(true);
		
		try {
	    	Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
			db.spacemgr.createContainer(trx, "testctr.dat", 1, 1, 20, db.spmgr.getPageType());
			trx.commit();
		}
		finally {
			db.shutdown();
		}		
	}
	
	void doInitContainer2() throws Exception {
		final BTreeDB db = new BTreeDB(true);
		
		try {
	    	Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
	    	db.btreeMgr.createIndex(trx, "testctr.dat", 1, 20, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, true);
			trx.commit();
		}
		finally {
			db.shutdown();
		}		
		
	}
	
	void doLoadData(ScanResult[] results) throws Exception {

		final BTreeDB db = new BTreeDB(false);
		try {
			Index index = db.btreeMgr.getIndex(1);

			IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory
					.getInstance(TYPE_STRINGKEYFACTORY);
			LocationFactory locationFactory = (LocationFactory) db.objectFactory
					.getInstance(TYPE_ROWLOCATIONFACTORY);

			for (int i = 0; i < results.length; i++) {
				IndexKey key = keyFactory.newIndexKey(1);
				key.parseString(results[i].getKey());
				Location location = locationFactory.newLocation();
				location.parseString(results[i].getLocation());

				Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
				index.insert(trx, key, location);
				trx.commit();
			}
		} finally {
			db.shutdown();
		}
	}

	void doLoadData(String filename) throws Exception {

		BufferedReader reader = new BufferedReader(new InputStreamReader(ClassUtils.getResourceAsStream(filename)));
		final BTreeDB db = new BTreeDB(false);
		try {
			Index index = db.btreeMgr.getIndex(1);

			IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory
					.getInstance(TYPE_STRINGKEYFACTORY);
			LocationFactory locationFactory = (LocationFactory) db.objectFactory
					.getInstance(TYPE_ROWLOCATIONFACTORY);

			String line = reader.readLine();
			while (line != null) {
				StringTokenizer st = new StringTokenizer(line, " ");
				
				IndexKey key = keyFactory.newIndexKey(1);
				Location location = locationFactory.newLocation();
				String k = st.nextToken();
				key.parseString(k);
				String l = st.nextToken();
				location.parseString(l);

				Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
				// System.out.println("Inserting (" + key + ", " + location + ")");
				index.insert(trx, key, location);
				trx.commit();
				line = reader.readLine();
			}
		} finally {
			reader.close();
			db.shutdown();
		}
	}
	
	
	/**
	 * Initialize data pages by loading data from specified XML resource.
	 */
	public void doLoadXml(boolean testUnique, String dataFile) throws Exception {

		final BTreeDB db = new BTreeDB(false);
		
		try {
			BTreeIndexManagerImpl.XMLLoader loader = new BTreeIndexManagerImpl.XMLLoader(db.btreeMgr);
			Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
			try {
				loader.parseResource(dataFile);
				for (LoadPageOperation loadPageOp : loader.getPageOperations()) {
					final PageId pageid = loadPageOp.getPageId();
					final BTreeIndexManagerImpl btreemgr = db.btreeMgr;
					BufferAccessBlock bab = btreemgr.bufmgr.fixExclusive(
							pageid, false, -1, 0);
					try {
						PageId spaceMapPageId = new PageId(pageid
								.getContainerId(), loadPageOp
								.getSpaceMapPageNumber());
						BufferAccessBlock smpBab = btreemgr.bufmgr
								.fixExclusive(spaceMapPageId, false, -1, 0);
						try {
							Lsn lsn = trx.logInsert(bab.getPage(), loadPageOp);
							btreemgr.redo(bab.getPage(), loadPageOp);
							bab.setDirty(lsn);
							btreemgr.redo(smpBab.getPage(), loadPageOp);
							smpBab.setDirty(lsn);
						} finally {
							smpBab.unfix();
						}
					} finally {
						bab.unfix();
					}
				}
			}
			finally {
				// Doesn't matter whether we commit or abort as the changes are
				// logged as redo-only.
				trx.abort();
			}
		}
		finally {
			db.shutdown();
		}		
	}

	/**
	 * Initialize data pages by loading data from specified XML resource.
	 */
	public void doValidateTree(String dataFile) throws Exception {

		final BTreeDB db = new BTreeDB(false);
		try {
			doValidateTree(db, dataFile);
		}
		finally {
			db.shutdown();
		}		
	}

	/**
	 * Initialize data pages by loading data from specified XML resource.
	 */
	public void doValidateTree(final BTreeDB db, String dataFile)
			throws Exception {

		BTreeIndexManagerImpl.XMLLoader loader = new BTreeIndexManagerImpl.XMLLoader(
				db.btreeMgr);
		loader.parseResource(dataFile);
		for (LoadPageOperation loadPageOp : loader.getPageOperations()) {
			final PageId pageid = loadPageOp.getPageId();
			final BTreeIndexManagerImpl btreemgr = db.btreeMgr;
			BufferAccessBlock bab = btreemgr.bufmgr.fixExclusive(pageid, false,
					-1, 0);
			try {
				/*
				 * Log record is being applied to BTree page.
				 */
				SlottedPage r = (SlottedPage) bab.getPage();
				BTreeNode node = new BTreeNode(loadPageOp);
				node.wrap(r);
				//System.out.println("Validating page ->");
				// node.dumpAsXml();
				//System.out.println("------------------");
				assertEquals(node.header.keyCount, loadPageOp.items.size());
				assertEquals(node.header.leftSibling, loadPageOp.leftSibling);
				assertEquals(node.header.rightSibling, loadPageOp.rightSibling);
				assertEquals(node.header.keyFactoryType, loadPageOp
						.getKeyFactoryType());
				assertEquals(node.header.locationFactoryType, loadPageOp
						.getLocationFactoryType());
				assertEquals(node.isLeaf(), loadPageOp.isLeaf());
				assertEquals(node.isUnique(), loadPageOp.isUnique());
				for (int k = 1, i = 0; k < r.getNumberOfSlots(); k++, i++) {
					if (r.isSlotDeleted(k)) {
						continue;
					}
					IndexItem item = loadPageOp.items.get(i);
					IndexItem item1 = node.getItem(k);
					assertEquals(item, item1);
				}
			} finally {
				bab.unfix();
			}
		}
	}
	
	
	
	/*
	 * Splits page 2 into 2 and 3.
	 */
	void doPageSplit(boolean testLeaf, boolean testUnique) throws Exception {
		/* Create the write ahead log */
		final BTreeDB db = new BTreeDB(false);
		
		try {
			final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testUnique);
			
			int pageNumber = 2;
			Transaction trx;
			
			BTreeCursor bcursor = new BTreeCursor();
			bcursor.setQ(db.bufmgr.fixForUpdate(new PageId(1, pageNumber), 0));
			try {
		    	trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
		    	boolean okay = false;
		    	try {
			    	//System.out.println("--> SPLITTING PAGE");
					bcursor.searchKey = generateKey(btree, "da", 5, -1, testLeaf);
		    		btree.doSplit(trx, bcursor);
		    	}
		    	finally {
		    		if (okay)
		    			trx.commit();
		    		else {
		    			trx.abort();
		    		}
		    	}
			}
			finally {
				bcursor.unfixQ();
			}
		}
		finally {
			db.shutdown();
		}		
	}
	
	/*
	 * Merges previously split pages 2 and 3.
	 */
	void doRestartAndMerge(boolean testLeaf, boolean testUnique, int l, int r) throws Exception {
		final BTreeDB db = new BTreeDB(false);

		try {
			final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testUnique);

	    	//System.out.println("--> BEFORE MERGE");
	    	BufferAccessBlock bab = db.bufmgr.fixShared(new PageId(1, l), 0);
	    	try {
	    		BTreeNode node = btree.getBTreeNode();
	    		node.wrap((SlottedPage) bab.getPage());
	    		node.dump();
	    	}
	    	finally {
	    		bab.unfix();
	    	}
	    	bab = db.bufmgr.fixShared(new PageId(1, r), 0);
	    	try {
	    		BTreeNode node = btree.getBTreeNode();
	    		node.wrap((SlottedPage) bab.getPage());
	    		node.dump();
	    	}
	    	finally {
	    		bab.unfix();
	    	}	    	

	    	BTreeCursor bcursor = new BTreeCursor();
			bcursor.setQ(db.bufmgr.fixForUpdate(new PageId(1, l), 0));
			bcursor.setR(db.bufmgr.fixForUpdate(new PageId(1, r), 0));
			try {
		    	Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
		    	boolean okay = false;
		    	try {
			    	//System.out.println("--> MERGING PAGE");
		    		btree.doMerge(trx, bcursor);
		    	}
		    	finally {
		    		if (okay)
		    			trx.commit();
		    		else {
		    			trx.abort();
		    		}
		    	}
			}
			finally {
				bcursor.unfixQ();
			}
		}
		finally {
			db.shutdown();
		}		
	}

	/*
	 * Merges previously split pages 2 and 3.
	 */
	void doRestart() throws Exception {
		final BTreeDB db = new BTreeDB(false);
		try {
	    	System.out.println("RESTART PROCESSING COMPLETED");
		}
		finally {
			db.shutdown();
		}		
	}
	
	
	void doRestartAndLink(boolean testLeaf, boolean testUnique) throws Exception {
		final BTreeDB db = new BTreeDB(false);
		try {
			final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testUnique);

			//System.out.println("--> BEFORE LINKING CHILD TO PARENT");
	    	BufferAccessBlock bab = db.bufmgr.fixShared(new PageId(1, 2), 0);
	    	try {
	    		BTreeNode node = btree.getBTreeNode();
	    		node.wrap((SlottedPage) bab.getPage());
	    		node.dump();
	    	}
	    	finally {
	    		bab.unfix();
	    	}
	    	bab = db.bufmgr.fixShared(new PageId(1, 3), 0);
	    	try {
	    		BTreeNode node = btree.getBTreeNode();
	    		node.wrap((SlottedPage) bab.getPage());
	    		node.dump();
	    	}
	    	finally {
	    		bab.unfix();
	    	}	    	

	    	// prepareParentPage(pageFactory, bufmgr, btree, 1, 4, testUnique, 2);
	    	
	    	BTreeCursor bcursor = new BTreeCursor();
			bcursor.setQ(db.bufmgr.fixForUpdate(new PageId(1, 2), 0));
			bcursor.setR(db.bufmgr.fixForUpdate(new PageId(1, 3), 0));
			bcursor.setP(db.bufmgr.fixForUpdate(new PageId(1, 4), 0));
			try {
		    	Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
		    	boolean okay = false;
		    	try {
			    	//System.out.println("--> LINKING CHILD TO PARENT");
		    		btree.doLink(trx, bcursor);
		    	}
		    	finally {
		    		if (okay)
		    			trx.commit();
		    		else {
		    			trx.abort();
		    		}
		    	}
			}
			finally {
				bcursor.unfixQ();
				bcursor.unfixR();
				bcursor.unfixP();
			}

			bcursor.setQ(db.bufmgr.fixForUpdate(new PageId(1, 2), 0));
			bcursor.setR(db.bufmgr.fixForUpdate(new PageId(1, 3), 0));
			bcursor.setP(db.bufmgr.fixForUpdate(new PageId(1, 4), 0));
			try {
		    	Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
		    	boolean okay = false;
		    	try {
			    	//System.out.println("--> DE-LINKING CHILD FROM PARENT");
		    		btree.doUnlink(trx, bcursor);
		    	}
		    	finally {
		    		if (okay)
		    			trx.commit();
		    		else {
		    			trx.abort();
		    		}
		    	}
			}
			finally {
				bcursor.unfixQ();
				bcursor.unfixR();
			}
		
		}
		finally {
			db.shutdown();
		}		
	}

	void doRestartLink(boolean testLeaf, boolean testUnique) throws Exception {
		final BTreeDB db = new BTreeDB(false);
		try {
			final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testUnique);

			//System.out.println("--> BEFORE LINKING CHILD TO PARENT");
	    	BufferAccessBlock bab = db.bufmgr.fixShared(new PageId(1, 2), 0);
	    	try {
	    		BTreeNode node = btree.getBTreeNode();
	    		node.wrap((SlottedPage) bab.getPage());
	    		node.dump();
	    	}
	    	finally {
	    		bab.unfix();
	    	}
	    	bab = db.bufmgr.fixShared(new PageId(1, 3), 0);
	    	try {
	    		BTreeNode node = btree.getBTreeNode();
	    		node.wrap((SlottedPage) bab.getPage());
	    		node.dump();
	    	}
	    	finally {
	    		bab.unfix();
	    	}	    	

	    	// prepareParentPage(pageFactory, bufmgr, btree, 1, 4, testUnique, 2);
	    	
	    	BTreeCursor bcursor = new BTreeCursor();
			bcursor.setQ(db.bufmgr.fixForUpdate(new PageId(1, 2), 0));
			bcursor.setR(db.bufmgr.fixForUpdate(new PageId(1, 3), 0));
			bcursor.setP(db.bufmgr.fixForUpdate(new PageId(1, 4), 0));
			try {
		    	Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
		    	boolean okay = false;
		    	try {
			    	//System.out.println("--> LINKING CHILD TO PARENT");
		    		btree.doLink(trx, bcursor);
		    	}
		    	finally {
		    		if (okay)
		    			trx.commit();
		    		else {
		    			trx.abort();
		    		}
		    	}
			}
			finally {
				bcursor.unfixQ();
				bcursor.unfixR();
				bcursor.unfixP();
			}
		}
		finally {
			db.shutdown();
		}		
	}

	void doRestartDelink(boolean testLeaf, boolean testUnique) throws Exception {
		final BTreeDB db = new BTreeDB(false);
		try {
			final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testUnique);

	    	BTreeCursor bcursor = new BTreeCursor();
			bcursor.setQ(db.bufmgr.fixForUpdate(new PageId(1, 2), 0));
			bcursor.setR(db.bufmgr.fixForUpdate(new PageId(1, 3), 0));
			bcursor.setP(db.bufmgr.fixForUpdate(new PageId(1, 4), 0));
			try {
		    	Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
		    	boolean okay = false;
		    	try {
			    	//System.out.println("--> DE-LINKING CHILD FROM PARENT");
		    		btree.doUnlink(trx, bcursor);
		    	}
		    	finally {
		    		if (okay)
		    			trx.commit();
		    		else {
		    			trx.abort();
		    		}
		    	}
			}
			finally {
				bcursor.unfixQ();
				bcursor.unfixR();
			}
		
		}
		finally {
			db.shutdown();
		}		
	}
	
	
	void doRestartAndUnlink(boolean testLeaf, boolean testUnique, int p, int q, int r) throws Exception {
		final BTreeDB db = new BTreeDB(false);
		try {
			final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testUnique);

	    	BTreeCursor bcursor = new BTreeCursor();
			bcursor.setQ(db.bufmgr.fixForUpdate(new PageId(1, q), 0));
			bcursor.setR(db.bufmgr.fixForUpdate(new PageId(1, r), 0));
			bcursor.setP(db.bufmgr.fixForUpdate(new PageId(1, p), 0));
			try {
		    	Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
		    	boolean okay = false;
		    	try {
			    	//System.out.println("--> DE-LINKING CHILD FROM PARENT");
		    		btree.doUnlink(trx, bcursor);
		    	}
		    	finally {
		    		if (okay)
		    			trx.commit();
		    		else {
		    			trx.abort();
		    		}
		    	}
			}
			finally {
				bcursor.unfixQ();
				bcursor.unfixR();
			}
		}
		finally {
			db.shutdown();
		}		
	}
	
	
	void doRestartAndRedistribute(boolean testLeaf, boolean testUnique) throws Exception {
		final BTreeDB db = new BTreeDB(false);
		try {
			final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testUnique);

	    	//System.out.println("--> BEFORE REDISTRIBUTING KEYS");
	    	BufferAccessBlock bab = db.bufmgr.fixShared(new PageId(1, 2), 0);
	    	try {
	    		BTreeNode node = btree.getBTreeNode();
	    		node.wrap((SlottedPage) bab.getPage());
	    		node.dump();
	    	}
	    	finally {
	    		bab.unfix();
	    	}
	    	bab = db.bufmgr.fixShared(new PageId(1, 3), 0);
	    	try {
	    		BTreeNode node = btree.getBTreeNode();
	    		node.wrap((SlottedPage) bab.getPage());
	    		node.dump();
	    	}
	    	finally {
	    		bab.unfix();
	    	}	    	

	    	BTreeCursor bcursor = new BTreeCursor();
			bcursor.setQ(db.bufmgr.fixForUpdate(new PageId(1, 2), 0));
			bcursor.setR(db.bufmgr.fixForUpdate(new PageId(1, 3), 0));
			try {
		    	Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
		    	boolean okay = false;
		    	try {
					bcursor.searchKey = generateKey(btree, "da", 5, -1, testLeaf);
			    	//System.out.println("--> REDISTRIBUTING KEYS");
		    		btree.doRedistribute(trx, bcursor);
		    	}
		    	finally {
		    		if (okay)
		    			trx.commit();
		    		else {
		    			trx.abort();
		    		}
		    	}
			}
			finally {
				bcursor.unfixQ();
			}
		}
		finally {
			db.shutdown();
		}		
	}

	void doRestartAndIncreaseTreeHeight(boolean testLeaf, boolean testUnique) throws Exception {
		final BTreeDB db = new BTreeDB(false);
		try {
			final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testUnique);

	    	//System.out.println("--> BEFORE INCREASING TREE HEIGHT");
	    	BufferAccessBlock bab = db.bufmgr.fixShared(new PageId(1, 2), 0);
	    	try {
	    		BTreeNode node = btree.getBTreeNode();
	    		node.wrap((SlottedPage) bab.getPage());
	    		node.dump();
	    	}
	    	finally {
	    		bab.unfix();
	    	}
	    	bab = db.bufmgr.fixShared(new PageId(1, 3), 0);
	    	try {
	    		BTreeNode node = btree.getBTreeNode();
	    		node.wrap((SlottedPage) bab.getPage());
	    		node.dump();
	    	}
	    	finally {
	    		bab.unfix();
	    	}	    	

	    	BTreeCursor bcursor = new BTreeCursor();
			bcursor.setQ(db.bufmgr.fixForUpdate(new PageId(1, 2), 0));
			bcursor.setR(db.bufmgr.fixForUpdate(new PageId(1, 3), 0));
			try {
		    	Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
		    	boolean okay = false;
		    	try {
					bcursor.searchKey = generateKey(btree, "da", 5, -1, testLeaf);
			    	//System.out.println("--> INCREASING TREE HEIGHT");
		    		btree.doIncreaseTreeHeight(trx, bcursor);
		    	}
		    	finally {
		    		if (okay)
		    			trx.commit();
		    		else {
		    			trx.abort();
		    		}
		    	}
			}
			finally {
				bcursor.unfixQ();
			}
		}
		finally {
			db.shutdown();
		}		
	}

	void doRestartAndDecreaseTreeHeight(boolean testLeaf, boolean testUnique, int p, int q) throws Exception {
		final BTreeDB db = new BTreeDB(false);
		try {
			final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testUnique);

	    	//System.out.println("--> BEFORE DECREASING TREE HEIGHT");
	    	BufferAccessBlock bab = db.bufmgr.fixShared(new PageId(1, p), 0);
	    	try {
	    		BTreeNode node = btree.getBTreeNode();
	    		node.wrap((SlottedPage) bab.getPage());
	    		node.dump();
	    	}
	    	finally {
	    		bab.unfix();
	    	}
	    	bab = db.bufmgr.fixShared(new PageId(1, q), 0);
	    	try {
	    		BTreeNode node = btree.getBTreeNode();
	    		node.wrap((SlottedPage) bab.getPage());
	    		node.dump();
	    	}
	    	finally {
	    		bab.unfix();
	    	}	    	

	    	BTreeCursor bcursor = new BTreeCursor();
			bcursor.setP(db.bufmgr.fixForUpdate(new PageId(1, p), 0));
			bcursor.setQ(db.bufmgr.fixForUpdate(new PageId(1, q), 0));
			try {
		    	Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
		    	boolean okay = false;
		    	try {
					bcursor.searchKey = generateKey(btree, "da", 5, -1, testLeaf);
			    	//System.out.println("--> DECREASING TREE HEIGHT");
		    		btree.doDecreaseTreeHeight(trx, bcursor);
		    	}
		    	finally {
		    		if (okay)
		    			trx.commit();
		    		else {
		    			trx.abort();
		    		}
		    	}
			}
			finally {
				bcursor.unfixP();
			}
		}
		finally {
			db.shutdown();
		}		
	}

	void doSingleInsert(boolean testUnique, boolean commit, String k, String loc) throws Exception {
		doSingleInsert(testUnique, commit, k, loc, null, null);
	}
	void doSingleInsert(boolean testUnique, boolean commit, String k, String loc, String commitresult, String abortresult) throws Exception {
		final BTreeDB db = new BTreeDB(false);
		try {
			final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testUnique);

			IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory.getInstance(TYPE_STRINGKEYFACTORY);
			LocationFactory locationFactory = (LocationFactory) db.objectFactory.getInstance(TYPE_ROWLOCATIONFACTORY);
			IndexKey key = keyFactory.newIndexKey(1);
			key.parseString(k);
			Location location = locationFactory.newLocation();
			location.parseString(loc);
			Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
			boolean okay = false;
			try {
				//System.out.println("--> INSERTING KEY");
				btree.insert(trx, key, location);
				if (commitresult != null) {
					doValidateTree(db, commitresult);
				}
				okay = true;
			} finally {
				if (okay && commit) {
					//System.out.println("--> COMMITTING INSERT");
					trx.commit();
					if (commitresult != null) {
						doValidateTree(db, commitresult);
					}
				}
				else {
					//System.out.println("--> ABORTING INSERT");
					trx.abort();
					if (abortresult != null) {
						doValidateTree(db, abortresult);
					}
				}
			}
		} finally {
			db.shutdown();
		}		
	}

	void doDoubleInsert(boolean testUnique, boolean commit1, String k1, String loc1, 
			boolean commit2, String k2, String loc2, String result1, String result2, String result3) throws Exception {
		final BTreeDB db = new BTreeDB(false);
		try {
			final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testUnique);

			IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory.getInstance(TYPE_STRINGKEYFACTORY);
			LocationFactory locationFactory = (LocationFactory) db.objectFactory.getInstance(TYPE_ROWLOCATIONFACTORY);
			
			IndexKey key1 = keyFactory.newIndexKey(1);
			key1.parseString(k1);
			Location location1 = locationFactory.newLocation();
			location1.parseString(loc1);
			Transaction trx1 = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
			boolean okay1 = false;
			try {
				//System.out.println("--> INSERTING KEY 1");
				btree.insert(trx1, key1, location1);
				doValidateTree(db, result1);
				okay1 = true;
				
				IndexKey key2 = keyFactory.newIndexKey(1);
				key2.parseString(k2);
				Location location2 = locationFactory.newLocation();
				location2.parseString(loc2);
				Transaction trx2 = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
				boolean okay2 = false;

				try {
					//System.out.println("--> INSERTING KEY 2");
					btree.insert(trx2, key2, location2);
					doValidateTree(db, result2);
					okay2 = true;
				}
				finally {
					if (okay2 && commit2) {
						//System.out.println("--> COMMITTING KEY 2");
						trx2.commit();
					}
					else {
						//System.out.println("--> ABORTING KEY 2");
						trx2.abort();
					}
				}
			} finally {
				if (okay1 && commit1) {
					//System.out.println("--> COMMITTING KEY 1");
					trx1.commit();
				}
				else {
					//System.out.println("--> ABORTING KEY 1");
					trx1.abort();
					doValidateTree(db, result3);
				}
			}
		} finally {
			db.shutdown();
		}		
	}

	void doSingleDelete(boolean testUnique, boolean commit, String k, String loc) throws Exception {
		final BTreeDB db = new BTreeDB(false);
		try {
			final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testUnique);

			IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory.getInstance(TYPE_STRINGKEYFACTORY);
			LocationFactory locationFactory = (LocationFactory) db.objectFactory.getInstance(TYPE_ROWLOCATIONFACTORY);
			IndexKey key = keyFactory.newIndexKey(1);
			key.parseString(k);
			Location location = locationFactory.newLocation();
			location.parseString(loc);
			Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
			boolean okay = false;
			try {
				//System.out.println("--> DELETING KEY");
				btree.delete(trx, key, location);
				okay = true;
			} finally {
				if (okay && commit)
					trx.commit();
				else {
					trx.abort();
				}
			}
		} finally {
			db.shutdown();
		}		
	}

	boolean t1Failed = false;
	boolean t2Failed = false;

	/**
	 * In this test, a delete is started on one thread, and on aother thread, a concurrent
	 * insert is started. The test should have following behaviour.
	 * The insert should wait for the outcome of the delete.
	 * If the delete commits, the insert should successfully proceed.
	 * If the delete aborts, the insert should fail with unique constraint violation error.
	 */
	void doDeleteInsertThreads(final boolean testUnique, final boolean commitDelete, final String k, final String loc, final String deleteResult, final String insertResult) throws Exception {
		final BTreeDB db = new BTreeDB(false);
    	final boolean testingUniqueIndex = testUnique;
		try {
			final Thread t1 = new Thread(new Runnable() {
				public void run() {
					try {
						final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testingUniqueIndex);
						IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory.getInstance(TYPE_STRINGKEYFACTORY);
						LocationFactory locationFactory = (LocationFactory) db.objectFactory.getInstance(TYPE_ROWLOCATIONFACTORY);
						IndexKey key = keyFactory.newIndexKey(1);
						key.parseString(k);
						Location location = locationFactory.newLocation();
						location.parseString(loc);
						Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
						boolean okay = false;
						try {
							//System.out.println("--> DELETING KEY");
							btree.delete(trx, key, location);
							if (deleteResult != null)
								doValidateTree(db, deleteResult);
							Thread.sleep(2000);
							okay = true;
						} finally {
							if (okay && commitDelete)
								trx.commit();
							else {
								trx.abort();
							}
							t1Failed = false;
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}, "T1");

			final Thread t2 = new Thread(new Runnable() {
				public void run() {
					try {
						final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testingUniqueIndex);
						IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory.getInstance(TYPE_STRINGKEYFACTORY);
						LocationFactory locationFactory = (LocationFactory) db.objectFactory.getInstance(TYPE_ROWLOCATIONFACTORY);
						IndexKey key = keyFactory.newIndexKey(1);
						key.parseString(k);
						Location location = locationFactory.newLocation();
						location.parseString(loc);
						Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
						boolean okay = false;
						try {
							//System.out.println("--> INSERTING KEY");
							btree.insert(trx, key, location);
							if (insertResult != null)
								doValidateTree(db, insertResult);
							okay = true;
							if (commitDelete) {
								//System.out.println("Setting t2Failed to false");
								t2Failed = false;
							}
						} finally {
							if (okay /*&& commit */) {
								trx.commit();
							}
							else {
								trx.abort();
							}
						}
					} catch (Exception e) {
						if (!commitDelete && e instanceof UniqueConstraintViolationException) {
							t2Failed = false;
						}
						e.printStackTrace();
					}
				}
			}, "T2");
			
			t1Failed = true;
			t2Failed = true;
			t1.start();
			Thread.sleep(1000);
			t2.start();
			t1.join();
			t2.join();
			assertTrue(!t1.isAlive());
			assertTrue(!t2.isAlive());
			assertTrue(!t2Failed);
		} finally {
			db.shutdown();
		}		
	}

    void doScanAndDelete(boolean testUnique, boolean commit, String k, String loc) throws Exception {
		final BTreeDB db = new BTreeDB(false);
        try {
            final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testUnique);

            IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory.getInstance(TYPE_STRINGKEYFACTORY);
            LocationFactory locationFactory = (LocationFactory) db.objectFactory.getInstance(TYPE_ROWLOCATIONFACTORY);
            IndexKey key = keyFactory.newIndexKey(1);
            key.parseString(k);
            Location location = locationFactory.newLocation();
            location.parseString(loc);
            Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
            boolean okay = false;
            IndexScan scan = btree.openScan(trx, key, location, true);
            try {
                //System.out.println("--> SCANNING TREE");
                while (scan.fetchNext()) {
                	//System.out.println("new ScanResult(\"" + scan.getCurrentKey() + "\", \"" + scan.getCurrentLocation() + "\"),");
                	// System.out.println("SCAN=" + scan.getCurrentKey() + "," + scan.getCurrentLocation());
                	trx.acquireLock(scan.getCurrentLocation(), LockMode.EXCLUSIVE, LockDuration.COMMIT_DURATION);
                	if (scan.isEof()) {
                		break;
                	}
                	//System.out.println("DELETING=" + scan.getCurrentKey() + "," + scan.getCurrentLocation());
                	btree.delete(trx, scan.getCurrentKey(), scan.getCurrentLocation());
                	scan.fetchCompleted(true);
                }
            } finally {
                scan.close();
                if (!doCrashTesting) {
					if (okay && commit)
						trx.commit();
					else {
						trx.abort();
					}
				}
            }
        } finally {
			db.shutdown();
        }
    }
	

    class ScanResult {
    	String key;
    	String location;
    	public ScanResult(String key, String location) {
    		this.key = key;
    		this.location = location;
    	}
		public String getKey() {
			return key;
		}
		public void setKey(String key) {
			this.key = key;
		}
		public String getLocation() {
			return location;
		}
		public void setLocation(String location) {
			this.location = location;
		}
    }
    
    /**
     * Scans the tree from a starting key to the eof.
     */
    void doScanTree(boolean testUnique, boolean commit, String k, String loc, ScanResult[] result) throws Exception {
		final BTreeDB db = new BTreeDB(false);
        try {
            final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testUnique);

            IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory.getInstance(TYPE_STRINGKEYFACTORY);
            LocationFactory locationFactory = (LocationFactory) db.objectFactory.getInstance(TYPE_ROWLOCATIONFACTORY);
            IndexKey key = keyFactory.newIndexKey(1);
            key.parseString(k);
            Location location = locationFactory.newLocation();
            location.parseString(loc);
            Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
            boolean okay = false;
            IndexScan scan = btree.openScan(trx, key, location, false);
            try {
                //System.out.println("--> SCANNING TREE {");
                int i = 0;
                while (scan.fetchNext()) {
                	if (result != null) {
                		assertEquals(result[i].getKey(), scan.getCurrentKey().toString());
                		assertEquals(result[i].getLocation(), scan.getCurrentLocation().toString());
                		i++;
                	}
                	scan.fetchCompleted(true);
                	//System.out.println("new ScanResult(\"" + scan.getCurrentKey() + "\", \"" + scan.getCurrentLocation() + "\"),");
                }
            } finally {
                scan.close();
                if (okay && commit)
                    trx.commit();
                else {
                    trx.abort();
                }
            }
        } finally {
			db.shutdown();
        }
    }

    /**
     * Scans the tree from a starting key to the eof.
     */
    void doScanTree(String filename) throws Exception {
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				ClassUtils.getResourceAsStream(filename)));
		final BTreeDB db = new BTreeDB(false);
		try {
			Index index = db.btreeMgr.getIndex(1);

			IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory
					.getInstance(TYPE_STRINGKEYFACTORY);
			LocationFactory locationFactory = (LocationFactory) db.objectFactory
					.getInstance(TYPE_ROWLOCATIONFACTORY);

			String line = reader.readLine();
			IndexScan scan = null;
			Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
			try {
				while (line != null) {
					StringTokenizer st = new StringTokenizer(line, " ");

					IndexKey key = keyFactory.newIndexKey(1);
					Location location = locationFactory.newLocation();
					String k = st.nextToken();
					key.parseString(k);
					String l = st.nextToken();
					location.parseString(l);

					if (scan == null) {
						scan = index.openScan(trx, key, location, false);
					}
					if (scan.fetchNext()) {
	                	// System.out.println("new ScanResult(\"" + scan.getCurrentKey() + "\", \"" + scan.getCurrentLocation() + "\"),");
						assertEquals(key.toString(), scan.getCurrentKey().toString());
						assertEquals(location.toString(), scan.getCurrentLocation()
								.toString());
						scan.fetchCompleted(true);
					} 
					else {
						fail("Scan for next key failed at (" + key + ", " + location + ")");
					}
					line = reader.readLine();
				}
			} finally {
				if (scan != null) {
					scan.close();
				}
				trx.abort();
			}
		} finally {
			reader.close();
			db.shutdown();
		}
	}

    /**
     * Scans the tree from a starting key to the eof.
     */
    void doFindInTree(String filename) throws Exception {
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				ClassUtils.getResourceAsStream(filename)));
		final BTreeDB db = new BTreeDB(false);
		try {
			Index index = db.btreeMgr.getIndex(1);

			IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory
					.getInstance(TYPE_STRINGKEYFACTORY);
			LocationFactory locationFactory = (LocationFactory) db.objectFactory
					.getInstance(TYPE_ROWLOCATIONFACTORY);

			String line = reader.readLine();
			Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
			try {
				while (line != null) {
					StringTokenizer st = new StringTokenizer(line, " ");

					IndexKey key = keyFactory.newIndexKey(1);
					Location location = locationFactory.newLocation();
					String k = st.nextToken();
					key.parseString(k);
					String l = st.nextToken();
					location.parseString(l);

					IndexScan scan = index.openScan(trx, key, location, false);
					if (scan.fetchNext()) {
	                	// System.out.println("new FindResult(\"" + scan.getCurrentKey() + "\", \"" + scan.getCurrentLocation() + "\"),");
						assertEquals(key.toString(), scan.getCurrentKey().toString());
						assertEquals(location.toString(), scan.getCurrentLocation()
								.toString());
						scan.fetchCompleted(true);
					} 
					else {
						fail("Find failed for (" + key + ", " + location + ")");
					}
					scan.close();
					line = reader.readLine();
				}
			} finally {
				trx.abort();
			}
		} finally {
			reader.close();
			db.shutdown();
		}
	}

    
    /**
	 * Starts two threads. First thread starts a delete on a key and then goes
	 * to sleep. Second thread scans the tree, and blocks when it reaches the
	 * deleted key. The delete thread resumes and commits the delete. This lets
	 * the scan thread continue and finish the scan.
	 */
    void doDeleteAndScanThreads(final boolean testUnique, final boolean commit, final String k, final String loc, final ScanResult[] result) throws Exception {
		final BTreeDB db = new BTreeDB(false);
        final boolean testingUniqueIndex = testUnique;
        final Lock lock = new ReentrantLock();
        final Condition deleted = lock.newCondition();
        final Condition lockWaitStarted = lock.newCondition();
        final LockEventListener listener = new LockEventListener() {
			public void beforeLockWait(Object owner, Object lockable, LockMode mode) {
				//System.out.println("LOCK WAIT STARTED");
				lock.lock();
				lockWaitStarted.signal();
				lock.unlock();
			}
        };
        try {
            Thread t1 = new Thread(new Runnable() {
                public void run() {
                    try {
                        final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testingUniqueIndex);
                        IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory.getInstance(TYPE_STRINGKEYFACTORY);
                        LocationFactory locationFactory = (LocationFactory) db.objectFactory.getInstance(TYPE_ROWLOCATIONFACTORY);
                        IndexKey key = keyFactory.newIndexKey(1);
                        key.parseString(k);
                        Location location = locationFactory.newLocation();
                        location.parseString(loc);
                        Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
                        boolean okay = false;
                        try {
                            //System.out.println("--> DELETING KEY [" + k + "," + loc + "]");
                            btree.delete(trx, key, location);
                            lock.lock();
                            deleted.signal();
                            //System.out.println("Awaiting lockWaitStarted");
                            lockWaitStarted.await();
                            //System.out.println("Received lockWaitStarted signal");
                            lock.unlock();
                            //Thread.sleep(5000);
                            okay = true;
                        } finally {
                            if (okay && commit) {
                                //System.out.println("--> COMMITTING DELETE OF KEY [" + k + "," + loc + "]");
                                trx.commit();
                            }
                            else {
                                //System.out.println("--> ABORTING DELETE OF KEY [" + k + "," + loc + "]");
                                trx.abort();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        t1Failed = true;
                    }
                }
            }, "T1");

            Thread t2 = new Thread(new Runnable() {
                public void run() {
                    try {
                        final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testingUniqueIndex);
                        IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory.getInstance(TYPE_STRINGKEYFACTORY);
                        LocationFactory locationFactory = (LocationFactory) db.objectFactory.getInstance(TYPE_ROWLOCATIONFACTORY);
                        IndexKey key = keyFactory.newIndexKey(1);
                        key.parseString("a1");
                        Location location = locationFactory.newLocation();
                        location.parseString("10");
                        Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
                        boolean okay = false;
                        IndexScan scan = btree.openScan(trx, key, location, false);
                        db.lockmgr.addLockEventListener(listener);
                        try {
                            //System.out.println("--> SCANNING TREE");
                            int i = 0;
                            while (scan.fetchNext()) {
                                //System.out.println("SCAN=" + scan.getCurrentKey() + "," + scan.getCurrentLocation());
                            	//System.out.println("new ScanResult(\"" + scan.getCurrentKey() + "\", \"" + scan.getCurrentLocation() + "\"),");
                            	if (result != null) {
                            		assertEquals(result[i].getKey(), scan.getCurrentKey().toString());
                            		assertEquals(result[i].getLocation(), scan.getCurrentLocation().toString());
                            		i++;
                            	}
                            	scan.fetchCompleted(true);
                                if (scan.isEof()) {
                                    break;
                                }
                            }
                        } finally {
                        	db.lockmgr.clearLockEventListeners();
                            scan.close();
                            if (okay && commit)
                                trx.commit();
                            else {
                                trx.abort();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        t2Failed = true;
                    }
                }
            }, "T2");
            
            t1Failed = false;
            t2Failed = false;
            t1.start();
            lock.lock();
            deleted.await();
            lock.unlock();
            // System.out.println("PROCEEDING");
            // Thread.sleep(1000);
            t2.start();
            t1.join();
            t2.join();
            assertTrue(!t1.isAlive());
            assertTrue(!t2.isAlive());
            assertFalse(t1Failed);
            assertFalse(t2Failed);
        } finally {
			db.shutdown();
        }       
    }

    void doScanAndDeleteThreads(final boolean testUnique, final boolean commit, final String k, final String loc, final ScanResult[] result) throws Exception {
		final BTreeDB db = new BTreeDB(false);
        final boolean testingUniqueIndex = testUnique;
        final Lock lock = new ReentrantLock();
        final Condition lockWaitStarted = lock.newCondition();
        final LockEventListener listener = new LockEventListener() {
			public void beforeLockWait(Object owner, Object lockable, LockMode mode) {
				//System.out.println("LOCK WAIT STARTED");
				lock.lock();
				lockWaitStarted.signal();
				lock.unlock();
			}
        };
        try {
            final Thread t1 = new Thread(new Runnable() {
                public void run() {
                    try {
                        final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testingUniqueIndex);
                        IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory.getInstance(TYPE_STRINGKEYFACTORY);
                        LocationFactory locationFactory = (LocationFactory) db.objectFactory.getInstance(TYPE_ROWLOCATIONFACTORY);
                        IndexKey key = keyFactory.newIndexKey(1);
                        key.parseString(k);
                        Location location = locationFactory.newLocation();
                        location.parseString(loc);
                        Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
                        boolean okay = false;
                        try {
                            //System.out.println("--> DELETING KEY [" + k + "," + loc + "]");
                            db.lockmgr.addLockEventListener(listener);
                            trx.acquireLock(location, LockMode.EXCLUSIVE, LockDuration.COMMIT_DURATION);
                            btree.delete(trx, key, location);
                            okay = true;
                        } finally {
                        	db.lockmgr.clearLockEventListeners();
                            if (okay && commit) {
                                //System.out.println("--> COMMITTING DELETE OF KEY [" + k + "," + loc + "]");
                                trx.commit();
                            }
                            else {
                                //System.out.println("--> ABORTING DELETE OF KEY [" + k + "," + loc + "]");
                                trx.abort();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        t1Failed = true;
                    }
                }
            }, "T1");

            Thread t2 = new Thread(new Runnable() {
                public void run() {
                    try {
                        final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testingUniqueIndex);
                        IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory.getInstance(TYPE_STRINGKEYFACTORY);
                        LocationFactory locationFactory = (LocationFactory) db.objectFactory.getInstance(TYPE_ROWLOCATIONFACTORY);
                        IndexKey key = keyFactory.newIndexKey(1);
                        key.parseString("a1");
                        Location location = locationFactory.newLocation();
                        location.parseString("10");
                        IndexKey delkey = keyFactory.newIndexKey(1);
                        delkey.parseString(k);
                        Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
                        boolean okay = false;
                        IndexScan scan = btree.openScan(trx, key, location, false);
                        try {
                            //System.out.println("--> SCANNING TREE");
                            int i = 0;
                            while (scan.fetchNext()) {
                            	//System.out.println("new ScanResult(\"" + scan.getCurrentKey() + "\", \"" + scan.getCurrentLocation() + "\"),");
//                                System.out.println("SCAN=" + scan.getCurrentKey() + "," + scan.getCurrentLocation());
//                                System.out.println("Comparing " + scan.getCurrentKey() + " with " + delkey);
                            	if (result != null) {
                            		assertEquals(result[i].getKey(), scan.getCurrentKey().toString());
                            		assertEquals(result[i].getLocation(), scan.getCurrentLocation().toString());
                            		i++;
                            	}
                                if (scan.getCurrentKey().equals(delkey)) {
                                	lock.lock();
                                	lockWaitStarted.await(15, TimeUnit.SECONDS);
                                	lock.unlock();
//                                    System.out.println("--> SCAN Sleeping for 15 seconds");
//                                    Thread.sleep(15000);
//                                    System.out.println("--> SCAN Sleep completed");
                                }
                                scan.fetchCompleted(true);
                                if (scan.isEof()) {
                                    break;
                                }
                            }
                        } finally {
                            scan.close();
                            if (okay && commit)
                                trx.commit();
                            else {
                                trx.abort();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        t2Failed = true;
                    }
                }
            }, "T2");
            
            t1Failed = false;
            t2Failed = false;
            t2.start();
            Thread.sleep(1000);
            t1.start();
            t1.join();
            t2.join();
            assertTrue(!t1.isAlive());
            assertTrue(!t2.isAlive());
        } finally {
			db.shutdown();
        }       
    }
    
    /**
     * Tests that when serialization mode is enabled, even if read unique 
     * for X fails, another transaction must wait for the reader to finish
     * before it can insert X.
     */
	void doReadUniqueX() throws Exception {
        final boolean testingUniqueIndex = true;
        final Lock lock = new ReentrantLock();
        final Condition lockWaitStarted = lock.newCondition();
        final AtomicInteger status = new AtomicInteger(0);
        final LockEventListener listener = new LockEventListener() {
			public void beforeLockWait(Object owner, Object lockable, LockMode mode) {
				//System.out.println("LOCK WAIT STARTED");
				lock.lock();
				lockWaitStarted.signal();
				status.incrementAndGet();
				lock.unlock();
			}
        };
		final BTreeDB db = new BTreeDB(false);
        final Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testingUniqueIndex);
                    IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory.getInstance(TYPE_STRINGKEYFACTORY);
                    LocationFactory locationFactory = (LocationFactory) db.objectFactory.getInstance(TYPE_ROWLOCATIONFACTORY);
                    IndexKey key = keyFactory.newIndexKey(1);
                    key.parseString("x");
                    Location location = locationFactory.newLocation();
                    location.parseString("2");
                    Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
                    boolean okay = false;
                    try {
                        IndexScan scan = btree.openScan(trx, key, location, false);
                        try {
                        	scan.fetchNext();
                        	lock.lock();
                        	lockWaitStarted.await(3, TimeUnit.SECONDS);
                        	lock.unlock();
                        	scan.fetchCompleted(true);
                        }
                        finally {
                        	scan.close();
                        }
                        okay = true;
                    } finally {
                        if (okay) {
                            trx.commit();
                        }
                        else {
                            trx.abort();
                        }
                    }
                } catch (Exception e) {
                	setThreadFailed(Thread.currentThread(), e);
                }
            }
        }, "T1");
        final Thread t2 = new Thread(new Runnable() {
            public void run() {
                try {
                    final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testingUniqueIndex);
                    IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory.getInstance(TYPE_STRINGKEYFACTORY);
                    LocationFactory locationFactory = (LocationFactory) db.objectFactory.getInstance(TYPE_ROWLOCATIONFACTORY);
                    IndexKey key = keyFactory.newIndexKey(1);
                    key.parseString("x");
                    Location location = locationFactory.newLocation();
                    location.parseString("2");
                    Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
                    boolean okay = false;
                    try {                       
                        try {
                            db.lockmgr.addLockEventListener(listener);
                            trx.acquireLock(location, LockMode.EXCLUSIVE, LockDuration.COMMIT_DURATION);
                            btree.insert(trx, key, location);
                        }
                        finally {
                        	db.lockmgr.clearLockEventListeners();
                        }
                        okay = true;
                    } finally {
                        if (okay) {
                            trx.commit();
                        }
                        else {
                            trx.abort();
                        }
                    }
                } catch (Exception e) {
                	setThreadFailed(Thread.currentThread(), e);
                }
            }
        }, "T2");
        try {
            t1.start();
            Thread.sleep(1000);
            t2.start();
            t1.join(10000);
            t2.join(10000);
            assertTrue(!t1.isAlive());
            assertTrue(!t2.isAlive());
            assertEquals(1, status.get());
            checkThreadFailures();
        } finally {
			db.shutdown();
        }       
	}

    /**
     * Tests the situation where a transaction needs to wait for the next key lock
     * during insert, and between the gap when it releases the latches and attempts 
     * an unconditional lock, another transaction inserts a key that changes the key range,
     * and forces the first transaction, after it obtains the lock, to restart.
     */
	void doReadUniqueX2() throws Exception {
        final boolean testingUniqueIndex = true;
        final Lock lock = new ReentrantLock();
        final Condition lockWaitStarted = lock.newCondition();
        final AtomicInteger status = new AtomicInteger(0);
        final LockEventListener listener = new LockEventListener() {
			public void beforeLockWait(Object owner, Object lockable, LockMode mode) {
				//System.out.println("LOCK WAIT STARTED");
				lock.lock();
				lockWaitStarted.signal();
				status.incrementAndGet();
				lock.unlock();
			}
        };
		final BTreeDB db = new BTreeDB(false);
        final Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testingUniqueIndex);
                    IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory.getInstance(TYPE_STRINGKEYFACTORY);
                    LocationFactory locationFactory = (LocationFactory) db.objectFactory.getInstance(TYPE_ROWLOCATIONFACTORY);
                    IndexKey key = keyFactory.newIndexKey(1);
                    key.parseString("x");
                    Location location = locationFactory.newLocation();
                    location.parseString("2");
                    Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
                    boolean okay = false;
                    try {
                        IndexScan scan = btree.openScan(trx, key, location, false);
                        try {
                        	scan.fetchNext();
                        	lock.lock();
                        	lockWaitStarted.await(3, TimeUnit.SECONDS);
                        	lock.unlock();
                        	scan.fetchCompleted(true);
                        }
                        finally {
                        	scan.close();
                        }
                        okay = true;
                    } finally {
                        if (okay) {
                            trx.commit();
                        }
                        else {
                            trx.abort();
                        }
                    }
                } catch (Exception e) {
                	setThreadFailed(Thread.currentThread(), e);
                }
            }
        }, "T1");
        final Thread t2 = new Thread(new Runnable() {
            public void run() {
                try {
                    final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testingUniqueIndex);
                    IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory.getInstance(TYPE_STRINGKEYFACTORY);
                    LocationFactory locationFactory = (LocationFactory) db.objectFactory.getInstance(TYPE_ROWLOCATIONFACTORY);
                    IndexKey key = keyFactory.newIndexKey(1);
                    key.parseString("x");
                    Location location = locationFactory.newLocation();
                    location.parseString("2");
                    Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
                    boolean okay = false;
                    try {                       
                        try {
                            trx.acquireLock(location, LockMode.EXCLUSIVE, LockDuration.COMMIT_DURATION);
                            btree.insert(trx, key, location);
                        }
                        finally {
                        }
                        okay = true;
                    } finally {
                        if (okay) {
                            trx.commit();
                        }
                        else {
                            trx.abort();
                        }
                    }
                } catch (Exception e) {
                	setThreadFailed(Thread.currentThread(), e);
                }
            }
        }, "TestingInsertRestartDueToKeyRangeModification");
        final Thread t3 = new Thread(new Runnable() {
            public void run() {
                try {
                    final BTreeImpl btree = db.btreeMgr.getBTreeImpl(1, TYPE_STRINGKEYFACTORY, TYPE_ROWLOCATIONFACTORY, testingUniqueIndex);
                    IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory.getInstance(TYPE_STRINGKEYFACTORY);
                    LocationFactory locationFactory = (LocationFactory) db.objectFactory.getInstance(TYPE_ROWLOCATIONFACTORY);
                    IndexKey key = keyFactory.newIndexKey(1);
                    key.parseString("x1");
                    Location location = locationFactory.newLocation();
                    location.parseString("21");
                    Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
                    boolean okay = false;
                    try {                       
                        try {
                            db.lockmgr.addLockEventListener(listener);
                            trx.acquireLock(location, LockMode.EXCLUSIVE, LockDuration.COMMIT_DURATION);
                            btree.insert(trx, key, location);
                        }
                        finally {
                        	db.lockmgr.clearLockEventListeners();
                        }
                        okay = true;
                    } finally {
                        if (okay) {
                            trx.commit();
                        }
                        else {
                            trx.abort();
                        }
                    }
                } catch (Exception e) {
                	setThreadFailed(Thread.currentThread(), e);
                }
            }
        }, "T3");
        try {
            t1.start();
            Thread.sleep(1000);
            t2.start();
            Thread.sleep(1000);
            t3.start();
            t1.join(10000);
            t2.join(10000);
            t3.join(10000);
            assertTrue(!t1.isAlive());
            assertTrue(!t2.isAlive());
            assertTrue(!t3.isAlive());
            assertEquals(1, status.get());
            checkThreadFailures();
        } finally {
			db.shutdown();
        }       
	}
	
	
	public void testPageSplitLeafUnique() throws Exception {
		doInitContainer();
		doLoadXml(true, "org/simpledbm/rss/impl/im/btree/data1ul.xml");
		doPageSplit(true, true);
		doValidateTree("org/simpledbm/rss/impl/im/btree/testPageSplitLeafUnique.xml");
	}

//	public void testPageSplitNonLeafUnique() throws Exception {
//		BTreeNode.TESTING_MODE = 2;
//		doInitContainer();
//		doTestXml(true, "org/simpledbm/rss/impl/im/btree/data1unl.xml");
//		doPageSplit(false, true);
//	}
	
	public void testPageSplitNonLeafUnique2() throws Exception {
		doInitContainer();
		doLoadXml(true, "org/simpledbm/rss/impl/im/btree/data1unl.xml");
		doPageSplit(false, true);
		doValidateTree("org/simpledbm/rss/impl/im/btree/testPageSplitNonLeafUnique2.xml");
	}

	public void testRestartAndMerge() throws Exception {
		doRestartAndMerge(false, true, 2, 3);
		doValidateTree("org/simpledbm/rss/impl/im/btree/testRestartAndMerge.xml");
	}

	public void testPageSplitLeafUnique2() throws Exception {
		doInitContainer();
		doLoadXml(true, "org/simpledbm/rss/impl/im/btree/data1ul.xml");
		doPageSplit(true, true);
		doValidateTree("org/simpledbm/rss/impl/im/btree/testPageSplitLeafUnique2.xml");
	}

	/*
	public void testRestartAndLink() throws Exception {
    	doLoadXml(true, "org/simpledbm/rss/impl/im/btree/data2unl.xml");
		doRestartAndLink(false, true);
	}
	*/
	
	public void testRestartLink() throws Exception {
    	doLoadXml(true, "org/simpledbm/rss/impl/im/btree/data2unl.xml");
		doRestartLink(false, true);
		doValidateTree("org/simpledbm/rss/impl/im/btree/testRestartLink.xml");
	}

	public void testRestartDelink() throws Exception {
		doRestartDelink(false, true);
		doValidateTree("org/simpledbm/rss/impl/im/btree/testRestartDelink.xml");
	}
	
	public void testRestartAndLinkAgain() throws Exception {
    	//System.out.println("--> PREPARING PARENT");
    	doLoadXml(true, "org/simpledbm/rss/impl/im/btree/data2unl.xml");
		doRestartAndLink(false, true);
	}

	public void testRestartAndRedistribute() throws Exception {
		doRestartAndRedistribute(false, true);
		doValidateTree("org/simpledbm/rss/impl/im/btree/testRestartAndRedistribute.xml");
	}

	public void testRestartAndIncreaseTreeHeight() throws Exception {
		doRestartAndIncreaseTreeHeight(false, true);
		doValidateTree("org/simpledbm/rss/impl/im/btree/testRestartAndIncreaseTreeHeight.xml");
	}

	public void testRestartAndUnlink() throws Exception {
		doRestartAndUnlink(false, true, 2, 5, 3);
	}

	public void testRestartAndMergeAgain() throws Exception {
		doRestartAndMerge(false, true, 5, 3);
	}

	public void testRestartAndDecreaseTreeHeight() throws Exception {
		doRestartAndDecreaseTreeHeight(false, true, 2, 5);
		doValidateTree("org/simpledbm/rss/impl/im/btree/testRestartAndDecreaseTreeHeight.xml");
	}

	public void testSimpleInsertAbort() throws Exception {
		doInitContainer();
		doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data3nul.xml");
		doSingleInsert(false, false, "a", "1", "org/simpledbm/rss/impl/im/btree/testSimpleInsertAbort_1.xml", "org/simpledbm/rss/impl/im/btree/testSimpleInsertAbort_2.xml");
	}
	
	public void testSimpleInsertCommit() throws Exception {
		doInitContainer();
		doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data3nul.xml");
		doSingleInsert(false, true, "a", "1", "org/simpledbm/rss/impl/im/btree/testSimpleInsertAbort_1.xml", "org/simpledbm/rss/impl/im/btree/testSimpleInsertAbort_1.xml");
	}
	
	public void testInsertSplitRootAbort() throws Exception {
		doInitContainer();
		// Generate a tree with just the root node which should be full
		doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data4nul.xml");
		// Following should split the root node
		doSingleInsert(false, true, "da", "8", "org/simpledbm/rss/impl/im/btree/testInsertSplitRootAbort_1.xml", null);
		// Following should cause tree height increase
		doSingleInsert(false, false, "b1", "9", "org/simpledbm/rss/impl/im/btree/testInsertSplitRootAbort_2.xml", "org/simpledbm/rss/impl/im/btree/testInsertSplitRootAbort_3.xml");
	}
	
	public void testInsertSplitRootCommit() throws Exception {
		doInitContainer();
		// Generate a tree with just the root node which should be full
		doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data4nul.xml");
		// Following should split the root node
		doSingleInsert(false, true, "da", "8", "org/simpledbm/rss/impl/im/btree/testInsertSplitRootAbort_1.xml", "org/simpledbm/rss/impl/im/btree/testInsertSplitRootAbort_1.xml");
		// Following should cause tree height increase
		doSingleInsert(false, true, "b1", "9", "org/simpledbm/rss/impl/im/btree/testInsertSplitRootAbort_2.xml", "org/simpledbm/rss/impl/im/btree/testInsertSplitRootAbort_2.xml");
	}

	public void testInsertSplitRootAbortLogical() throws Exception {
		doInitContainer();
		// Generate a tree with just the root node which should be full
		doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data5nul.xml");
		// This should trigger logical undo as page will be split by
		// second insert
		doDoubleInsert(false, false, "g", "7", true, "da", "8",
				"org/simpledbm/rss/impl/im/btree/testInsertSplitRootAbortLogical_1.xml",
				"org/simpledbm/rss/impl/im/btree/testInsertSplitRootAbortLogical_2.xml",
				"org/simpledbm/rss/impl/im/btree/testInsertSplitRootAbortLogical_3.xml");
	}
	
	public void testInsertUnderflowFig13() throws Exception {
		doInitContainer();
		doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data6nul.xml");
		doSingleInsert(false, true, "a0", "1");
		doValidateTree("org/simpledbm/rss/impl/im/btree/testInsertUnderflowFig13.xml");
	}
	
	public void testInsertUnderflowFig14() throws Exception {
		doInitContainer();
		doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data6nul.xml");
		doSingleInsert(false, true, "c19", "1");
		doValidateTree("org/simpledbm/rss/impl/im/btree/testInsertUnderflowFig14.xml");
	}

	public void testInsertUnderflowFig5() throws Exception {
		doInitContainer();
		doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data7nul.xml");
		doSingleInsert(false, true, "k3", "1");
		doValidateTree("org/simpledbm/rss/impl/im/btree/testInsertUnderflowFig5.xml");
	}
	
	public void testInsertUnderflowFig19() throws Exception {
		doInitContainer();
		doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data6nul.xml");
		doSingleInsert(false, true, "k3", "1");
		doValidateTree("org/simpledbm/rss/impl/im/btree/testInsertUnderflowFig19.xml");
	}
	
	public void testInsertUnderflowFig15() throws Exception {
		doInitContainer();
		doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data6nul.xml");
		doSingleInsert(false, true, "g19", "1");
		doValidateTree("org/simpledbm/rss/impl/im/btree/testInsertUnderflowFig15.xml");		
	}

	// TODO test next key loc across a page
	public void testInsertNextKeyInNextPage() throws Exception {
		doInitContainer();
		doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data7nul.xml");
		doSingleInsert(false, true, "b4", "1");
		doValidateTree("org/simpledbm/rss/impl/im/btree/testInsertNextKeyInNextPage.xml");		
	}

	public void testInsertUnderflowFig17() throws Exception {
		doInitContainer();
		doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data8nul.xml");
		doSingleInsert(false, true, "c0", "1");
		doValidateTree("org/simpledbm/rss/impl/im/btree/testInsertUnderflowFig17.xml");		
	}

	public void testDelete1() throws Exception {
		doInitContainer();
		doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data6nul.xml");
		doSingleDelete(false, true, "a1", "10");
		doValidateTree("org/simpledbm/rss/impl/im/btree/testDelete1.xml");		
	}
	
	public void testDeleteInsert1() throws Exception {
		doInitContainer();
		doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data6nul.xml");
		doDeleteInsertThreads(false, true, "a1", "10", "org/simpledbm/rss/impl/im/btree/testDeleteInsert1_1.xml", "org/simpledbm/rss/impl/im/btree/testDeleteInsert1_2.xml");
	}
	
    public void testDeleteInsert2() throws Exception {
		doInitContainer();
		doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data6nul.xml");
		doDeleteInsertThreads(false, false, "a1", "10", "org/simpledbm/rss/impl/im/btree/testDeleteInsert1_1.xml", null);
	}
	
    public void testScan1() throws Exception {
        doInitContainer();
        doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data6nul.xml");
        ScanResult[] scanResults = new ScanResult[] {
        		new ScanResult("a1", "10"),
        		new ScanResult("a2", "11"),
        		new ScanResult("b1", "21"),
        		new ScanResult("b2", "22"),
        		new ScanResult("b3", "23"),
        		new ScanResult("b4", "24"),
        		new ScanResult("c1", "31"),
        		new ScanResult("c2", "32"),
        		new ScanResult("d1", "41"),
        		new ScanResult("d2", "42"),
        		new ScanResult("d3", "43"),
        		new ScanResult("d4", "44"),
        		new ScanResult("e1", "51"),
        		new ScanResult("e2", "52"),
        		new ScanResult("e3", "53"),
        		new ScanResult("e4", "54"),
        		new ScanResult("f1", "61"),
        		new ScanResult("f2", "62"),
        		new ScanResult("f3", "63"),
        		new ScanResult("f4", "64"),
        		new ScanResult("g1", "71"),
        		new ScanResult("g2", "72"),
        		new ScanResult("h1", "81"),
        		new ScanResult("h2", "82"),
        		new ScanResult("h3", "83"),
        		new ScanResult("h4", "84"),
        		new ScanResult("i1", "91"),
        		new ScanResult("i2", "92"),
        		new ScanResult("j1", "101"),
        		new ScanResult("j2", "102"),
        		new ScanResult("j3", "103"),
        		new ScanResult("j4", "104"),
        		new ScanResult("k1", "111"),
        		new ScanResult("k2", "112"),
        		new ScanResult("<INFINITY>", "999")
        };
        doScanTree(false, false, "a1", "10", scanResults);
    }

	public void testScan2() throws Exception {
        doInitContainer();
        doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data6nul.xml");
        ScanResult[] scanResults = new ScanResult[] {
        		new ScanResult("a1", "10"),
        		new ScanResult("a2", "11"),
        		new ScanResult("b1", "21"),
        		new ScanResult("b2", "22"),
        		new ScanResult("b3", "23"),
        		new ScanResult("b4", "24"),
        		new ScanResult("c1", "31"),
        		new ScanResult("c2", "32"),
        		new ScanResult("d1", "41"),
        		new ScanResult("d2", "42"),
        		new ScanResult("d3", "43"),
        		new ScanResult("d4", "44"),
        		new ScanResult("e1", "51"),
        		new ScanResult("e2", "52"),
        		new ScanResult("e3", "53"),
        		new ScanResult("e4", "54"),
        		new ScanResult("f1", "61"),
        		new ScanResult("f2", "62"),
        		new ScanResult("f3", "63"),
        		new ScanResult("f4", "64"),
        		new ScanResult("g1", "71"),
        		new ScanResult("g2", "72"),
        		new ScanResult("h1", "81"),
        		new ScanResult("h2", "82"),
        		new ScanResult("h3", "83"),
        		new ScanResult("h4", "84"),
        		new ScanResult("i1", "91"),
        		new ScanResult("i2", "92"),
        		new ScanResult("j1", "101"),
        		new ScanResult("j2", "102"),
        		new ScanResult("j3", "103"),
        		new ScanResult("j4", "104"),
        		new ScanResult("k1", "111"),
        		new ScanResult("k2", "112"),
        		new ScanResult("<INFINITY>", "999")
        };
        ScanResult[] scanResults2 = new ScanResult[] {
        		new ScanResult("a1", "10"),
        		new ScanResult("a2", "11"),
        		new ScanResult("b1", "21"),
        		new ScanResult("b2", "22"),
        		new ScanResult("b3", "23"),
        		new ScanResult("b4", "24"),
        		new ScanResult("c1", "31"),
        		new ScanResult("c2", "32"),
        		new ScanResult("d1", "41"),
        		new ScanResult("d2", "42"),
        		new ScanResult("d3", "43"),
        		new ScanResult("d4", "44"),
        		new ScanResult("e1", "51"),
        		new ScanResult("e2", "52"),
        		new ScanResult("e3", "53"),
        		new ScanResult("e4", "54"),
        		new ScanResult("f1", "61"),
        		new ScanResult("f2", "62"),
        		new ScanResult("f4", "64"),
        		new ScanResult("g1", "71"),
        		new ScanResult("g2", "72"),
        		new ScanResult("h1", "81"),
        		new ScanResult("h2", "82"),
        		new ScanResult("h3", "83"),
        		new ScanResult("h4", "84"),
        		new ScanResult("i1", "91"),
        		new ScanResult("i2", "92"),
        		new ScanResult("j1", "101"),
        		new ScanResult("j2", "102"),
        		new ScanResult("j3", "103"),
        		new ScanResult("j4", "104"),
        		new ScanResult("k1", "111"),
        		new ScanResult("k2", "112"),
        		new ScanResult("<INFINITY>", "999")
        };
        doScanAndDelete(false, false, "a1", "10");
        doScanTree(false, false, "a1", "10", scanResults);
        doDeleteAndScanThreads(false, true, "f3", "63", scanResults2);
    }

    public void testScan3() throws Exception {
        ScanResult[] scanResults = new ScanResult[] {
				new ScanResult("a1", "10"), new ScanResult("a2", "11"),
				new ScanResult("b1", "21"), new ScanResult("b2", "22"),
				new ScanResult("b3", "23"), new ScanResult("b4", "24"),
				new ScanResult("c1", "31"), new ScanResult("c2", "32"),
				new ScanResult("d1", "41"), new ScanResult("d2", "42"),
				new ScanResult("d3", "43"), new ScanResult("d4", "44"),
				new ScanResult("e1", "51"), new ScanResult("e2", "52"),
				new ScanResult("e3", "53"), new ScanResult("e4", "54"),
				new ScanResult("f1", "61"), new ScanResult("f2", "62"),
				new ScanResult("f3", "63"), new ScanResult("f4", "64"),
				new ScanResult("g1", "71"), new ScanResult("g2", "72"),
				new ScanResult("h1", "81"), new ScanResult("h2", "82"),
				new ScanResult("h3", "83"), new ScanResult("h4", "84"),
				new ScanResult("i1", "91"), new ScanResult("i2", "92"),
				new ScanResult("j1", "101"), new ScanResult("j2", "102"),
				new ScanResult("j3", "103"), new ScanResult("j4", "104"),
				new ScanResult("k1", "111"), new ScanResult("k2", "112"),
				new ScanResult("<INFINITY>", "999") };  	
        doInitContainer();
        doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data6nul.xml");
        doDeleteAndScanThreads(false, false, "f3", "63", scanResults);
    }
	
    public void testScan4() throws Exception {
        ScanResult[] scanResults = new ScanResult[] {
				new ScanResult("a1", "10"), new ScanResult("a2", "11"),
				new ScanResult("b1", "21"), new ScanResult("b2", "22"),
				new ScanResult("b3", "23"), new ScanResult("b4", "24"),
				new ScanResult("c1", "31"), new ScanResult("c2", "32"),
				new ScanResult("d1", "41"), new ScanResult("d2", "42"),
				new ScanResult("d3", "43"), new ScanResult("d4", "44"),
				new ScanResult("e1", "51"), new ScanResult("e2", "52"),
				new ScanResult("e3", "53"), new ScanResult("e4", "54"),
				new ScanResult("f1", "61"), new ScanResult("f2", "62"),
				new ScanResult("f3", "63"), new ScanResult("f4", "64"),
				new ScanResult("g1", "71"), new ScanResult("g2", "72"),
				new ScanResult("h1", "81"), new ScanResult("h2", "82"),
				new ScanResult("h3", "83"), new ScanResult("h4", "84"),
				new ScanResult("i1", "91"), new ScanResult("i2", "92"),
				new ScanResult("j1", "101"), new ScanResult("j2", "102"),
				new ScanResult("j3", "103"), new ScanResult("j4", "104"),
				new ScanResult("k1", "111"), new ScanResult("k2", "112"),
				new ScanResult("<INFINITY>", "999") };  	
        doInitContainer();
        doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data6nul.xml");
        doScanAndDeleteThreads(false, false, "f3", "63", scanResults);
    }

    /**
     * This test scans and deletes all keys from the BTree as a single transaction.
     * It completes without committing or aborting the transaction.
     * At next restart, the expected behaviour is that the tree will
     * be restored because the delete transaction will be rolled back.
     * <p>This test must be followed by {@link #testScanAfterCrash()}.
     * @throws Exception
     */
	public void testScanDeleteCrash() throws Exception {
        doInitContainer();
        doLoadXml(false, "org/simpledbm/rss/impl/im/btree/data6nul.xml");
        doScanAndDelete(false, false, "a1", "10");
    }

	/**
	 * This test must be run after {@link #testScanDeleteCrash()}.
	 * It verifies that the BTree has been restored after system restart.
	 */
	public void testScanAfterCrash() throws Exception {
        ScanResult[] scanResults = new ScanResult[] {
				new ScanResult("a1", "10"), new ScanResult("a2", "11"),
				new ScanResult("b1", "21"), new ScanResult("b2", "22"),
				new ScanResult("b3", "23"), new ScanResult("b4", "24"),
				new ScanResult("c1", "31"), new ScanResult("c2", "32"),
				new ScanResult("d1", "41"), new ScanResult("d2", "42"),
				new ScanResult("d3", "43"), new ScanResult("d4", "44"),
				new ScanResult("e1", "51"), new ScanResult("e2", "52"),
				new ScanResult("e3", "53"), new ScanResult("e4", "54"),
				new ScanResult("f1", "61"), new ScanResult("f2", "62"),
				new ScanResult("f3", "63"), new ScanResult("f4", "64"),
				new ScanResult("g1", "71"), new ScanResult("g2", "72"),
				new ScanResult("h1", "81"), new ScanResult("h2", "82"),
				new ScanResult("h3", "83"), new ScanResult("h4", "84"),
				new ScanResult("i1", "91"), new ScanResult("i2", "92"),
				new ScanResult("j1", "101"), new ScanResult("j2", "102"),
				new ScanResult("j3", "103"), new ScanResult("j4", "104"),
				new ScanResult("k1", "111"), new ScanResult("k2", "112"),
				new ScanResult("<INFINITY>", "999") };
        doScanTree(false, false, "a1", "10", scanResults);
    }

	/**
	 * This test loads a set of sorted data, and then scans the tree to verify
	 * that the tree contains the data in the same sort order.
	 */
	public void testInsertInOrder() throws Exception {
        ScanResult[] inserts = new ScanResult[] {
				new ScanResult("a1", "10"), new ScanResult("a2", "11"),
				new ScanResult("b1", "21"), new ScanResult("b2", "22"),
				new ScanResult("b3", "23"), new ScanResult("b4", "24"),
				new ScanResult("c1", "31"), new ScanResult("c2", "32"),
				new ScanResult("d1", "41"), new ScanResult("d2", "42"),
				new ScanResult("d3", "43"), new ScanResult("d4", "44"),
				new ScanResult("e1", "51"), new ScanResult("e2", "52"),
				new ScanResult("e3", "53"), new ScanResult("e4", "54"),
				new ScanResult("f1", "61"), new ScanResult("f2", "62"),
				new ScanResult("f3", "63"), new ScanResult("f4", "64"),
				new ScanResult("g1", "71"), new ScanResult("g2", "72"),
				new ScanResult("h1", "81"), new ScanResult("h2", "82"),
				new ScanResult("h3", "83"), new ScanResult("h4", "84"),
				new ScanResult("i1", "91"), new ScanResult("i2", "92"),
				new ScanResult("j1", "101"), new ScanResult("j2", "102"),
				new ScanResult("j3", "103"), new ScanResult("j4", "104"),
				new ScanResult("k1", "111"), new ScanResult("k2", "112") };
        ScanResult[] scanResults = new ScanResult[] {
				new ScanResult("a1", "10"), new ScanResult("a2", "11"),
				new ScanResult("b1", "21"), new ScanResult("b2", "22"),
				new ScanResult("b3", "23"), new ScanResult("b4", "24"),
				new ScanResult("c1", "31"), new ScanResult("c2", "32"),
				new ScanResult("d1", "41"), new ScanResult("d2", "42"),
				new ScanResult("d3", "43"), new ScanResult("d4", "44"),
				new ScanResult("e1", "51"), new ScanResult("e2", "52"),
				new ScanResult("e3", "53"), new ScanResult("e4", "54"),
				new ScanResult("f1", "61"), new ScanResult("f2", "62"),
				new ScanResult("f3", "63"), new ScanResult("f4", "64"),
				new ScanResult("g1", "71"), new ScanResult("g2", "72"),
				new ScanResult("h1", "81"), new ScanResult("h2", "82"),
				new ScanResult("h3", "83"), new ScanResult("h4", "84"),
				new ScanResult("i1", "91"), new ScanResult("i2", "92"),
				new ScanResult("j1", "101"), new ScanResult("j2", "102"),
				new ScanResult("j3", "103"), new ScanResult("j4", "104"),
				new ScanResult("k1", "111"), new ScanResult("k2", "112"),
				new ScanResult("<INFINITY>", "0") };
        doInitContainer2();
        doLoadData(inserts);
        doScanTree(true, false, "a1", "10", scanResults);
    }

	/**
	 * This test loads a set of sorted data from a file.
	 * Scans the tree and verifies that the scan order matches the sort order in the file.
	 * It then does a find for each key and verifies that all finds succeed.
	 * The data set is large enough to cause the container to be extended.
	 */
	public void testInsertInOrderFromFile() throws Exception {
        doInitContainer2();
        doLoadData("org/simpledbm/rss/impl/im/btree/data1.txt");
        doScanTree("org/simpledbm/rss/impl/im/btree/data1.txt");
        doFindInTree("org/simpledbm/rss/impl/im/btree/data1.txt");
    }
	
	/**
	 * Tests the phantom record scenario as described in TPCT.
	 * Suppose that a file has sorted list of records with key values
	 * w, y, z. There are 4 operations of interest:
	 * <ol>
	 * <li>Read unique. Read a unique record (say X), given its key.</li>
	 * <li>Read next. Read the next record, Y, after the record W.</li>
	 * <li>Insert. Insert record X between W and Y.</li>
	 * <li>Delete. Delete record Y.</li>
	 * </ol>
	 * <p>Phantom records with keys X and Y arise in the following cases.</p>
	 * <p>If transaction T performs a read unique of record X, and it is not
	 * found, T must prevent others from inserting phantom record X until T
	 * commits.</p>
	 * <p>If T is at record W and does a read next to get record Y, then
	 * W and Y cannot change; in addition, no one may insert a new record (phantom record X)
	 * between W and Y, until T commits.</p>
	 * <p>If T deletes record Y, no other transaction should insert a
	 * phantom Y, until T commits. In addition no other transaction should
	 * notice that the original Y is missing and that Z is now immediately after X, until
	 * T commits.</p>
	 * 
	 * @throws Exception
	 */
	public void testPhantomRecords1() throws Exception {
        doInitContainer2();
        doLoadData("org/simpledbm/rss/impl/im/btree/data2.txt");
		doReadUniqueX();
        ScanResult[] scanResults = new ScanResult[] {
				new ScanResult("w", "1"), new ScanResult("x", "2"),
				new ScanResult("y", "3"), new ScanResult("z", "4"),
				new ScanResult("<INFINITY>", "0") };
        doScanTree(true, false, "w", "1", scanResults);
	}

	public void testPhantomRecords2() throws Exception {
        doInitContainer2();
        doLoadData("org/simpledbm/rss/impl/im/btree/data2.txt");
		doReadUniqueX2();
        ScanResult[] scanResults = new ScanResult[] {
				new ScanResult("w", "1"), new ScanResult("x", "2"), new ScanResult("x1", "21"),
				new ScanResult("y", "3"), new ScanResult("z", "4"),
				new ScanResult("<INFINITY>", "0") };
        doScanTree(true, false, "w", "1", scanResults);
	}

	class DataLoaderThread implements Runnable {
		
		final String filename;
		
		final BTreeDB db;
		
		int count = 0;

		public DataLoaderThread(final BTreeDB db, String filename) {
			this.db = db;
			this.filename = filename;
		}
		
		void doLoadData() throws Exception {

			BufferedReader reader = new BufferedReader(new InputStreamReader(
					ClassUtils.getResourceAsStream(filename)));
			try {
				Index index = db.btreeMgr.getIndex(1);

				IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory
						.getInstance(TYPE_STRINGKEYFACTORY);
				LocationFactory locationFactory = (LocationFactory) db.objectFactory
						.getInstance(TYPE_ROWLOCATIONFACTORY);

				String line = reader.readLine();
				int i = 1;
				while (line != null) {
					String k = line.trim();
					IndexKey key = keyFactory.newIndexKey(1);
					key.parseString(k);
					RowLocation location = (RowLocation) locationFactory.newLocation();
					location.loc = i++;
					while (true) {
						Transaction trx = db.trxmgr
							.begin(IsolationMode.SERIALIZABLE);
						boolean okay = false;
						try {
							// System.out.println("Inserting (" + key + ", " + location
							// + ")");
                            trx.acquireLock(location, LockMode.EXCLUSIVE, LockDuration.COMMIT_DURATION);
							index.insert(trx, key, location);
							// System.out.println(Thread.currentThread() + ": inserted " + k);
							okay = true;
						}
						catch (LockDeadlockException e) {
							// assume deadlock
							e.printStackTrace();
						}
						catch (UniqueConstraintViolationException e) {
							break;
						}
						finally {
							if (okay)
								trx.commit();
							else
								trx.abort();
						}
						if (okay) {
							break;
						}
					}
					line = reader.readLine();
				}
				count = i-1;
			} finally {
				reader.close();
			}
		}

		public void run() {
			try {
				doLoadData();
			} catch (Exception e) {
				e.printStackTrace();
				TestBTreeManager.this.setThreadFailed(Thread.currentThread(), e);
			}
		}
	}

	class DataDeleterThread implements Runnable {
		
		final String filename;
		
		final BTreeDB db;
		
		int count = 0;
		
		boolean doCommit;

		public DataDeleterThread(final BTreeDB db, String filename, boolean doCommit) {
			this.db = db;
			this.filename = filename;
			this.doCommit = doCommit;
		}
		
		void doLoadData() throws Exception {

			BufferedReader reader = new BufferedReader(new InputStreamReader(
					ClassUtils.getResourceAsStream(filename)));
			try {
				Index index = db.btreeMgr.getIndex(1);

				IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory
						.getInstance(TYPE_STRINGKEYFACTORY);
				LocationFactory locationFactory = (LocationFactory) db.objectFactory
						.getInstance(TYPE_ROWLOCATIONFACTORY);

				String line = reader.readLine();
				int i = 1;
				while (line != null) {
					String k = line.trim();
					IndexKey key = keyFactory.newIndexKey(1);
					key.parseString(k);
					RowLocation location = (RowLocation) locationFactory.newLocation();
					location.loc = i++;
					while (true) {
						Transaction trx = db.trxmgr
							.begin(IsolationMode.SERIALIZABLE);
						boolean okay = false;
						try {
							// System.out.println("Inserting (" + key + ", " + location
							// + ")");
                            trx.acquireLock(location, LockMode.EXCLUSIVE, LockDuration.COMMIT_DURATION);
							index.delete(trx, key, location);
							// System.out.println(Thread.currentThread() + ": inserted " + k);
							okay = true;
						}
						catch (LockDeadlockException e) {
							// assume deadlock
							e.printStackTrace();
						}
						finally {
							if (okay && doCommit) {
								trx.commit();
								count++;
							}
							else
								trx.abort();
						}
						if (okay) {
							break;
						}
					}
					line = reader.readLine();
				}
			} finally {
				reader.close();
			}
		}

		public void run() {
			try {
				doLoadData();
			} catch (Exception e) {
				e.printStackTrace();
				TestBTreeManager.this.setThreadFailed(Thread.currentThread(), e);
			}
		}
	}
	
	
	int doConcurrentInserts(String file1, String file2) throws Exception {
		final BTreeDB db = new BTreeDB(false);
		try {
			DataLoaderThread d1 = new DataLoaderThread(db, file1);
			DataLoaderThread d2 = new DataLoaderThread(db, file2);
			Thread t1 = new Thread(d1, "T1");
			Thread t2 = new Thread(d2, "T2");
			
			t1.start();
			t2.start();
			
			t1.join();
			t2.join();
			checkThreadFailures();
			System.out.println("Inserted " + (d1.count + d2.count) + " keys" );
			return d1.count + d2.count;
		}
		finally {
			db.shutdown();
		}
	}

	int doConcurrentDeletes(String file1, String file2) throws Exception {
		final BTreeDB db = new BTreeDB(false);
		try {
			DataDeleterThread d1 = new DataDeleterThread(db, file1, true);
			DataDeleterThread d2 = new DataDeleterThread(db, file2, false);
			Thread t1 = new Thread(d1, "T1");
			Thread t2 = new Thread(d2, "T2");
			
			t1.start();
			t2.start();
			
			t1.join();
			t2.join();
			checkThreadFailures();
			System.out.println("Deleted " + (d1.count + d2.count) + " keys" );
			return d1.count + d2.count;
		}
		finally {
			db.shutdown();
		}
	}
	
	
    /**
     * Scans the tree from a starting key to the eof.
     */
    void doFindInTree2(String filename) throws Exception {
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				ClassUtils.getResourceAsStream(filename)));
		final BTreeDB db = new BTreeDB(false);
		try {
			Index index = db.btreeMgr.getIndex(1);

			IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory
					.getInstance(TYPE_STRINGKEYFACTORY);
			LocationFactory locationFactory = (LocationFactory) db.objectFactory
					.getInstance(TYPE_ROWLOCATIONFACTORY);

			String line = reader.readLine();
			Transaction trx = db.trxmgr.begin(IsolationMode.SERIALIZABLE);
			try {
				int i = 1;
				while (line != null) {
					String k = line.trim();
					IndexKey key = keyFactory.newIndexKey(1);
					key.parseString(k);
					RowLocation location = (RowLocation) locationFactory.newLocation();
					location.loc = i++;
					IndexScan scan = index.openScan(trx, key, location, false);
					if (scan.fetchNext()) {
	                	// System.out.println("new FindResult(\"" + scan.getCurrentKey() + "\", \"" + scan.getCurrentLocation() + "\"),");
						assertEquals(key.toString(), scan.getCurrentKey().toString());
						assertEquals(location.toString(), scan.getCurrentLocation()
								.toString());
						scan.fetchCompleted(true);
					} 
					else {
						fail("Find failed for (" + key + ", " + location + ")");
					}
					scan.close();
					line = reader.readLine();
				}
			} finally {
				trx.abort();
			}
		} finally {
			reader.close();
			db.shutdown();
		}
	}

    /**
     * Scans the tree from a starting key to the eof.
     * Verifies that the keys are in sorted order.
     */
    int doDumpTree(String filename) throws Exception {
//		BufferedWriter writer = new BufferedWriter(new FileWriter(
//				filename));
		final BTreeDB db = new BTreeDB(false);
		int count = 0;
		try {
			Index index = db.btreeMgr.getIndex(1);

			IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory
					.getInstance(TYPE_STRINGKEYFACTORY);
			LocationFactory locationFactory = (LocationFactory) db.objectFactory
					.getInstance(TYPE_ROWLOCATIONFACTORY);

			Transaction trx = db.trxmgr.begin(IsolationMode.READ_COMMITTED);
			try {
				IndexKey key = keyFactory.newIndexKey(1);
				key.parseString(" ");
				RowLocation location = (RowLocation) locationFactory
						.newLocation();
				location.loc = 1;
				IndexScan scan = index.openScan(trx, key, location, false);
				String prevKey = null;
				try {
					while (scan.fetchNext()) {
						if (scan.isEof()) {
							break;
						}
						count++;
//						writer.write(scan.getCurrentKey() + ","
//								+ scan.getCurrentLocation() + "\n");
						if (prevKey != null
								&& !scan.getCurrentKey().toString().equals(
										StringKey.MAX_KEY)) {
							assertTrue(scan.getCurrentKey().toString()
									.compareTo(prevKey) > 0);
						}
						prevKey = scan.getCurrentKey().toString();
						scan.fetchCompleted(true);
					}
				} finally {
					scan.close();
				}
			} finally {
				trx.abort();
			}
		} finally {
//			writer.close();
			db.shutdown();
		}
		System.out.println("Number of keys found = " + count);
		return count;
	}

    
    void doIsolationTest_ReadCommitted() throws Exception {
		final BTreeDB db = new BTreeDB(false);
		try {
			int containerId = 1;
			Index index = db.btreeMgr.getIndex(containerId);

			IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory
					.getInstance(TYPE_STRINGKEYFACTORY);
			LocationFactory locationFactory = (LocationFactory) db.objectFactory
					.getInstance(TYPE_ROWLOCATIONFACTORY);

			/*
			 * Scan the tree with lock isolation mode of READ_COMMITTED and verify locks.
			 */
			Transaction trx = db.trxmgr.begin(IsolationMode.READ_COMMITTED);
			try {
				IndexKey key = keyFactory.newIndexKey(containerId);
				key.parseString(" ");
				RowLocation location = (RowLocation) locationFactory
						.newLocation();
				location.loc = 1;
				IndexScan scan = index.openScan(trx, key, location, false);
				Location prevLocation = null;
				try {
					int i = 0;
					while (scan.fetchNext() && i < 10) {
						if (scan.isEof()) {
							break;
						}
						assertEquals(LockMode.SHARED, trx.hasLock(scan.getCurrentLocation()));
						if (prevLocation != null) {
							assertEquals(LockMode.NONE, trx.hasLock(prevLocation));
						}
						prevLocation = scan.getCurrentLocation();
						scan.fetchCompleted(true);
						assertEquals(LockMode.NONE, trx.hasLock(prevLocation));
						i++;
					}
				} finally {
					scan.close();
				}
			} finally {
				trx.abort();
			}
		} finally {
			db.shutdown();
		}
	}
    
    void doIsolationTest_CursorStability() throws Exception {
		final BTreeDB db = new BTreeDB(false);
		try {
			int containerId = 1;
			Index index = db.btreeMgr.getIndex(containerId);

			IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory
					.getInstance(TYPE_STRINGKEYFACTORY);
			LocationFactory locationFactory = (LocationFactory) db.objectFactory
					.getInstance(TYPE_ROWLOCATIONFACTORY);

			/*
			 * Scan the tree with lock isolation mode of CURSOR_STABILITY and verify locks.
			 */
			Transaction trx = db.trxmgr.begin(IsolationMode.CURSOR_STABILITY);
			try {
				IndexKey key = keyFactory.newIndexKey(containerId);
				key.parseString(" ");
				RowLocation location = (RowLocation) locationFactory
						.newLocation();
				location.loc = 1;
				IndexScan scan = index.openScan(trx, key, location, false);
				Location prevLocation = null;
				try {
					int i = 0;
					while (scan.fetchNext() && i < 10) {
						if (scan.isEof()) {
							break;
						}
						assertEquals(LockMode.SHARED, trx.hasLock(scan.getCurrentLocation()));
						if (prevLocation != null) {
							assertEquals(LockMode.NONE, trx.hasLock(prevLocation));
						}
						prevLocation = scan.getCurrentLocation();
						scan.fetchCompleted(true);
						assertEquals(LockMode.SHARED, trx.hasLock(prevLocation));
						i++;
					}
				} finally {
					scan.close();
				}
			} finally {
				trx.abort();
			}
		} finally {
			db.shutdown();
		}
	}
    
    void doIsolationTest_RepeatableRead(boolean updateMode) throws Exception {
		final BTreeDB db = new BTreeDB(false);
		try {
			int containerId = 1;
			Index index = db.btreeMgr.getIndex(containerId);

			IndexKeyFactory keyFactory = (IndexKeyFactory) db.objectFactory
					.getInstance(TYPE_STRINGKEYFACTORY);
			LocationFactory locationFactory = (LocationFactory) db.objectFactory
					.getInstance(TYPE_ROWLOCATIONFACTORY);

			/*
			 * Scan the tree with lock isolation mode of CURSOR_STABILITY and verify locks.
			 */
			Transaction trx = db.trxmgr.begin(IsolationMode.REPEATABLE_READ);
			try {
				IndexKey key = keyFactory.newIndexKey(containerId);
				key.parseString(" ");
				RowLocation location = (RowLocation) locationFactory
						.newLocation();
				location.loc = 1;
				IndexScan scan = index.openScan(trx, key, location, updateMode);
				Location prevLocation = null;
				try {
					int i = 0;
					LockMode mode = LockMode.SHARED;
					if (updateMode) {
						mode = LockMode.UPDATE;						
					}
					while (scan.fetchNext() && i < 10) {
						if (scan.isEof()) {
							break;
						}
						assertEquals(mode, trx.hasLock(scan.getCurrentLocation()));
						if (prevLocation != null) {
							assertEquals(LockMode.SHARED, trx.hasLock(prevLocation));
						}
						prevLocation = scan.getCurrentLocation();
						scan.fetchCompleted(true);
						assertEquals(mode, trx.hasLock(prevLocation));
						i++;
					}
				} finally {
					scan.close();
				}
			} finally {
				trx.abort();
			}
		} finally {
			db.shutdown();
		}
	}
    
    /**
     * Search for a specific key and return true if found.
     */
    boolean doFindKeyLockTest(IsolationMode isolationMode, String k, int loc,
			boolean forUpdate) throws Exception {
		final BTreeDB db = new BTreeDB(false);
		try {
			BTreeImpl index = (BTreeImpl) db.btreeMgr.getIndex(1);

			IndexKey key = index.getNewIndexKey();
			key.parseString(k);
			RowLocation location = (RowLocation) index.getNewLocation();
			location.loc = loc;

			/*
			 * Searches for a specified key and verifies lock mode in
			 * found and not found situations.
			 */
			Transaction trx = db.trxmgr.begin(isolationMode);
			boolean found = false;
			try {
				IndexScan scan = index.openScan(trx, key, location, forUpdate);
				try {
					if (scan.fetchNext()) {
						if (key.toString().equals(
								scan.getCurrentKey().toString())
								&& location.equals(scan.getCurrentLocation())) {
							// found
							if (forUpdate) {
								assertEquals(LockMode.UPDATE, trx.hasLock(scan
										.getCurrentLocation()));
							} else {
								assertEquals(LockMode.SHARED, trx.hasLock(scan
										.getCurrentLocation()));
							}
							found = true;
						} else {
							// not found - but IndexScan has no concept of not found
							// so lock should be held on next key - lock is released if 
							// necessary when fetchCompleted(false) is called.
							if (forUpdate) {
								assertEquals(LockMode.UPDATE, trx.hasLock(scan
										.getCurrentLocation()));
							} else {
								assertEquals(LockMode.SHARED, trx.hasLock(scan
										.getCurrentLocation()));
							}
						}
						System.err.println("FOUND = " + found);
						System.err.println("KEY = " + scan.getCurrentKey().toString());
						System.err.println("LOCATION = " + scan.getCurrentLocation().toString());
						scan.fetchCompleted(found);
						if (trx.getIsolationMode() == IsolationMode.READ_COMMITTED) {
							/*
							 * Regardless of fetch result, lock on current key should have been released
							 * after fetchCompleted().
							 */
							assertEquals(LockMode.NONE, trx.hasLock(scan
									.getCurrentLocation()));
						} else if (!found) {
							if (trx.getIsolationMode() == IsolationMode.CURSOR_STABILITY || trx.getIsolationMode() == IsolationMode.REPEATABLE_READ) {
								/*
								 * Lock on next key should have been released
								 */
								assertEquals(LockMode.NONE, trx.hasLock(scan
										.getCurrentLocation()));
							}
							else if (trx.getIsolationMode() == IsolationMode.SERIALIZABLE) {
								/*
								 * Lock on next key should be downgraded
								 */
								assertEquals(LockMode.SHARED, trx.hasLock(scan
									.getCurrentLocation()));
							}
						}
						return found;
					} else {
						assertTrue(scan.isEof());
						System.err.println("EOF");
						System.err.println("KEY = " + scan.getCurrentKey().toString());
						System.err.println("LOCATION = " + scan.getCurrentLocation().toString());
						return false;
					}
				} finally {
					if (scan != null) {
						scan.close();
						if (!found) {
							if (isolationMode == IsolationMode.SERIALIZABLE) {
								assertEquals(LockMode.SHARED, trx.hasLock(scan.getCurrentLocation()));
							}
							else {
								assertEquals(LockMode.NONE, trx.hasLock(scan.getCurrentLocation()));
							}
						}
					}
				}
			} finally {
				trx.abort();
			}
		} finally {
			db.shutdown();
		}
	}
    
    
    void doTestMultiThreadedInserts(String file1, String file2) throws Exception {
    	compressKeys = true;
    	largeBM = true;
		doInitContainer2();
		System.out.println("Starting inserts");
		doConcurrentInserts(file1, file2);
		System.out.println("Dumping tree");
		doDumpTree("testdata/TestBTreeManager/dump.txt");
		//doFindRubens();
		System.out.println("Doing first set of finds");
		doFindInTree2(file1);
		System.out.println("Doing second set of finds");
		doFindInTree2(file2);
		System.out.println("Starting deletes");
		doConcurrentDeletes(file1, file2);
		System.out.println("Dumping tree again");
		doDumpTree("testdata/TestBTreeManager/dump2.txt");
		System.out.println("Validating second set of finds");
		doFindInTree2(file2);
		compressKeys = false;
		largeBM = false;
    }
    
    /**
     * Tests various concurrent activities. 
     * @throws Exception
     */
    public void testMultiThreadedInserts() throws Exception {
    	doTestMultiThreadedInserts("org/simpledbm/rss/impl/im/btree/english.0", 
    			"org/simpledbm/rss/impl/im/btree/english.1");
	}

    public void testMultiThreadedInsertsRandom() throws Exception {
    	doTestMultiThreadedInserts("org/simpledbm/rss/impl/im/btree/random.0", 
    			"org/simpledbm/rss/impl/im/btree/random.1");
	}
    
    public void testMultiThreadedInsertsDescending() throws Exception {
    	doTestMultiThreadedInserts("org/simpledbm/rss/impl/im/btree/reverse.0", 
    			"org/simpledbm/rss/impl/im/btree/reverse.1");
	}

    public void testIsolation() throws Exception {
		ScanResult[] inserts = new ScanResult[] { new ScanResult("a1", "10"),
				new ScanResult("a2", "11"), new ScanResult("b1", "21"),
				new ScanResult("b2", "22"), new ScanResult("b3", "23"),
				new ScanResult("b4", "24"), new ScanResult("c1", "31"),
				new ScanResult("c2", "32"), new ScanResult("d1", "41"),
				new ScanResult("d2", "42"), new ScanResult("d3", "43"),
				new ScanResult("d4", "44"), new ScanResult("e1", "51"),
				new ScanResult("e2", "52"), new ScanResult("e3", "53"),
				new ScanResult("e4", "54"), new ScanResult("f1", "61"),
				new ScanResult("f2", "62"), new ScanResult("f3", "63"),
				new ScanResult("f4", "64"), new ScanResult("g1", "71"),
				new ScanResult("g2", "72"), new ScanResult("h1", "81"),
				new ScanResult("h2", "82"), new ScanResult("h3", "83"),
				new ScanResult("h4", "84"), new ScanResult("i1", "91"),
				new ScanResult("i2", "92"), new ScanResult("j1", "101"),
				new ScanResult("j2", "102"), new ScanResult("j3", "103"),
				new ScanResult("j4", "104"), new ScanResult("k1", "111"),
				new ScanResult("k2", "112") };
		doInitContainer2();
		doLoadData(inserts);
//		doIsolationTest_ReadCommitted();
//		doIsolationTest_CursorStability();
//		doIsolationTest_RepeatableRead(false);
//		doIsolationTest_RepeatableRead(true);
		doFindKeyLockTest(IsolationMode.READ_COMMITTED, "b13", 21, false);
		doFindKeyLockTest(IsolationMode.READ_COMMITTED, "b13", 21, true);
		doFindKeyLockTest(IsolationMode.READ_COMMITTED, "zz", 21, false);
		doFindKeyLockTest(IsolationMode.CURSOR_STABILITY, "b13", 21, false);
		doFindKeyLockTest(IsolationMode.CURSOR_STABILITY, "b13", 21, true);
		doFindKeyLockTest(IsolationMode.REPEATABLE_READ, "b13", 21, false);
		doFindKeyLockTest(IsolationMode.REPEATABLE_READ, "b13", 21, true);
		doFindKeyLockTest(IsolationMode.SERIALIZABLE, "b13", 21, false);
		doFindKeyLockTest(IsolationMode.SERIALIZABLE, "b13", 21, true);
	}
    
    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTest(new TestBTreeManager("testPageSplitLeafUnique"));
        suite.addTest(new TestBTreeManager("testPageSplitNonLeafUnique2"));
        suite.addTest(new TestBTreeManager("testRestartAndMerge"));
        suite.addTest(new TestBTreeManager("testPageSplitLeafUnique2"));
//        suite.addTest(new TestBTreeManager("testRestartAndLink"));
        suite.addTest(new TestBTreeManager("testRestartLink"));
        suite.addTest(new TestBTreeManager("testRestartDelink"));
        suite.addTest(new TestBTreeManager("testRestartAndLinkAgain"));
        suite.addTest(new TestBTreeManager("testRestartAndRedistribute"));
        suite.addTest(new TestBTreeManager("testRestartAndIncreaseTreeHeight"));
        suite.addTest(new TestBTreeManager("testRestartAndUnlink"));
        suite.addTest(new TestBTreeManager("testRestartAndMergeAgain"));
        suite.addTest(new TestBTreeManager("testRestartAndDecreaseTreeHeight"));
        suite.addTest(new TestBTreeManager("testSimpleInsertAbort"));
        suite.addTest(new TestBTreeManager("testSimpleInsertCommit"));
        suite.addTest(new TestBTreeManager("testInsertSplitRootAbort"));
        suite.addTest(new TestBTreeManager("testInsertSplitRootCommit"));
        suite.addTest(new TestBTreeManager("testInsertSplitRootAbortLogical"));
        suite.addTest(new TestBTreeManager("testInsertUnderflowFig13"));
        suite.addTest(new TestBTreeManager("testInsertUnderflowFig14"));
        suite.addTest(new TestBTreeManager("testInsertUnderflowFig5"));
        suite.addTest(new TestBTreeManager("testInsertUnderflowFig19"));
        suite.addTest(new TestBTreeManager("testInsertUnderflowFig15"));
        suite.addTest(new TestBTreeManager("testInsertNextKeyInNextPage"));
        suite.addTest(new TestBTreeManager("testInsertUnderflowFig17"));
        suite.addTest(new TestBTreeManager("testDelete1"));
        suite.addTest(new TestBTreeManager("testDeleteInsert1"));
        suite.addTest(new TestBTreeManager("testDeleteInsert2"));
        suite.addTest(new TestBTreeManager("testScan1"));
        suite.addTest(new TestBTreeManager("testScan2"));
        suite.addTest(new TestBTreeManager("testScan3"));
        suite.addTest(new TestBTreeManager("testScan4"));
        suite.addTest(new TestBTreeManager("testScanDeleteCrash", true));
        suite.addTest(new TestBTreeManager("testScanAfterCrash", true));
        suite.addTest(new TestBTreeManager("testInsertInOrder"));
        suite.addTest(new TestBTreeManager("testInsertInOrderFromFile"));
        suite.addTest(new TestBTreeManager("testPhantomRecords1"));
        suite.addTest(new TestBTreeManager("testPhantomRecords2"));
        long i = System.currentTimeMillis() % 3;
        i += 5;
        if (i == 0)  
        	suite.addTest(new TestBTreeManager("testMultiThreadedInserts"));
        else if (i == 1)
        	suite.addTest(new TestBTreeManager("testMultiThreadedInsertsRandom"));
        else if (i == 2)
        	suite.addTest(new TestBTreeManager("testMultiThreadedInsertsDescending"));
        suite.addTest(new TestBTreeManager("testIsolation"));
        return suite;
    }

    public static class BTreeDB {
        final LogFactoryImpl logFactory;
        final ObjectRegistry objectFactory;
        final StorageContainerFactory storageFactory;
        final StorageManager storageManager;
        final LatchFactory latchFactory;
        final PageFactory pageFactory;
        final SlottedPageManager spmgr;
        final LockMgrFactory lockmgrFactory;
        final LockManagerImpl lockmgr;
        final LogManagerImpl logmgr;
        final BufferManagerImpl bufmgr;
        final LoggableFactory loggableFactory;
        final TransactionalModuleRegistry moduleRegistry;
		final TransactionManagerImpl trxmgr;
        final FreeSpaceManager spacemgr;
        final BTreeIndexManagerImpl btreeMgr;

        public BTreeDB(boolean create) throws Exception {

    		Properties properties = new Properties();
    		properties.setProperty("log.ctl.1", "ctl.a");
    		properties.setProperty("log.ctl.2", "ctl.b");
    		properties.setProperty("log.groups.1.path", ".");
    		properties.setProperty("log.archive.path", ".");
    		properties.setProperty("log.group.files", "3");
    		if (largeBM) {
        		properties.setProperty("log.file.size", "5242880");
        		properties.setProperty("log.buffer.size", "5242880");
        		properties.setProperty("log.buffer.limit", "4");
        		properties.setProperty("log.flush.interval", "30");
    		}
    		else {
        		properties.setProperty("log.file.size", "16384");
        		properties.setProperty("log.buffer.size", "16384");
        		properties.setProperty("log.buffer.limit", "4");
        		properties.setProperty("log.flush.interval", "5");
    		}
    		properties.setProperty("storage.basePath", "testdata/TestBTreeManager");

    		logFactory = new LogFactoryImpl();
        	if (create) {
        		logFactory.createLog(properties);
        	}
			objectFactory = new ObjectRegistryImpl();
			storageFactory = new FileStorageContainerFactory(properties);
			storageManager = new StorageManagerImpl();
			latchFactory = new LatchFactoryImpl();
			pageFactory = new PageFactoryImpl(objectFactory, storageManager, latchFactory);
			spmgr = new SlottedPageManagerImpl(objectFactory);
			lockmgrFactory = new LockManagerFactoryImpl();
			lockmgr = (LockManagerImpl) lockmgrFactory.create(null);
			logmgr = (LogManagerImpl) logFactory.getLog(properties);
			if (largeBM) {
				logmgr.setDisableExplicitFlushRequests(true);
			}
			bufmgr = new BufferManagerImpl(logmgr, pageFactory, largeBM ? 350 :20, largeBM ? 389 : 11);
			if (largeBM) {
				bufmgr.setBufferWriterSleepInterval(60000);
			}
			loggableFactory = new LoggableFactoryImpl(objectFactory);
			moduleRegistry = new TransactionalModuleRegistryImpl();
			trxmgr = new TransactionManagerImpl(logmgr, storageFactory, storageManager, bufmgr, lockmgr, loggableFactory, latchFactory, objectFactory, moduleRegistry);
			spacemgr = new FreeSpaceManagerImpl(objectFactory, pageFactory, logmgr, bufmgr, storageManager, storageFactory, loggableFactory, trxmgr, moduleRegistry);
			btreeMgr = new BTreeIndexManagerImpl(objectFactory, loggableFactory, spacemgr, bufmgr, spmgr, moduleRegistry);

	    	objectFactory.register(TYPE_STRINGKEYFACTORY, StringKeyFactory.class.getName());
			objectFactory.register(TYPE_ROWLOCATIONFACTORY, RowLocationFactory.class.getName());

			lockmgr.start();
			logmgr.start();
			bufmgr.start();

			if (create) {
				StorageContainer sc = storageFactory.create("dual");
				storageManager.register(0, sc);
				Page page = pageFactory.getInstance(pageFactory
						.getRawPageType(), new PageId(0, 0));
				pageFactory.store(page);
			}
			if (largeBM) {
				trxmgr.setCheckpointInterval(60000);
			}
	    	trxmgr.start();
        }
        
        public void shutdown() {
        	trxmgr.shutdown();
        	bufmgr.shutdown();
			logmgr.shutdown();
			storageManager.shutdown();
			lockmgr.shutdown();
        }
    }
    
}

/*
 Write following tests:
 
 1) Thread 1 inserts a row.
    Thread 2 locks the row exclusively.
    Thread 3 inserts the same key as thread 1 and blocks.
    Thread 2 deletes the row and commits.
    Verify that thread 3 proceeds with the insert and completes successfully.
    
 2) Thread 1 inserts a row.
    Thread 2 locks row exclusively.
    Thread 3 inserts same key as thread 1 and blocks.
    Thread 2 deletes key with old row, and inserts same key with new row and commits.
    Verify that thread 3 releases lock on old row and restarts insert. It should fail if index in unique.
    
 3) Insert an exsting key and verify that shared lock is held in case of RR or CZ, but released in
    case of CS or RC.
 
 4) Start an index scan and position to a row. 
 	Create a savepoint.
 	Scan a few more rows.
 	Rollback to savepoint.
 	Test that the cursor is back on the same row before the savepoint.
 	Test that the cursor has reacquired lock in case of CS.
    Test the situation where row has been deleted in the meantime. Cursor should be positioned on
    next higher key which should be locked in case of CS.
    
 5) Run multithreaded test with random inserts/deletes.
 6) Run multithreaded test with reverse order inserts/deletes.
*/