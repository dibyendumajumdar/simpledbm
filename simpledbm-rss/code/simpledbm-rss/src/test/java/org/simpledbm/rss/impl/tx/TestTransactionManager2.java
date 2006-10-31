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
package org.simpledbm.rss.impl.tx;

import java.nio.ByteBuffer;
import java.util.Properties;

import junit.framework.TestCase;

import org.simpledbm.rss.api.bm.BufferManager;
import org.simpledbm.rss.api.bm.BufferManagerException;
import org.simpledbm.rss.api.bm.BufferAccessBlock;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockManager;
import org.simpledbm.rss.api.locking.LockMgrFactory;
import org.simpledbm.rss.api.locking.LockMode;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageException;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageContainerFactory;
import org.simpledbm.rss.api.st.StorageException;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.api.tx.BaseLoggable;
import org.simpledbm.rss.api.tx.BaseTransactionalModule;
import org.simpledbm.rss.api.tx.Compensation;
import org.simpledbm.rss.api.tx.Loggable;
import org.simpledbm.rss.api.tx.LoggableFactory;
import org.simpledbm.rss.api.tx.NonTransactionRelatedOperation;
import org.simpledbm.rss.api.tx.PageFormatOperation;
import org.simpledbm.rss.api.tx.Redoable;
import org.simpledbm.rss.api.tx.Savepoint;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.api.tx.TransactionalModuleRegistry;
import org.simpledbm.rss.api.tx.TransactionException;
import org.simpledbm.rss.api.tx.TransactionManager;
import org.simpledbm.rss.api.tx.Undoable;
import org.simpledbm.rss.api.wal.LogManager;
import org.simpledbm.rss.api.wal.Lsn;
import org.simpledbm.rss.impl.bm.BufferManagerImpl;
import org.simpledbm.rss.impl.latch.LatchFactoryImpl;
import org.simpledbm.rss.impl.locking.LockManagerFactoryImpl;
import org.simpledbm.rss.impl.pm.PageFactoryImpl;
import org.simpledbm.rss.impl.registry.ObjectRegistryImpl;
import org.simpledbm.rss.impl.st.FileStorageContainerFactory;
import org.simpledbm.rss.impl.st.StorageManagerImpl;
import org.simpledbm.rss.impl.tx.LoggableFactoryImpl;
import org.simpledbm.rss.impl.tx.TransactionalModuleRegistryImpl;
import org.simpledbm.rss.impl.tx.TransactionManagerImpl;
import org.simpledbm.rss.impl.wal.LogFactoryImpl;
import org.simpledbm.rss.util.ByteString;

public class TestTransactionManager2 extends TestCase {

	static final short TYPE_BITMGRPAGE = 25000;
	static final short TYPE_BITMGRLOGCREATECONTAINER = 25001;
	static final short TYPE_BITMGRLOGFORMATPAGE = 25002;
	static final short TYPE_BITMGRLOGREDOUNDO = 25003;
	static final short TYPE_BITMGRLOGCLR = 25004;
	static final short TYPE_BITMGRLOGOPENCONTAINER = 25005;

	public TestTransactionManager2(String arg0) {
		super(arg0);
	}

	void setupObjectFactory(ObjectRegistry objectFactory) {
        objectFactory.register(TYPE_BITMGRPAGE, BitMgrPage.class.getName());
		objectFactory.register(TYPE_BITMGRLOGCREATECONTAINER, BitMgrLogCreateContainer.class.getName());
		objectFactory.register(TYPE_BITMGRLOGFORMATPAGE, BitMgrLogFormatPage.class.getName());
		objectFactory.register(TYPE_BITMGRLOGREDOUNDO, BitMgrLogRedoUndo.class.getName());
		objectFactory.register(TYPE_BITMGRLOGCLR, BitMgrLogCLR.class.getName());
		objectFactory.register(TYPE_BITMGRLOGOPENCONTAINER, BitMgrLogOpenContainer.class.getName());
	}
	
	public void testTrxMgrStart() throws Exception {
		
		/* Create the write ahead log */
		final LogFactoryImpl logFactory = new LogFactoryImpl();
		logFactory.createLog(new Properties());
		
		final ObjectRegistry objectFactory = new ObjectRegistryImpl();
		setupObjectFactory(objectFactory);
        final StorageContainerFactory storageFactory = new FileStorageContainerFactory();
        final StorageManager storageManager = new StorageManagerImpl();
        final LatchFactory latchFactory = new LatchFactoryImpl();
        final PageFactory pageFactory = new PageFactoryImpl(objectFactory,
                storageManager, latchFactory);
        final LockMgrFactory lockmgrFactory = new LockManagerFactoryImpl();
        final LockManager lockmgr = lockmgrFactory.create(null);
        final LogManager logmgr = logFactory.getLog(new Properties());
        logmgr.start();
        final BufferManager bufmgr = new BufferManagerImpl(logmgr, pageFactory, 3, 11);
        bufmgr.start();
        final LoggableFactory loggableFactory = new LoggableFactoryImpl(objectFactory);
        final TransactionalModuleRegistry moduleRegistry = new TransactionalModuleRegistryImpl();
		final TransactionManagerImpl trxmgr = new TransactionManagerImpl(logmgr, storageFactory, storageManager, bufmgr, lockmgr, loggableFactory, latchFactory, objectFactory, moduleRegistry);
		OneBitMgr bitmgr = new OneBitMgr(storageFactory, storageManager, pageFactory, bufmgr, loggableFactory, trxmgr, objectFactory);
        moduleRegistry.registerModule(OneBitMgr.moduleId, bitmgr);
        
        /* Now we are ready to start */
        try {
        	trxmgr.start();
        }
        finally {
        	trxmgr.shutdown();
        	bufmgr.shutdown();
        	logmgr.shutdown();
        	storageManager.shutdown();
        }
	}

	public void testBitMgrCreateContainer() throws Exception {
		
		final LogFactoryImpl logFactory = new LogFactoryImpl();
		final ObjectRegistry objectFactory = new ObjectRegistryImpl();
		setupObjectFactory(objectFactory);
        final StorageContainerFactory storageFactory = new FileStorageContainerFactory();
        final StorageManager storageManager = new StorageManagerImpl();
        final LatchFactory latchFactory = new LatchFactoryImpl();
        final PageFactory pageFactory = new PageFactoryImpl(objectFactory,
                storageManager, latchFactory);
        final LockMgrFactory lockmgrFactory = new LockManagerFactoryImpl();
        final LockManager lockmgr = lockmgrFactory.create(null);
        final LogManager logmgr = logFactory.getLog(new Properties());
        logmgr.start();
        final BufferManager bufmgr = new BufferManagerImpl(logmgr, pageFactory, 3, 11);
        bufmgr.start();
        final LoggableFactory loggableFactory = new LoggableFactoryImpl(objectFactory);
        final TransactionalModuleRegistry moduleRegistry = new TransactionalModuleRegistryImpl();
		final TransactionManagerImpl trxmgr = new TransactionManagerImpl(logmgr, storageFactory, storageManager, bufmgr, lockmgr, loggableFactory, latchFactory, objectFactory, moduleRegistry);
		OneBitMgr bitmgr = new OneBitMgr(storageFactory, storageManager, pageFactory, bufmgr, loggableFactory, trxmgr, objectFactory);
        moduleRegistry.registerModule(OneBitMgr.moduleId, bitmgr);
        
        /* Now we are ready to start */
        try {
        	StorageContainer sc = storageFactory.create("dual");
        	storageManager.register(999, sc);
        	Page page = pageFactory.getInstance(pageFactory.getRawPageType(), new PageId(999, 0));
        	pageFactory.store(page);
        	trxmgr.start();
        	bitmgr.create("bit.dat", 1, 0);
        }
        finally {
        	trxmgr.shutdown();
        	bufmgr.shutdown();
        	logmgr.shutdown();
        	storageManager.shutdown();
        }
        
	}
	
	void printBits(Transaction trx, OneBitMgr bitmgr, int start, int n, int[] values) throws Exception {
		Savepoint sp = trx.createSavepoint();
		try {
			for (int i = start; i < start+n; i++) {
				int bit = bitmgr.getBit(trx, i);
				System.out.println("Bit[" + i + "] = " + bit);
				if (bit != values[i]) {
					throw new Exception("Bit[" + i + "] = " + bit + ", expected value=" + values[i]);
				}
			}
		}
		finally {
			trx.rollback(sp);
		}
	}
	
	public void testBitMgrSingleThread() throws Exception {
		
		final LogFactoryImpl logFactory = new LogFactoryImpl();
		final ObjectRegistry objectFactory = new ObjectRegistryImpl();
		setupObjectFactory(objectFactory);
        final StorageContainerFactory storageFactory = new FileStorageContainerFactory();
        final StorageManager storageManager = new StorageManagerImpl();
        final LatchFactory latchFactory = new LatchFactoryImpl();
        final PageFactory pageFactory = new PageFactoryImpl(objectFactory,
                storageManager, latchFactory);
        final LockMgrFactory lockmgrFactory = new LockManagerFactoryImpl();
        final LockManager lockmgr = lockmgrFactory.create(null);
        final LogManager logmgr = logFactory.getLog(new Properties());
        logmgr.start();
        final BufferManager bufmgr = new BufferManagerImpl(logmgr, pageFactory, 3, 11);
        bufmgr.start();
        final LoggableFactory loggableFactory = new LoggableFactoryImpl(objectFactory);
        final TransactionalModuleRegistry moduleRegistry = new TransactionalModuleRegistryImpl();
		final TransactionManagerImpl trxmgr = new TransactionManagerImpl(logmgr, storageFactory, storageManager, bufmgr, lockmgr, loggableFactory, latchFactory, objectFactory, moduleRegistry);
		OneBitMgr bitmgr = new OneBitMgr(storageFactory, storageManager, pageFactory, bufmgr, loggableFactory, trxmgr, objectFactory);
        moduleRegistry.registerModule(OneBitMgr.moduleId, bitmgr);
    	StorageContainer sc = storageFactory.open("dual");
    	storageManager.register(999, sc);
        
        /* Now we are ready to start */
        try {
        	boolean success = false;
        	trxmgr.start();
        	Transaction trx = trxmgr.begin();
        	try {
        		System.out.println("After restart 0-7");
        		printBits(trx, bitmgr, 0, 7, new int[] {0, 0, 0, 0, 0, 0, 0});
        		bitmgr.changeBit(trx, 0, 10);
        		bitmgr.changeBit(trx, 1, 15);
        		bitmgr.changeBit(trx, 2, 20);
        		bitmgr.changeBit(trx, 3, 25);
        		bitmgr.changeBit(trx, 4, 30);
        		bitmgr.changeBit(trx, 5, 35);
        		bitmgr.changeBit(trx, 6, 40);        
        		success = true;
        	}
        	finally {
        		if (success)
        			trx.commit();
        		else 
        			trx.abort();
        	}
        	trxmgr.checkpoint();

        	trx = trxmgr.begin();
        	try {
        		System.out.println("After commit 0-7");
        		printBits(trx, bitmgr, 0, 7, new int[] {10, 15, 20, 25, 30, 35, 40});
        		bitmgr.changeBit(trx, 0, 11);
        		bitmgr.changeBit(trx, 1, 16);
        		bitmgr.changeBit(trx, 2, 21);
        		bitmgr.changeBit(trx, 3, 26);
        		bitmgr.changeBit(trx, 4, 31);
        		bitmgr.changeBit(trx, 5, 36);
        		bitmgr.changeBit(trx, 6, 41);        
        		success = false;
        	}
        	finally {
        		if (success)
        			trx.commit();
        		else 
        			trx.abort();
        	}

        	trx = trxmgr.begin();
        	try {
        		System.out.println("After rollback 0-7");
        		printBits(trx, bitmgr, 0, 7, new int[] {10, 15, 20, 25, 30, 35, 40});
        		bitmgr.changeBit(trx, 3, 90);
        		bitmgr.changeBit(trx, 4, 99);
        		success = true;
        	}
        	finally {
        		if (success)
        			trx.commit();
        		else 
        			trx.abort();
        	}
        	
           	trx = trxmgr.begin();
        	try {
        		System.out.println("After commit 3-4");
        		printBits(trx, bitmgr, 0, 7, new int[] {10, 15, 20, 90, 99, 35, 40});
           		bitmgr.changeBit(trx, 0, 11);
        		bitmgr.changeBit(trx, 1, 16);
        		bitmgr.changeBit(trx, 2, 21);
        		Savepoint sp = trx.createSavepoint();
        		bitmgr.changeBit(trx, 3, 26);
        		bitmgr.changeBit(trx, 4, 31);
        		trx.rollback(sp);
        		bitmgr.changeBit(trx, 5, 36);
        		bitmgr.changeBit(trx, 6, 41);        
        		System.out.println("After commit 0-2, rollback sp(3-4), change 5-6");
        		printBits(trx, bitmgr, 0, 7, new int[] {11, 16, 21, 90, 99, 36, 41});        		
        		success = true;
        	}
        	catch (Exception e) {
       			trx.abort();
        	}        	
        	// 
        }
        finally {
        	trxmgr.shutdown();
        	bufmgr.shutdown();
        	logmgr.shutdown();
        	storageManager.shutdown();
        }
        
	}	

	public void testBitMgrSingleThreadRestart() throws Exception {
		
		final LogFactoryImpl logFactory = new LogFactoryImpl();
		final ObjectRegistry objectFactory = new ObjectRegistryImpl();
		setupObjectFactory(objectFactory);
        final StorageContainerFactory storageFactory = new FileStorageContainerFactory();
        final StorageManager storageManager = new StorageManagerImpl();
        final LatchFactory latchFactory = new LatchFactoryImpl();
        final PageFactory pageFactory = new PageFactoryImpl(objectFactory,
                storageManager, latchFactory);
        final LockMgrFactory lockmgrFactory = new LockManagerFactoryImpl();
        final LockManager lockmgr = lockmgrFactory.create(null);
        final LogManager logmgr = logFactory.getLog(new Properties());
        logmgr.start();
        final BufferManager bufmgr = new BufferManagerImpl(logmgr, pageFactory, 3, 11);
        bufmgr.start();
        final LoggableFactory loggableFactory = new LoggableFactoryImpl(objectFactory);
        final TransactionalModuleRegistry moduleRegistry = new TransactionalModuleRegistryImpl();
		final TransactionManagerImpl trxmgr = new TransactionManagerImpl(logmgr, storageFactory, storageManager, bufmgr, lockmgr, loggableFactory, latchFactory, objectFactory, moduleRegistry);
		OneBitMgr bitmgr = new OneBitMgr(storageFactory, storageManager, pageFactory, bufmgr, loggableFactory, trxmgr, objectFactory);
        moduleRegistry.registerModule(OneBitMgr.moduleId, bitmgr);
    	StorageContainer sc = storageFactory.open("dual");
    	storageManager.register(999, sc);
    	// FIXME 
    	sc = storageFactory.open("bit.dat");
    	storageManager.register(1, sc);
        
        /* Now we are ready to start */
        try {
        	boolean success = false;
        	trxmgr.start();
        	Transaction trx = trxmgr.begin();
        	try {
        		System.out.println("After restart 0-7");
        		printBits(trx, bitmgr, 0, 7, new int[] {10, 15, 20, 90, 99, 35, 40});
        		success = true;
        	}
        	finally {
        		if (success)
        			trx.commit();
        		else 
        			trx.abort();
        	}
        	trxmgr.checkpoint();
        }
        finally {
        	trxmgr.shutdown();
        	bufmgr.shutdown();
        	logmgr.shutdown();
        	storageManager.shutdown();
        }
	}	
	
	public static class ObjectLock {
		
		static final int CONTAINER = 1;
		static final int BIT = 2;
		
		final int type;
		final int value;
		
		public ObjectLock(int type, int value) {
			this.type = type;
			this.value = value;
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof ObjectLock) {
				ObjectLock lock = (ObjectLock) obj;
				return lock.type == type && lock.value == value;
			}
			return false;
		}

		@Override
		public int hashCode() {
			return type ^ value;
		}

		@Override
		public String toString() {
			return "ObjectLock(type=" + type + ", value = " + value + ")";
		}
	}
	
    public static class BitMgrContainerOperation extends BaseLoggable {

        int containerId;
        ByteString name;
        
        @Override
        public void init() {
            containerId = -1;
            name = new ByteString("");
        }
        
        public final int getContainerId() {
            return containerId;
        }

        public final void setContainerId(int containerId) {
            this.containerId = containerId;
        }

        public final String getName() {
            return name.toString();
        }

        public final void setName(String name) {
            this.name = new ByteString(name);
        }

        @Override
        public int getStoredLength() {
            return super.getStoredLength() + (Integer.SIZE/Byte.SIZE) + name.getStoredLength();
        }

        @Override
        public void retrieve(ByteBuffer bb) {
            super.retrieve(bb);
            containerId = bb.getInt();
            name = new ByteString();
            name.retrieve(bb);
        }

        @Override
        public void store(ByteBuffer bb) {
            super.store(bb);
            bb.putInt(containerId);
            name.store(bb);
        }
    }
    
    public static class BitMgrLogCreateContainer extends BitMgrContainerOperation implements Redoable {
    }

    public static class BitMgrLogOpenContainer extends BitMgrContainerOperation implements NonTransactionRelatedOperation {
    }
	
	public static class BitMgrLogFormatPage extends BaseLoggable implements Redoable, PageFormatOperation {

		@Override
		public void init() {
		}
		
	}
	
	public static class BitMgrLogRedoUndo extends BaseLoggable implements Undoable {

		int index;
		int newValue;
		int oldValue;
		
		static final int SIZE = BaseLoggable.SIZE + (Integer.SIZE/Byte.SIZE) * 3;
		
		@Override
		public void init() {
		}

		public final int getIndex() {
			return index;
		}

		public final void setIndex(int index) {
			this.index = index;
		}

		public final int getNewValue() {
			return newValue;
		}

		public final void setNewValue(int newValue) {
			this.newValue = newValue;
		}

		public final int getOldValue() {
			return oldValue;
		}

		public final void setOldValue(int oldValue) {
			this.oldValue = oldValue;
		}

		@Override
		public int getStoredLength() {
			return SIZE;
		}

		@Override
		public void retrieve(ByteBuffer bb) {
			super.retrieve(bb);
			index = bb.getInt();
			newValue = bb.getInt();
			oldValue = bb.getInt();
		}

		@Override
		public void store(ByteBuffer bb) {
			super.store(bb);
			bb.putInt(index);
			bb.putInt(newValue);
			bb.putInt(oldValue);
		}
	}

	public static class BitMgrLogCLR extends BaseLoggable implements Compensation {

		int index;
		int newValue;
		
		static final int SIZE = BaseLoggable.SIZE + (Integer.SIZE/Byte.SIZE) * 2;
		
		@Override
		public void init() {
		}

		public final int getIndex() {
			return index;
		}

		public final void setIndex(int index) {
			this.index = index;
		}

		public final int getNewValue() {
			return newValue;
		}

		public final void setNewValue(int newValue) {
			this.newValue = newValue;
		}

		@Override
		public int getStoredLength() {
			return SIZE;
		}

		@Override
		public void retrieve(ByteBuffer bb) {
			super.retrieve(bb);
			index = bb.getInt();
			newValue = bb.getInt();
		}

		@Override
		public void store(ByteBuffer bb) {
			super.store(bb);
			bb.putInt(index);
			bb.putInt(newValue);
		}
	}
	
	public static class BitMgrPage extends Page {

		byte[] bits;
		
		@Override
		public void init() {
			int n_bits = super.getStoredLength() - Page.SIZE;
			bits = new byte[n_bits];
		}

		@Override
		public void retrieve(ByteBuffer bb) {
			super.retrieve(bb);
			bb.get(bits);
		}

		@Override
		public void store(ByteBuffer bb) {
			super.store(bb);
			bb.put(bits);
		}
		
		public void setBit(int offset, byte value) {
			bits[offset] = value;
		}
		
		public int getBit(int offset) {
			return bits[offset];
		}
		
	}
	
	public static class OneBitMgr extends BaseTransactionalModule {
		
		static final short moduleId = 10;
		
		BufferManager bufmgr;
		
		LogManager logmgr;
		
		LoggableFactory loggableFactory;

		PageId pageId = new PageId(1, 0);
		
		StorageContainerFactory storageFactory;
		
		StorageManager storageManager;
		
		TransactionManager trxmgr;

		PageFactory pageFactory;
		
		ObjectRegistry objectFactory;
		
		public OneBitMgr(StorageContainerFactory storageFactory, StorageManager storageManager, PageFactory pageFactory, BufferManager bufmgr, LoggableFactory loggableFactory, TransactionManager trxmgr, ObjectRegistry objectFactory) {
			this.storageFactory = storageFactory;
			this.storageManager = storageManager;
			this.pageFactory = pageFactory;
			this.bufmgr = bufmgr;
			this.loggableFactory = loggableFactory;
			this.objectFactory = objectFactory;
			this.trxmgr = trxmgr;
		}
		
		@Override
		public Compensation generateCompensation(Undoable undoable) {
			if (undoable instanceof BitMgrLogRedoUndo) {
				BitMgrLogRedoUndo logrec = (BitMgrLogRedoUndo) undoable;
			
				BitMgrLogCLR clr = (BitMgrLogCLR) loggableFactory.getInstance(OneBitMgr.moduleId, TestTransactionManager2.TYPE_BITMGRLOGCLR);
				clr.index = logrec.index;
				clr.newValue = logrec.oldValue;
				System.out.println("UNDO: Setting bit[" + clr.index + "] to " + clr.newValue);
				return clr;
			}
			return null;
		}
		
		@Override
		public void redo(Loggable loggable) throws StorageException {
			if (loggable instanceof BitMgrLogOpenContainer) {
				BitMgrLogOpenContainer logrec = (BitMgrLogOpenContainer) loggable;
				StorageContainer sc = storageManager.getInstance(logrec.getContainerId()); 
				if (sc == null) {
					sc = storageFactory.open(logrec.getName());
					storageManager.register(logrec.getContainerId(), sc);
				}
			}
		}

		@Override
		public void redo(Page page, Redoable loggable) throws StorageException, PageException {
			if (loggable instanceof BitMgrLogCreateContainer) {
				BitMgrLogCreateContainer logrec = (BitMgrLogCreateContainer) loggable;
				StorageContainer sc = storageFactory.create(logrec.getName());
				storageManager.register(logrec.getContainerId(), sc);
			}
			else if (loggable instanceof BitMgrLogRedoUndo) {
				BitMgrLogRedoUndo logrec = (BitMgrLogRedoUndo) loggable;
				System.out.println("REDO: Setting bit[" + logrec.index + "] to " + logrec.newValue);
				BitMgrPage bpage = (BitMgrPage) page;
				bpage.setBit(logrec.index, (byte) logrec.newValue);
			}
			else if (loggable instanceof BitMgrLogFormatPage) {
				page.setPageLsn(loggable.getLsn());
				pageFactory.store(page);
			}
			else if (loggable instanceof BitMgrLogCLR) {
				BitMgrLogCLR logrec = (BitMgrLogCLR) loggable;
				System.out.println("REDO (CLR): Setting bit[" + logrec.index + "] to " + logrec.newValue);
				BitMgrPage bpage = (BitMgrPage) page;
				bpage.setBit(logrec.index, (byte) logrec.newValue);
			}
		}
		
		public void changeBit(Transaction trx, int index, int newValue) throws BufferManagerException, TransactionException {
			trx.acquireLock(new ObjectLock(ObjectLock.CONTAINER, pageId.getContainerId()), LockMode.SHARED, LockDuration.MANUAL_DURATION);
			trx.acquireLock(new ObjectLock(ObjectLock.BIT, index), LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION);
			BitMgrLogRedoUndo logrec = (BitMgrLogRedoUndo) loggableFactory.getInstance(OneBitMgr.moduleId, TestTransactionManager2.TYPE_BITMGRLOGREDOUNDO);
			BufferAccessBlock bab = bufmgr.fixExclusive(pageId, false, TestTransactionManager2.TYPE_BITMGRPAGE, 0);
			try {
				BitMgrPage page = (BitMgrPage) bab.getPage();
				
				logrec.index = index;
				logrec.newValue = newValue;
				logrec.oldValue = page.getBit(index);
				System.out.println("DO: Setting bit[" + logrec.index + "] to " + logrec.newValue);
				
				// logrec.setPageId(objectFactory.getTypeCode("bitmgrpage"), pageId);
				Lsn lsn = trx.logInsert(page, logrec);
				page.setBit(logrec.index, (byte) logrec.newValue);
				bab.setDirty(lsn);
			}
			finally {
				bab.unfix();
			}
		}
		
		public int getBit(Transaction trx, int index) throws BufferManagerException, TransactionException {
			trx.acquireLock(new ObjectLock(ObjectLock.CONTAINER, pageId.getContainerId()), LockMode.SHARED, LockDuration.MANUAL_DURATION);
			trx.acquireLock(new ObjectLock(ObjectLock.BIT, index), LockMode.SHARED, LockDuration.MANUAL_DURATION);
			BufferAccessBlock bab = bufmgr.fixShared(pageId, 0);
			int value = -1;
			try {
				BitMgrPage page = (BitMgrPage) bab.getPage();
				value = page.getBit(index);
			}
			finally {
				bab.unfix();
			}
			return value;
		}
		
		public int getBitForUpdate(Transaction trx, int index) throws BufferManagerException, TransactionException {
			trx.acquireLock(new ObjectLock(ObjectLock.CONTAINER, pageId.getContainerId()), LockMode.SHARED, LockDuration.MANUAL_DURATION);
			trx.acquireLock(new ObjectLock(ObjectLock.BIT, index), LockMode.UPDATE, LockDuration.MANUAL_DURATION);
			BufferAccessBlock bab = bufmgr.fixShared(pageId, 0);
			int value = -1;
			try {
				BitMgrPage page = (BitMgrPage) bab.getPage();
				value = page.getBit(index);
			}
			finally {
				bab.unfix();
			}
			return value;
		}
		
		public void create(String name, int containerId, int pageNumber) throws StorageException, BufferManagerException, TransactionException, PageException {
			
			Transaction trx = trxmgr.begin();
			boolean success = false;
			try {
				BitMgrLogCreateContainer logcreate = (BitMgrLogCreateContainer) loggableFactory.getInstance(OneBitMgr.moduleId, TestTransactionManager2.TYPE_BITMGRLOGCREATECONTAINER);
				BitMgrLogOpenContainer logopen = (BitMgrLogOpenContainer) loggableFactory.getInstance(OneBitMgr.moduleId, TestTransactionManager2.TYPE_BITMGRLOGOPENCONTAINER);
				BitMgrLogFormatPage formatpage = (BitMgrLogFormatPage) loggableFactory.getInstance(OneBitMgr.moduleId, TestTransactionManager2.TYPE_BITMGRLOGFORMATPAGE);
				
				trx.acquireLock(new ObjectLock(ObjectLock.CONTAINER, containerId), LockMode.EXCLUSIVE, LockDuration.MANUAL_DURATION);
				/* check if the container already exists */
				StorageContainer sc = storageManager.getInstance(containerId);
				if (sc != null) {
					throw new StorageException(); // TODO change exception type
				}
				
				BufferAccessBlock bab1 = bufmgr.fixExclusive(new PageId(999, 0), true, pageFactory.getRawPageType(), 0);
				try {
					Page page1 = bab1.getPage();
					logcreate.setContainerId(containerId);
					logcreate.setName(name);
					Lsn mylsn = trx.logInsert(page1, logcreate);
					logcreate.setLsn(mylsn);
					redo(page1, logcreate);

					logopen.setContainerId(containerId);
					logopen.setName(name);
//					mylsn = trx.logInsert(page1, logopen);
//					logopen.setLsn(mylsn);
//					redo(logopen);
//					bab1.setDirty(mylsn);
					trxmgr.logNonTransactionRelatedOperation(logopen);
					
					PageId pageId = new PageId(containerId, pageNumber);
					formatpage.setPageId(TYPE_BITMGRPAGE, pageId);
					BufferAccessBlock bab = bufmgr.fixExclusive(pageId, true, TestTransactionManager2.TYPE_BITMGRPAGE, 0);
					try {
						Page page = bab.getPage();
						mylsn = trx.logInsert(page, formatpage);
						formatpage.setLsn(mylsn);
						redo(page, formatpage);
						bab.setDirty(mylsn);
					}				
					finally {
						bab.unfix();
					}
				}
				finally {
					bab1.unfix();
				}
			}
			finally {
				/*
				 * To test we deliberatly do not set success to true, causing the
				 * transaction to rollback. 
				 */
				if (success) {
					trx.commit();
				}
				else {
					trx.abort();
				}
			}
		}
		
	}
	
}
