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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.simpledbm.rss.api.bm.BufferAccessBlock;
import org.simpledbm.rss.api.bm.BufferManager;
import org.simpledbm.rss.api.bm.BufferManagerException;
import org.simpledbm.rss.api.bm.DirtyPageInfo;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageException;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.api.wal.LogException;
import org.simpledbm.rss.api.wal.LogManager;
import org.simpledbm.rss.api.wal.Lsn;
import org.simpledbm.rss.util.Linkable;
import org.simpledbm.rss.util.SimpleLinkedList;
import org.simpledbm.rss.util.logging.Logger;

/**
 * Implements the Buffer Manager module. This implementation uses a global
 * LRU chain. The intention is to however allow hints that enable the client
 * to influence where a page is placed within the LRU chain.
 * <p>
 * The Buffer Manager implementation is based upon the description provided
 * in <cite>Transaction Processing: Concepts and Techniques, by Jim Gray
 * and Andreas Reuter.</cite>
 * </p>
 * 17-Sep-2005: In order to support requirements for BTree implementation, page latches can
 * be acquired in update mode in addition to exclusive and shared modes. 
 * 
 * @author Dibyendu Majumdar
 * @since 15-Aug-2005
 */
public final class BufferManagerImpl implements BufferManager {

	private static final String BUFFERPOOL_NUMBUFFERS = "bufferpool.numbuffers";

	private static final String LOG_CLASS_NAME = BufferManagerImpl.class.getName();

	static final Logger log = Logger.getLogger(BufferManagerImpl.class.getPackage().getName());

	private static final int LATCH_EXCLUSIVE = 2;
	
	private static final int LATCH_UPDATE = 3;
	
	private static final int LATCH_SHARED = 1;

    /**
     * Buffer Manager does not directly depend upon the Storage Manager, but
     * if it has access to the StorageManager then it can validate page fix requests.
     * This is useful when running tests.
     */
    private StorageManager storageManager;
    
	/**
	 * The page factory instance that will manage IO of pages.
	 */
	private PageFactory pageFactory;

	/**
	 * Log Manager instance, may be set to null for testing
	 * purposes.
	 */
	private LogManager logMgr;

	/**
	 * The BufferPool is an array of slots where pages can be held. Each slot is
	 * called a frame (using terminology from the book <cite>Transaction
	 * Processing: Concepts and Techniques</cite>).
	 */
	private Page[] bufferpool;

	/**
	 * To enable quick retrieval of cached pages, a hash table of BCBs is
	 * maintained. All pages that are in the cache must be present in the hash
	 * table.
	 */
	private BufferHashBucket[] bufferHash;

	/**
	 * Pages in the cache are also put into the LRU list. The head of the list
	 * is the LRU end, whereas the tail is the MRU.
	 */
	private final SimpleLinkedList<BufferControlBlock> lru = new SimpleLinkedList<BufferControlBlock>();

	/*
     * Notes on latching:
	 * In this implementation, the latching order is always: 
	 * LRU chain -> Hash bucket
	 *
	 * Page latch is completely outside of the BufMgr processing logic.
	 */

	/**
	 * LRU latch protects the LRU. 
	 */
	private final ReentrantReadWriteLock lruLatch = new ReentrantReadWriteLock();

	/**
	 * Background thread for writing dirty pages.
	 */
	private Thread bufferWriter;

	/**
	 * Causes the Buffer Manager to shutdown. 
	 */
	volatile boolean stop = false;

	/**
	 * Points to the top of the {@link #freeFrames} stack.
	 */
	private volatile int nextAvailableFrame = -1;

	/**
	 * A stack of available free slots in the buffer pool.
	 * {@link #nextAvailableFrame} points to the top of the stack.
	 */
	private int[] freeFrames;

	/**
	 * The interval in milli bseconds for which the Buffer Writer
	 * thread sleeps between each write.
	 */
	private int bufferWriterSleepInterval = 5000;

	/**
	 * The interval for which a client will wait for
	 * the Buffer Writer thread to complete.
	 */
	private final int bufferWriterMaxWait = 1000;

	/**
	 * A count of number of pages estimated to be
	 * dirty.
	 */
	volatile int dirtyBuffersCount = 0;

	/**
	 * Used for managing synchronisation between Buffer Writer
	 * thread and clients.
	 */
	final Object waitingForBuffers = new Object();

	/**
	 * Used for managing synchronisation between Buffer Writer
	 * thread and clients.
	 */
	final Object bufferWriterLatch = new Object();

    /**
     * Define the number of times the BufMgr will retry when it cannot locate
     * an empty frame. Each time, it retries, the Buffer Writer will be triggered.
     */
    private int maxRetriesDuringBufferWait = 10;

	static final int hashPrimes[] = {
		53, 97, 193, 389, 769, 1543, 3079, 6151, 12289, 24593, 49157,
		98317, 196613, 393241, 786433
	};

    private int getNumericProperty(Properties props, String name, int defaultValue) {
    	String value = props.getProperty(name);
    	if (value != null) {
    		try {
				int returnValue = Integer.parseInt(value);
				return returnValue;
			} catch (NumberFormatException e) {
				// Ignore and return default value
			}
    	}
    	return defaultValue;
    }
    
    /**
     * Initialize the Buffer Manager instance.
     */
    private void init(LogManager logMgr, PageFactory pageFactory, int bufferpoolsize) {
		this.logMgr = logMgr;
		this.pageFactory = pageFactory;
		bufferpool = new Page[bufferpoolsize];
		freeFrames = new int[bufferpoolsize];
		for (int f = 0; f < bufferpoolsize; f++) {
			freeFrames[++nextAvailableFrame] = f;
		}
		int h = 0; 
		for (;h < hashPrimes.length; h++) {
			if (hashPrimes[h] > bufferpoolsize) {
				break;
			}
		}
		if (h == hashPrimes.length) {
			h = hashPrimes.length-1;
		}
		int hashsize = hashPrimes[h];
		bufferHash = new BufferHashBucket[hashsize];
		for (int i = 0; i < hashsize; i++) {
			bufferHash[i] = new BufferHashBucket();
		}
		// Setup background thread for writer
		bufferWriter = new Thread(new BufferWriter(this), "BufferWriter");
	}

    
	/**
	 * Create a Buffer Manager instance.
	 * 
	 * @param logMgr Log Manager intance, may be null for testing purspose.
	 * @param pageFactory PageFactory instance, required.
	 * @param bufferpoolsize The size of the buffer pool.
	 * @param hashsize Size of the hash table, should preferably be a prime number.
	 */
	public BufferManagerImpl(LogManager logMgr, PageFactory pageFactory, int bufferpoolsize, int hashsize) {
		init(logMgr, pageFactory, bufferpoolsize);
	}

	/**
	 * Create a Buffer Manager instance.
	 * 
	 * @param logMgr Log Manager intance, may be null for testing purspose.
	 * @param pageFactory PageFactory instance, required.
	 * @param props Properties that specify how the Buffer Manager should be configured
	 */
	public BufferManagerImpl(LogManager logMgr, PageFactory pageFactory, Properties props) {
		int bufferpoolsize = getNumericProperty(props, BUFFERPOOL_NUMBUFFERS, 500);
		init(logMgr, pageFactory, bufferpoolsize);
	}
	
	/* (non-Javadoc)
	 * @see org.simpledbm.rss.bm.BufMgr#start()
	 */
	public void start() {
		bufferWriter.start();
	}
	
	/**
	 * Wakes up the Buffer Writer thread if necesary, and waits for it to
	 * finish.
	 */
	public void signalBufferWriter() {
		if (log.isTraceEnabled()) {
			log.trace(LOG_CLASS_NAME, "signalBufferWriter", "SIGNALLING Buffer Writer");
		}
//		synchronized (bufferWriterLatch) {
//			bufferWriterLatch.notify();
//		}
		LockSupport.unpark(bufferWriter);
	}

	/**
	 * Waits for some buffers to become available
     * FIXME: This does not work very reliably. 
	 */
	private void waitForFreeBuffers() {
		long start = System.currentTimeMillis();
		signalBufferWriter();
		for (;;) {
			synchronized (waitingForBuffers) {
				try {
					waitingForBuffers.wait(bufferWriterMaxWait);
				} catch (InterruptedException e) {
					// ignore
				}
			}
			if (stop || dirtyBuffersCount < bufferpool.length) {
				break;
			}
			signalBufferWriter();
		}
		long end = System.currentTimeMillis();
		if (log.isTraceEnabled()) {
			log.trace(LOG_CLASS_NAME, "waitForFreeBuffers", "WAITED " + (end - start) + " millisecs for Buffer Writer");
		}
	}

	/**
	 * Shuts down the Buffer Manager instance.
	 */
	public void shutdown() {
		// System.err.println("Shutting down");
		stop = true;
		signalBufferWriter();
		try {
			bufferWriter.join();
		} catch (InterruptedException e) {
			log.error(LOG_CLASS_NAME, "shutdown", "SIMPLEDBM-LOG: Error occurred while shutting down Buffer Manager", e);
		}
		writeBuffers();
	}

	/**
	 * Get an empty frame.
	 * <p>
	 * Algorithm:
	 * <ol>
	 * <li> First, check if there is an unused frame that can be used. </li>
	 * <li> If not, we need to identify a page that can be replaced. This page
	 * should ideally be the least recently used page which is unpinned and not
	 * dirty. Scan the LRU list for a page that qualifies for replacement. </li>
	 * <li> If found, remove the page BCB from the Hash chain, and from the LRU
	 * list, and return frame index previously occupied by the page. </li>
	 * <li> If a replacement victim wasn't found, trigger the buffer writer
	 * thread and restart the process. The whole process should be repeated a
	 * few times, and if after all the attempts, a page is still not found, then
	 * throw an exception.</li>
	 * </ol>
	 * <p>
	 * TODO: At present we scan the whole of LRU list before deciding to invoke
	 * Buffer writes - should we scan only a percentage or even just keep stats
	 * on percentage dirty pages?
	 * </p>
	 * <p>
	 * TEST CASE: test the free frames array, a) until it is exhausted, and b)
	 * when frames are put back into it.
	 * </p>
	 */
	private int getFrame() {
		int frameNo = -1;

		/* Check if there is a free frame that can be used */
		synchronized (freeFrames) {
			if (nextAvailableFrame >= 0) {
				frameNo = freeFrames[nextAvailableFrame--];
				return frameNo;
			}
		}
		/*
		 * Find a replacement victim - this should be the Least Recently Used
		 * Buffer that is unfixed.
		 */
		for (int retryAttempt = 0; retryAttempt < maxRetriesDuringBufferWait; retryAttempt++) {

			if (log.isTraceEnabled()) {
				log.trace(LOG_CLASS_NAME, "getFrame", "Scanning LRU chain, " + retryAttempt + " attempt");
			}

			lruLatch.writeLock().lock();
			try {
                Iterator<BufferControlBlock> iterator = lru.iterator();
                while (iterator.hasNext()) {
                    BufferControlBlock nextBcb = iterator.next();
					PageId pageid = nextBcb.pageId;
					int h = pageid.hashCode() % bufferHash.length;
					BufferHashBucket bucket = bufferHash[h];

					/*
					 * The LRU latch always obtained before the bucket latch. 
                     * Hence it is safe to wait unconditionally for the
					 * bucket k.
					 */
					bucket.lock();

					try {
						/* In case the BCB has changed then skip */
						if (!nextBcb.pageId.equals(pageid)) {
							if (log.isDebugEnabled()) {
								log
										.debug(
												LOG_CLASS_NAME,
												"getFrame",
												"Skipping bcb "
														+ nextBcb
														+ " because page id has changed");
							}
							continue;
						}

						/*
						 * If the buffer is pinned or is waiting for IO, then
						 * skip
						 */
						if (nextBcb.isValid()
								&& (nextBcb.isInUse() || nextBcb.isDirty()
										|| nextBcb.isBeingRead() || nextBcb.isBeingWritten())) {
							if (log.isDebugEnabled()) {
								log.debug(
									LOG_CLASS_NAME,	"getFrame",	"Skipping bcb "
										+ nextBcb + " because fixCount > 0 or dirty or IO in progress");
							}
							continue;
						}

						if (log.isDebugEnabled()) {
							log.debug(LOG_CLASS_NAME, "getFrame", "Bcb "
									+ nextBcb + " chosen as relacement victim");
						}
						/* Remove BCB from LRU Chain */
						iterator.remove();
						/* Remove BCB from Hash Chain */
						bucket.chain.remove(nextBcb);
						bufferpool[nextBcb.frameIndex] = null;
						return nextBcb.frameIndex;

					} finally {
						bucket.unlock();
					}
				}
			} finally {
				lruLatch.writeLock().unlock();
			}

            /**
             * Trigger Buffer Writer and wait for some buffers to be freed.
             */
			waitForFreeBuffers();

		}

		return -1;
	}

	/**
	 * Read a page into the Buffer Cache. Algorithm: 
	 * <ol>
	 * <li>Allocate a new Buffer Control Block.</li>
	 * <li>Initialize it and attach to the appropriate Hash chain.</li>
	 * <li>Obtain a frame (slot) for the page by calling {@link #getFrame()}.</li>
	 * <li>If the page is new, instantiate a new page, else, read it from the disk.</li>
	 * </ol>
	 * @return The newly allocated BufferControlBlock or null to indicate that the caller must retry.
	 */
	private BufferControlBlock locatePage(PageId pageId, int hashCode, boolean isNew, int pagetype, int latchMode) {

		BufferControlBlock nextBcb = new BufferControlBlock();
		BufferHashBucket bucket = bufferHash[hashCode];
		bucket.lock();
		try {

			/**
			 * 16-feb-03: The gap between releasing the shared latch and
			 * acquiring it in exclusive mode here means that some other thread
			 * may have read the page we are trying to read. (I hit this problem
			 * while testing - just goes to show that no amount of testing
			 * is ever enough !). To work around this situation, we check the
			 * the hash chain again.
			 * Dec-2006: We need to additionally check whether the page is
			 * currently beig read or written, in which case, we need to retry.
			 */

			for (BufferControlBlock bcb : bucket.chain) {
				if (bcb.isValid() && bcb.pageId.equals(pageId)) {
					if (log.isDebugEnabled()) {
						log.debug(LOG_CLASS_NAME, "locatePage", "Page " + pageId + " has been read in the meantime");
					}
					// Bug discovered when testing on Intel CoreDuo (3 Dec 2006) - if the page is being read
					// we need to wait for the read to be over. Since we need to give up latches
					// easiest option is to retry. 
					// For now, we return null to the caller indicate that this must be retried
					// FIXME - need to do this in a better way
					// Another bug: 16 Dec 2006
					// We need to check for writeInProgress as well.
					// we allow SHARED access if writeInProgress is true.
					if (bcb.isBeingRead() || (bcb.isBeingWritten() && latchMode != LATCH_SHARED)) {
						// System.err.println(Thread.currentThread().getName() + ": Page " + bcb.pageId + " is being read, so we need to retry");
						return null;
					}
					else {
						return bcb;
					}
				}
			}

			nextBcb.pageId = pageId;
			/*
			 * frameIndex set to -1 indicates that the page is being read in.
			 */
			nextBcb.frameIndex = -1;

			bucket.chain.addFirst(nextBcb);
		} finally {
			bucket.unlock();
		}

		/*
		 * If any other thread attempts to read the same page, they will find
		 * the BCB we have just inserted. However, they will notice the
		 * io_in_progress flag and wait for us to finish IO.
		 */

		/* Get an empty frame */
		int frameNo = getFrame();

		if (frameNo == -1) {
			stop = true;
			// TODO Add proper error message
			throw new BufferManagerException();
		}

		assert bufferpool[frameNo] == null;
		
		if (isNew) {
			bufferpool[frameNo] = pageFactory.getInstance(pagetype, pageId);
		} else {
			boolean readOk = false;
			try {
				bufferpool[frameNo] = pageFactory.retrieve(pageId);
				readOk = true;
			} finally {
				if (!readOk) {
					/*
					 * This is not neat, but we want to leave things in a tidy
					 * state. We want to remove the invalid BCB from the hash chain,
					 * and return the buffer pool frame to the freelist. 
					 */
					log.error(LOG_CLASS_NAME, "locatePage", "Error occurred while attempting to read page " + pageId);
					bucket.lock();
					try {
						bucket.chain.remove(nextBcb);
						synchronized (freeFrames) {
							freeFrames[++nextAvailableFrame] = frameNo;
						}
					} finally {
						bucket.unlock();
					}
				}
			}
		}

		assert nextBcb.pageId.equals(bufferpool[frameNo].getPageId());
		/*
		 * Read complete
		 */
		bucket.lock();
		try {
			nextBcb.frameIndex = frameNo;
		}
		finally {
			bucket.unlock();
		}

		return nextBcb;
	}

	/**
	 * Check that the Buffer Manager is still valid.
	 * 
	 * @throws BufferManagerException
	 */
	private void checkStatus() {
		if (stop) {
			throw new BufferManagerException();
		}
	}

	/**
	 * Fix and lock a page in buffer pool. Algorithm: 
	 * <ol>
	 * <li>Check if the requested page is already present in the buffer pool.</li>
	 * <li>If not, call {@link #locatePage} to read the page in.</li>
	 * <li>Increment fix count and allocate a new Buffer Access Block.</li>
	 * <li>Add the BCB to the LRU chain</li>
	 * <li>Acquire page latch as requested.</li>
	 * </ol>
	 * @throws StorageException 
	 */
	private BufferAccessBlock fix(PageId pageid, boolean isNew, int pagetype, int latchMode, int hint) {

		checkStatus();
        
        if (storageManager != null) {
            // Validate that we are fixing page for a container that exists.
            StorageContainer sc = storageManager.getInstance(pageid.getContainerId());
            assert sc != null;
        }

		boolean found = false;
		BufferControlBlock nextBcb = null;
		int h = pageid.hashCode() % bufferHash.length;
		BufferHashBucket bucket = bufferHash[h];

		search_again: for (;;) {

			for (;;) {
				found = false;
				bucket.lock();
				for (BufferControlBlock bcb : bucket.chain) {
                    /*
                     * Ignore invalid pages.
                     */
					if (bcb.isValid() && bcb.pageId.equals(pageid)) {
						found = true;
						/*
						 * A frameIndex of -1 indicates that the page is being
						 * read in. We must wait if this is the case.
						 * If the page is being written out (writeInProgress ==
						 * true) we must not allow exclusive access to it
						 * until the IO is complete. However, a
						 * non-exclusive access (for reading) is permitted.
						 * ISSUE: 17-Sep-05
						 * We treat an update mode access as an exclusive request because
						 * we do not know whether the page will subsequently be latched
						 * exclusively or not.  
						 */
						if (!bcb.isBeingRead() && (latchMode == LATCH_SHARED || !bcb.isBeingWritten())) {
							nextBcb = bcb;
							/*
							 * we leave the BCB exclusively latched and
							 * bucket latched in shared mode
							 */
							break search_again;
						}
						break;
					}
				}
				bucket.unlock();
				if (!found) {
					break;
				}
				else {
					Thread.yield();
				}
			}

			if (!found) {
				/*
				 * Page not found - must read it in
				 */
				nextBcb = null;
				while (nextBcb == null && !stop) {
					Thread.yield();
					nextBcb = locatePage(pageid, h, isNew, pagetype, latchMode);
				}
				if (stop || nextBcb == null) {
					throw new BufferManagerException();
				}
				bucket.lock();
				if (!nextBcb.pageId.equals(pageid)) {
					bucket.unlock();
				} else {
					break;
				}
			}
		}

		/*
		 * At this point the Hash chain should be latched in SHARED mode and BCB
		 * should be latched in EXCLUSIVE mode
		 */

		assert nextBcb.frameIndex != -1;
		assert nextBcb.pageId.equals(pageid);
		assert bucket.latch.isHeldByCurrentThread();
		
		/*
		 * Now that we have got the desired page, a) increment fix count. b)
		 * Make this page the most recently used one c) release all latches d)
		 * Acquire user requested page latch.
		 */
		nextBcb.fixcount++;
		/*
		 * ISSUE: 17-Sep-05
		 * We set the recoveryLsn if an update mode access is requested because
		 * we do not know whether the page will subsequently be latched
		 * exclusively or not.  
		 */
		if (latchMode == LATCH_EXCLUSIVE || latchMode == LATCH_UPDATE) {
			if (nextBcb.recoveryLsn.isNull() && logMgr != null) {
				nextBcb.recoveryLsn = logMgr.getMaxLsn();
			}
		}
		/* Allocate a Buffer Access Block and initialize it */
		BufferAccessBlockImpl bab = new BufferAccessBlockImpl(this, nextBcb, bufferpool[nextBcb.frameIndex]);

		/*
		 * All latches must be released before we acquire the user requested
		 * page latch
		 */
		bucket.unlock();

		/* 
		 * At this point the page is in the hash table, but not in LRU.
		 * This means it can be found by clients, but may be invisible to the
		 * page replacement logic. However this is not a problem because:
		 * a) If this is not the first time this page is being accessed,
		 * then it must already be on the LRU list and therefore must be
		 * visible to the page replacement and buffer writer algorithms.
		 * This is true also when the page is dirty.
		 * b) The only time this page will not exist in the LRU if it
		 * as been just read in or is a new page. In that case, the page
		 * cannot be dirty, and its fixcount is at least 1, so either way
		 * the page is not eligible for dirty pages list or for writing out.
		 * 
		 * Make this page the most recently used page 
		 */
		lruLatch.writeLock().lock();
		try {
			// TODO: This is inefficient, as remove searches through the entire
			// list. Must implement a custom linked list :-(
			if (hint == 0) {
				if (lru.getLast() != nextBcb) {
					if (nextBcb.isMember()) {
						lru.remove(nextBcb);
					}
					lru.addLast(nextBcb);
				}
			} else {
				if (lru.getFirst() != nextBcb) {
					if (nextBcb.isMember()) {
						lru.remove(nextBcb);
					}
					lru.addFirst(nextBcb);
				}
			}
		} finally {
			lruLatch.writeLock().unlock();
		}

		if (latchMode == LATCH_EXCLUSIVE) {
			bab.latchExclusively();
		}
		else if (latchMode == LATCH_UPDATE) {
			bab.latchForUpdate();
		} else {
			bab.latchShared();
		}

		assert bab.bcb.pageId.equals(bab.page.getPageId());
		
		return bab;
	}

	/* (non-Javadoc)
	 * @see org.simpledbm.bm.BufMgr#fixShared(org.simpledbm.pm.PageId, int)
	 */
	public BufferAccessBlock fixShared(PageId pageid, int hint) {
		return fix(pageid, false, -1, LATCH_SHARED, hint);
	}

	/* (non-Javadoc)
	 * @see org.simpledbm.bm.BufMgr#fixExclusive(org.simpledbm.pm.PageId, boolean, java.lang.String, int)
	 */
	public BufferAccessBlock fixExclusive(PageId pageid, boolean isNew,
			int pagetype, int hint) {
		return fix(pageid, isNew, pagetype, LATCH_EXCLUSIVE, hint);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.simpledbm.bm.BufMgr#fixForUpdate(org.simpledbm.pm.PageId, int)
	 */
	public BufferAccessBlock fixForUpdate(PageId pageid, int hint) {
		return fix(pageid, false, -1, LATCH_UPDATE, hint);
	}
	
	
	/**
	 * Unfix a Buffer. Algorithm:
	 * <ol>
	 * <li>Release page latch.</li>
	 * <li>Set dirty flag.</li>
	 * <li>Decrement fix count.</li>
	 * </ol>
	 * 
	 * @throws BufferManagerException
	 */
	void unfix(BufferAccessBlockImpl bab) {

		BufferControlBlock bcb = bab.bcb;
		int h = bcb.pageId.hashCode() % bufferHash.length;
		BufferHashBucket bucket = bufferHash[h];
		bucket.lock();
		/*
		 * Note that if the page is being written out, we must not set the dirty
		 * flag. TEST CASE: Test the situation where a page is unfixed while it
		 * is being written out.
		 * FIXME: The test for writeInProgress is probably redundant as the
		 * Buffer Writer will only steal pages with fixcount == 0. 
		 */
		if (bab.dirty && !bcb.isBeingWritten() && !bcb.isDirty() && bcb.isValid()) {
			bcb.dirty = true;
			dirtyBuffersCount++;
		}
		bcb.fixcount--;
		bucket.unlock();

		bab.unlatch();

		checkStatus();

	}

	/**
	 * Retrieve the list of dirty pages. This method is called whenever the
	 * transaction manager decides to create a checkpoint. 
	 */
	public DirtyPageInfo[] getDirtyPages() {
		ArrayList<DirtyPageInfo> dplist = new ArrayList<DirtyPageInfo>();
		lruLatch.readLock().lock();
		try {
			for (BufferControlBlock bcb : lru) {
				if (bcb.isDirty() && bcb.isValid()) {
					DirtyPageInfo dp = new DirtyPageInfo(bcb.pageId, bcb.recoveryLsn, bcb.recoveryLsn);
					/*
					 * Since we have not latched the BCB, it is possible that within the small
					 * gap between the check for bcb.dirty and the copying of dirty page info,
					 * the page has been written out to disk, and therefore no longer dirty.
					 * Since a dirty page has recoveryLsn set to a non-null value, we can use this
					 * fact to double-check.
					 * The alternative to this approach would be to latch hash bucket/bcb everytime we check a BCB. 
					 */
					if (!dp.getRecoveryLsn().isNull()) {
						dplist.add(dp);
					}
				}
			}
			return dplist.toArray(new DirtyPageInfo[0]);
		} finally {
			lruLatch.readLock().unlock();
		}
	}

	/**
	 * Allows the update of recoveryLsn after transaction manager has performed
	 * recovery.
	 * 
	 * @param dirty_pages
	 */
	public void updateRecoveryLsns(DirtyPageInfo[] dirty_pages) {
		for (BufferControlBlock bcb : lru) {
			if (bcb.isDirty() && bcb.isValid()) {
				for (DirtyPageInfo dp : dirty_pages) {
					if (dp.getPageId().equals(bcb.pageId)) {
						bcb.recoveryLsn = dp.getRealRecoveryLsn();
						break;
					}
				}
			}
		}
	}

    /**
     * @see org.simpledbm.rss.api.bm.BufferManager#invalidateContainer(int)
     */
    public void invalidateContainer(int containerId) {

        int writeWaits = 0;
        
        lruLatch.readLock().lock();
        try {
            for (BufferControlBlock bcb : lru) {
            	
        		int h = bcb.pageId.hashCode() % bufferHash.length;
        		BufferHashBucket bucket = bufferHash[h];
        		bucket.lock();
				try {
					if (bcb.isValid() && bcb.pageId.getContainerId() == containerId) {
						bcb.invalid = true;
						if (log.isTraceEnabled()) {
							log.trace(LOG_CLASS_NAME, "invalidateContainer",
									"INVALIDATING Page " + bcb.pageId);
						}
						if (bcb.isBeingWritten()) {
							writeWaits++;
						}
					}
				} finally {
					bucket.unlock();
				}
            }
        } finally {
            lruLatch.readLock().unlock();
        }

        while (writeWaits > 0) {
            synchronized (waitingForBuffers) {
                if (log.isTraceEnabled()) {
                    log.trace(LOG_CLASS_NAME, "invalidateContainer", "Writes were in progress during invalidations, hence must wait");
                }
                try {
                    waitingForBuffers.wait(bufferWriterMaxWait);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            writeWaits = 0;
            lruLatch.readLock().lock();
            try {
                for (BufferControlBlock bcb : lru) {
					int h = bcb.pageId.hashCode() % bufferHash.length;
					BufferHashBucket bucket = bufferHash[h];
					bucket.lock();
					try {
						if (bcb.isBeingWritten()
								&& bcb.pageId.getContainerId() == containerId) {
							assert bcb.isInvalid();
							writeWaits++;
						}
					} finally {
						bucket.unlock();
					}
				}
            } finally {
                lruLatch.readLock().unlock();
            }
        }
    }
    
	/**
	 * Writes dirty pages to disk. After each page is written, clients waiting
	 * for free frames are informed so that they can try again.
	 * 
	 * @throws LogException
	 * @throws StorageException
	 */
	void writeBuffers() {

		/*
		 * First make a list of all the dirty pages. By making a copy we avoid
		 * having to lock the LRU list for long.
		 */
		ArrayList<BufferControlBlock> dplist = new ArrayList<BufferControlBlock>();
		lruLatch.readLock().lock();
		try {
			/*
			 * We scan the LRU list and make a note of the dirty pages.
			 */
			for (BufferControlBlock bcb : lru) {
				if (bcb.isDirty()) {
					dplist.add(bcb);
				}
			}
		} finally {
			lruLatch.readLock().unlock();
		}

		/*
		 * Write out the dirty pages.
		 */
		for (BufferControlBlock bcb : dplist) {
			/*
			 * If we cannot obtain a conditional latch on the page, then skip
			 * it.
			 */
    		int h = bcb.pageId.hashCode() % bufferHash.length;
    		BufferHashBucket bucket = bufferHash[h];

    		if (!bucket.tryLock()) {
				continue;
			}

			try {
				/*
				 * frameIndex is equal to -1 for a page being faulted in.
				 */
				if (bcb.isValid() && !bcb.isBeingRead() && bcb.isDirty() && !bcb.isInUse()) {
					/*
					 * Set a flag to indicate that the page is being written
					 * out. Pages can be accessed for reading (shared mode) when this is true,
					 * but not for writing (exclusive mode).
					 */
					bcb.writeInProgress = true;
                    if (log.isTraceEnabled()) {
                        log.trace(LOG_CLASS_NAME, "writeBuffers", "WRITING Page " + bcb.pageId);
                    }
                    bucket.unlock();

					try {
						
						Page page = bufferpool[bcb.frameIndex];
						assert bcb.pageId.equals(page.getPageId());
						Lsn lsn = page.getPageLsn();
						if (logMgr != null && !lsn.isNull()) {
							/*
							 * The Write Ahead log protocol requires that the log be
							 * flushed prior to writing the page.
							 */
							logMgr.flush(lsn);
						}
						pageFactory.store(page);
					} finally {
						bucket.lock();
					}

                    if (log.isTraceEnabled()) {
                        log.trace(LOG_CLASS_NAME, "writeBuffers", "COMPLETED WRITING Page " + bcb.pageId);
                    }
					bcb.recoveryLsn = new Lsn();
					bcb.dirty = false;
					bcb.writeInProgress = false;
					dirtyBuffersCount--;

					/*
					 * Inform any waiting clients that a page has been 
					 * written.
					 */
					synchronized (waitingForBuffers) {
						waitingForBuffers.notifyAll();
					}
				}
			} finally {
				bucket.unlock();
			}
		}
	}

    public final StorageManager getStorageManager() {
        return storageManager;
    }

    public final void setStorageManager(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    
    
	/**
	 * Maintains administrative information about pages buffered in memory.
	 * Information includes the location of the page (frame) within the buffer pool, the
	 * number of times the page has been fixed by clients, the oldest LSN that
	 * may have made a change to the page, whether the poge is dirty, and
	 * whether the page is being read/written from/to disk.
	 * <p>Access Rules:
	 * <ol>
	 * <li>A page may not be accessed at all if the frameIndex is -1.</li>
	 * <li>A page may be accessed for reading (non-exclusive access) even if
	 * writeInProgress is true. However, exclusive access to such a page is not
	 * permitted.</li>
	 * <li>The Buffer Writer may steal a page and write to disk if the
	 * page has a fixcount of 0 and is dirty, subject to rule 1.</li>
	 * <li>The page cannot be marked as dirty if writeInProgress is true.
	 * Due to the order of events, the page write will make the dirty 
	 * flag redundant, ie, the page will not be dirty anymore.</li>
	 * <li>recoveryLsn must be set to the current Log LSN when the 
	 * page is accessed exclusively first time. recoveryLsn must be 
	 * set to Null LSN when the page is written out.
	 * </li>
	 * <li>Once a BCB has been inserted into the hash chain, any changes
	 * to its attributes needs to be protected via the latch.</li>
	 * </ol>
	 * @author Dibyendu Majumdar
	 * @since 20-Aug-2005
	 */
	static final class BufferControlBlock extends Linkable {

		/**
		 * Id of the page that this BCB holds information about.
		 */
		volatile PageId pageId = new PageId();

		/**
		 * Location of the page within the buffer pool array. When this is -1,
		 * the system assumes that the page has yet not been read from disk.
		 */
		volatile int frameIndex = -1;

		/**
		 * Indicates that the page is dirty and needs to be written out. Can
		 * only be set when frameIndex != -1 and writeInProgress is not true.
		 * This is because when a page has been read, it cannot be dirty, and
		 * there is no point of setting this if a page is in the process of
		 * being written out.
		 */
		volatile boolean dirty = false;

        /**
         * This flag is set when a container is deleted and its buffers
         * are invalidated.
         */
        volatile boolean invalid = false;
        
		/**
		 * Maintains count of the number of fixes. A fixed page cannot be be the
		 * victim of the LRU replacement algorithm. A fixed page also cannot be
		 * written out by the Buffer Writer, however, a page may be fixed in
		 * shared mode after Buffer Writer has marked the page for writing.
		 */
		volatile int fixcount = 0;

		/**
		 * LSN of the oldest log record that may have made changes to the
		 * contents of the page. Set when the page is latched exclusively for
		 * the first time. This is reset when the page has been written out.
		 */
		volatile Lsn recoveryLsn = new Lsn();

		/**
		 * Indicates that a write is in progress. This prevents any exclusive
		 * access to the page while it is being written out. Note that once this
		 * is set, the page may be fixed for read access.
		 */
		volatile boolean writeInProgress = false;

		@Override
		public final String toString() {
			return "BCB(pageid=" + pageId + ", frame=" + frameIndex + ", isDirty=" + dirty + ", fixcount=" + fixcount + ", isBeingWritten=" + writeInProgress + ")";
		}
		
		final boolean isInUse() {
			return fixcount > 0;
		}
		
		final boolean isValid() {
			return !invalid;
		}
		
		final boolean isInvalid() {
			return invalid;
		}
		
		final boolean isBeingRead() {
			return frameIndex == -1;
		}
		
		final boolean isBeingWritten() {
			return writeInProgress;
		}
		
		final boolean isDirty() {
			return dirty;
		}
	}

	/**
	 * Maintains the hash table chain, and protects with a latch.
	 * 
	 * @author Dibyendu Majumdar
	 * @since 20-Aug-2005
	 */
	static final class BufferHashBucket {
		/**
		 * Protects access to the hash chain.
		 */
		final private ReentrantLock latch = new ReentrantLock();

		/**
		 * A linked list of hashed BCBs.
		 * <p>
		 * TODO: Inefficient for removal, need a custom implementation.
		 */
		final LinkedList<BufferControlBlock> chain = new LinkedList<BufferControlBlock>();
		
		final void lock() {
			latch.lock();
		}
		
		final void unlock() {
			latch.unlock();
		}
		
		final boolean tryLock() {
			return latch.tryLock();
		}
	}

	/**
	 * Default implementation of BufferAcessBlock. Keeps track of the latch
	 * state so that the correct unlatch action can be taken. 
	 * 
	 * @author Dibyendu Majumdar
	 * @since 21-Aug-2005
	 */
	static final class BufferAccessBlockImpl implements BufferAccessBlock {

		final BufferControlBlock bcb;

		final Page page;

		final BufferManagerImpl bufMgr;

		boolean dirty = false;
		
		int latchMode = 0;
		
		BufferAccessBlockImpl(BufferManagerImpl bufMgr, BufferControlBlock bcb, Page page) {
			this.bufMgr = bufMgr;
			this.bcb = bcb;
			this.page = page;
		}

		void latchExclusively() {
			page.latchExclusive();
			latchMode = BufferManagerImpl.LATCH_EXCLUSIVE;
		}

		void latchForUpdate() {
			page.latchUpdate();
			latchMode = BufferManagerImpl.LATCH_UPDATE;
		}
		
		void latchShared() {
			page.latchShared();
			latchMode = BufferManagerImpl.LATCH_SHARED;
		}

		void unlatch() {
			if (latchMode == BufferManagerImpl.LATCH_EXCLUSIVE) {
				page.unlatchExclusive();
			} else if (latchMode == BufferManagerImpl.LATCH_UPDATE) {
				page.unlatchUpdate();
			} else if (latchMode == BufferManagerImpl.LATCH_SHARED) {
				page.unlatchShared();
			} else {
				throw new IllegalStateException();
			}
			latchMode = 0;
		}

		public boolean isLatchedExclusively() {
			return latchMode == BufferManagerImpl.LATCH_EXCLUSIVE;
		}
		
		public boolean isLatchedForUpdate() {
			return latchMode == BufferManagerImpl.LATCH_UPDATE;
		}
		
		public boolean isLatchedShared() {
			return latchMode == BufferManagerImpl.LATCH_SHARED;
		}
		
		/* (non-Javadoc)
		 * @see org.simpledbm.bm.BufferAccessBlock#setDirty(org.simpledbm.log.Lsn)
		 */
		public void setDirty(Lsn lsn) {
			if (latchMode == BufferManagerImpl.LATCH_EXCLUSIVE) {
				dirty = true;
				page.setPageLsn(lsn);
			}
			else {
				throw new IllegalStateException("Page can be marked as dirty only if it has been latched exclusively");
			}
		}

		/* (non-Javadoc)
		 * @see org.simpledbm.bm.BufferAccessBlock#getPage()
		 */
		public Page getPage() {
			return page;
		}

		/* (non-Javadoc)
		 * @see org.simpledbm.bm.BufferAccessBlock#unfix()
		 */
		public void unfix() {
			bufMgr.unfix(this);
		}

		/* (non-Javadoc)
		 * @see org.simpledbm.bm.BufferAccessBlock#upgradeUpdateLatch()
		 */
		public void upgradeUpdateLatch() {
			if (latchMode != BufferManagerImpl.LATCH_UPDATE) {
				throw new IllegalStateException();
			}
			page.upgradeUpdate();
			latchMode = BufferManagerImpl.LATCH_EXCLUSIVE;
		}

		/* (non-Javadoc)
		 * @see org.simpledbm.bm.BufferAccessBlock#downgradeExclusiveLatch()
		 */
		public void downgradeExclusiveLatch() {
			if (latchMode != BufferManagerImpl.LATCH_EXCLUSIVE) {
				throw new IllegalStateException();
			}
			page.downgradeExclusive();
			latchMode = BufferManagerImpl.LATCH_UPDATE;
		}

	}

	/**
	 * Background Buffer Writer thread. This thread wakes up periodically or
	 * upon request, and writes out as many buffers as it can. It does not hold any
	 * latches while writing out pages, hence clients are not blocked while
	 * this thread is running.
	 *  
	 * @author Dibyendu Majumdar
	 * @since 21-Aug-2005
	 */
	static final class BufferWriter implements Runnable {
		
		static final String LOG_CLASS_NAME = BufferWriter.class.getName();
		
		final BufferManagerImpl bufmgr;

		BufferWriter(BufferManagerImpl bufmgr) {
			this.bufmgr = bufmgr;
		}

		public final void run() {
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "run", "Buffer writer STARTED");
			}
			for (;;) {
//				synchronized (bufmgr.bufferWriterLatch) {
//					try {
//						bufmgr.bufferWriterLatch.wait(bufmgr.bufferWriterSleepInterval);
//					} catch (InterruptedException e) {
//						// ignore
//					}
//				}
				LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(bufmgr.bufferWriterSleepInterval, TimeUnit.MILLISECONDS));
				try {
					if (log.isTraceEnabled()) {
						log.trace(LOG_CLASS_NAME, "run", "BEFORE Writing Buffers: Dirty Buffers Count = " + bufmgr.dirtyBuffersCount);
					}
					long start = System.currentTimeMillis();
					// System.err.println("Writing buffers");
					bufmgr.writeBuffers();
					long end = System.currentTimeMillis();
					if (log.isTraceEnabled()) {
						log.trace(LOG_CLASS_NAME, "run", "AFTER Writing Buffers: Dirty Buffers Count = " + bufmgr.dirtyBuffersCount);
						log.trace(LOG_CLASS_NAME, "run","BUFFER WRITER took " + (end - start) + " millisecs"); 
					}
					synchronized (bufmgr.waitingForBuffers) {
						bufmgr.waitingForBuffers.notifyAll();
					}
				} catch (Exception e) {
					log.error(LOG_CLASS_NAME, "run", "Error occurred while writing buffer pages", e);
					e.printStackTrace();
					bufmgr.stop = true;
				}
				if (bufmgr.stop) {
					break;
				}
			}
			if (log.isDebugEnabled()) {
				log.debug(LOG_CLASS_NAME, "run", "Buffer writer STOPPED");
			}
		}
	}

	public void setBufferWriterSleepInterval(int bufferWriterSleepInterval) {
		this.bufferWriterSleepInterval = bufferWriterSleepInterval;
	}

}
