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
package org.simpledbm.rss.impl.bm;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.simpledbm.rss.api.bm.BufferAccessBlock;
import org.simpledbm.rss.api.bm.BufferManager;
import org.simpledbm.rss.api.bm.BufferManagerException;
import org.simpledbm.rss.api.bm.DirtyPageInfo;
import org.simpledbm.rss.api.exception.ExceptionHandler;
import org.simpledbm.rss.api.platform.Platform;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.api.wal.LogManager;
import org.simpledbm.rss.api.wal.Lsn;
import org.simpledbm.rss.util.Dumpable;
import org.simpledbm.rss.util.Linkable;
import org.simpledbm.rss.util.SimpleLinkedList;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;

/**
 * Implements an LRU Buffer Manager. This implementation uses a global
 * LRU chain. 
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
    private static final String BUFFER_WRITER_WAIT = "bufferpool.writerSleepInterval";

    final Logger log;

    final ExceptionHandler exceptionHandler;
    
    final MessageCatalog mcat;

    private static final int LATCH_EXCLUSIVE = 2;

    private static final int LATCH_UPDATE = 3;

    private static final int LATCH_SHARED = 1;

    static BufferManagerStatistics statistics = new BufferManagerStatistics();
    
    final Platform platform;
    
    /**
     * Buffer Manager does not directly depend upon the Storage Manager, but
     * if it has access to the StorageManager then it can validate page fix requests.
     * This is useful when running tests.
     */
    private StorageManager storageManager;

    /**
     * The page factory instance that will manage IO of pages.
     * The page size is determined by the page factory.
     */
    private PageManager pageFactory;

    /**
     * Write Ahead Log Manager instance, may be set to null for testing
     * purposes.
     */
    private LogManager logMgr;

    /**
     * The BufferPool is an array of slots where pages are held. Each slot is
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
     * BCBs in the cache are also put into the LRU list. The head of the list
     * is the LRU end, whereas the tail is the MRU.
     */
    private final SimpleLinkedList<BufferControlBlock> lru = new SimpleLinkedList<BufferControlBlock>();

    /*
     * Notes on latching:
     * In this implementation, the latching order is always: 
     * LRU chain -> Hash bucket -> BCB
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
     * Flag that triggers Buffer Manager to shutdown. Should be set
     * if an unrecoverable error is detected.
     */
    volatile boolean stop = false;

    /**
     * Points to the top of the {@link #freeFrames} stack.
     * Access to this is protected by synchronizing the freeFrames
     * array. 
     */
    private int nextAvailableFrame = -1;

    /**
     * A stack of available free slots in the buffer pool.
     * {@link #nextAvailableFrame} points to the top of the stack.
     * Access must be synchronized for thread safety.
     */
    private int[] freeFrames;

    /**
     * The interval in milliseconds for which the Buffer Writer
     * thread sleeps between each write.
     */
    private int bufferWriterSleepInterval = 5000;

    /**
     * A count of number of pages estimated to be
     * dirty.
     */
    private AtomicInteger dirtyBuffersCount = new AtomicInteger(0);

    static final int hashPrimes[] = { 53, 97, 193, 389, 769, 1543, 3079, 6151,
            12289, 24593, 49157, 98317, 196613, 393241, 786433 };

    private int getNumericProperty(Properties props, String name,
            int defaultValue) {
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
     * The hash table size is automatically determined based upon
     * the bufferpool size.
     */
    private void init(LogManager logMgr, PageManager pageFactory,
            int bufferpoolsize) {
        this.logMgr = logMgr;
        this.pageFactory = pageFactory;
        bufferpool = new Page[bufferpoolsize];
        freeFrames = new int[bufferpoolsize];
        for (int f = 0; f < bufferpoolsize; f++) {
            freeFrames[++nextAvailableFrame] = f;
        }
        int h = 0;
        for (; h < hashPrimes.length; h++) {
            if (hashPrimes[h] > bufferpoolsize) {
                break;
            }
        }
        if (h == hashPrimes.length) {
            h = hashPrimes.length - 1;
        }
        int hashsize = hashPrimes[h];
        bufferHash = new BufferHashBucket[hashsize];
        for (int i = 0; i < hashsize; i++) {
            bufferHash[i] = new BufferHashBucket();
        }
        statistics.bufferpoolsize = bufferpoolsize;
        statistics.hashTableSize = hashsize;
        statistics.writersleepinterval = bufferWriterSleepInterval;
    }

    /**
     * Create a Buffer Manager instance.
     * 
     * @param logMgr Log Manager instance, may be null for testing purposes.
     * @param pageFactory PageFactory instance, required.
     * @param bufferpoolsize The size of the buffer pool.
     * @param unused Not used
     */
    public BufferManagerImpl(Platform platform, LogManager logMgr, PageManager pageFactory,
            int bufferpoolsize, int unused) {
    	this.log = platform.getLogger(BufferManager.LOGGER_NAME);
    	this.mcat = platform.getMessageCatalog();
    	this.exceptionHandler = platform.getExceptionHandler(log);
    	this.platform = platform;
        init(logMgr, pageFactory, bufferpoolsize);
    }

    /**
     * Create a Buffer Manager instance.
     * 
     * @param logMgr Log Manager instance, may be null for testing purposes.
     * @param pageFactory PageFactory instance, required.
     * @param props Properties that specify how the Buffer Manager should be configured
     */
    public BufferManagerImpl(Platform platform, LogManager logMgr, PageManager pageFactory,
            Properties props) {
    	this.log = platform.getLogger(BufferManager.LOGGER_NAME);
    	this.mcat = platform.getMessageCatalog();
    	this.exceptionHandler = platform.getExceptionHandler(log);
    	this.platform = platform;
        int bufferpoolsize = getNumericProperty(
            props,
            BUFFERPOOL_NUMBUFFERS,
            500);
        bufferWriterSleepInterval = getNumericProperty(
            props,
            BUFFER_WRITER_WAIT,
            5000);
        init(logMgr, pageFactory, bufferpoolsize);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.bm.BufMgr#start()
     */
    public void start() {
        /* 
         * Setup background thread for buffer writer.
         */
        bufferWriter = new Thread(new BufferWriter(this), "BufferWriter");
        bufferWriter.start();
    }

    /**
     * Wakes up the Buffer Writer thread.
     */
    void signalBufferWriter() {
        if (log.isTraceEnabled()) {
            log.trace(
                this.getClass().getName(),
                "signalBufferWriter",
                "SIMPLEDBM-DEBUG: SIGNALLING Buffer Writer");
        }
        if (bufferWriter != null && bufferWriter.isAlive()) {
        	LockSupport.unpark(bufferWriter);
        }
    }

    /**
     * Instructs the Buffer Manager to shutdown.
     */
    private void setStop() {
        stop = true;
    }

    /**
     * Shuts down the Buffer Manager instance.
     */
    public void shutdown() {
        setStop();
        signalBufferWriter();
        try {
        	if (bufferWriter != null && bufferWriter.isAlive()) {
        		bufferWriter.join();
        	}
        } catch (InterruptedException e) {
            log.error(this.getClass().getName(), "shutdown", mcat
                .getMessage("EM0001"), e);
        }
        writeBuffers();
        dumpStatistics();
    }

    /**
     * Get an empty frame (slot in bufferpool); if necessary, evict the LRU page from the
     * buffer pool to make space.
     * <p>
     * Algorithm:
     * <ol>
     * <li> First, check if there is an unused frame that can be used. </li>
     * <li> If not, we need to identify a page that can be replaced. This page
     * should be the least recently used page which is not in use. 
     * Pages that are marked as in use or busy (pending IO) cannot be evicted.
     * Scan the LRU list for a page that qualifies for replacement. </li>
     * <li>If the page identified for eviction is dirty, flush it to disk,
     * following the Write Ahead Log protocol. Remove the page BCB from the 
     * Hash chain, and from the LRU list, and return frame index previously 
     * occupied by the page. </li>
     * </ol>
     * <p>
     * TEST CASE: test the free frames array, a) until it is exhausted, and b)
     * when frames are put back into it.
     * </p>
     * <p>
     * Latching: No latches should be held when this is called.
     * LRU latch and bucket latch obtained while scanning the LRU chain.
     * All latches released when this method returns.
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

		
		for (;;) {
			/*
			 * Find a replacement victim - this should be the Least Recently
			 * Used Buffer that is not in use.
			 */
			BufferControlBlock victim = null;
			boolean doWrite = false;

			/*
			 * Search for a victim starting from the LRU end
			 */
			lruLatch.readLock().lock();
			try {
				for (BufferControlBlock nextBcb : lru) {
					nextBcb.lock();
					try {
						/*
						 * If the buffer is pinned or is waiting for IO, then
						 * skip
						 */
						if (nextBcb.isValid()
								&& (nextBcb.isInUse() || nextBcb.isBeingRead() || nextBcb
										.isBeingWritten())) {
							if (log.isDebugEnabled()) {
								log
										.debug(
												this.getClass().getName(),
												"getFrame",
												"SIMPLEDBM-DEBUG: Skipping bcb "
														+ nextBcb
														+ " because fixCount > 0 or IO in progress");
							}
							continue;
						} else {
							/*
							 * Found a victim. If it is a valid page and is
							 * dirty we will have to flush it to disk.
							 */
							victim = nextBcb;
							if (victim.isValid() && victim.isDirty()) {
								assert !victim.isBeingWritten();
								assert !victim.isInUse();
								assert !victim.isBeingRead();
								/*
								 * Needs to be flushed.
								 */
								doWrite = true;
							}
							/*
							 * Regardless of whether we need to flush this page
							 * or not, we need to tell others that we are about
							 * to use this page. We set a flag to say that IO is
							 * in progress; this stops others from messing
							 * around with this BCB.
							 */
							victim.setBeingWritten(true);
							break;
						}
					} finally {
						nextBcb.unlock();
					}
				}
			} finally {
				lruLatch.readLock().unlock();
			}

			if (victim == null) {
				/*
				 * All pages are in use?
				 */
				return -1;
			}

			/*
			 * Flush the page to disk if necessary, using the write ahead log
			 * protocol. We could use a blocking queue here to pass on the write
			 * request to the BufferWriter, but for now, we will do the write
			 * ourselves.
			 */
			if (doWrite) {
				flushUsingWriteAheadLogProtocol(victim);
			}

			/*
			 * Now lets remove the victim from the LRU chain and the Hash Table.
			 * As we have set the IO flag on the BCB, it will be unmolested even
			 * though we haven't got a lock on it.
			 */
			lruLatch.writeLock().lock();
			try {
				final PageId pageid = victim.getPageId();
				final int h = (pageid.hashCode() & 0x7FFFFFFF)
						% bufferHash.length;
				final BufferHashBucket bucket = bufferHash[h];

				/*
				 * The LRU latch is always obtained before the bucket latch.
				 * Hence it is safe to wait unconditionally for the bucket k.
				 */
				bucket.lockExclusive();
				try {
					/*
					 * Reset the BCB because it may have also been picked up by
					 * the BufferWriter thread as a dirty buffer
					 */
					victim.lock();
					try {
						/*
						 * In the interest of increasing concurrency we allow
						 * readers to access pages while they are being written out.
						 * So we need to check the fixcount here in case a reader
						 * got the page while we were writing it out.
						 */
						if (victim.isInUse()) {
							/*
							 * Okay, someone else has booked this page.
							 * We need to reset the flag we set and look for another page.
							 */
							victim.setBeingWritten(false);
							victim.signalIOCompleted();
							continue;
						}
						
						/* Remove BCB from LRU Chain */
						lru.remove(victim);
						/* Remove BCB from Hash Chain */
						bucket.chain.remove(victim);
						victim.setRecoveryLsn(new Lsn());
						victim.setDirty(false);
						victim.setBeingWritten(false);
						victim.signalIOCompleted();
						victim.setInvalid(true);
						bufferpool[victim.getFrameIndex()] = null;
						/*
						 * The frame previously occupied by the victim is now
						 * free for use.
						 */
						return victim.getFrameIndex();
					} finally {
						victim.unlock();
					}
				} finally {
					bucket.unlockExclusive();
				}
			} finally {
				lruLatch.writeLock().unlock();
			}
		}
	}
    
    /**
     * Wait for pending IO to be completed
     * @param bcb The BufferControlBlock that is blocked due to IO
     */
    private void waitIfBusy(BufferControlBlock bcb) {
    	bcb.lock();
    	try {
    		if (bcb.isValid() && (bcb.isBeingRead() || bcb.isBeingWritten())) {
    			bcb.awaitIOCompletion();
    		}
    	}
    	finally {
    		bcb.unlock();
    	}
    }

    /**
     * Read a page into the Buffer Cache. Algorithm: 
     * <ol>
     * <li>First check that the page in question hasn't been read in the gap
     * before the call to this method.</li>
     * <li>Allocate a new BufferControlBlock.</li>
     * <li>Initialize it and attach to the appropriate Hash chain. The BCB must be marked inUse
     * to avoid getting swapped out.</li>
     * <li>Obtain a frame (slot) for the page by calling {@link #getFrame()}.</li>
     * <li>If the page is new, instantiate a new page, else, read it from the disk.</li>
     * <li>Return the page wrapped in a new <{@link BufferAccessBlock}</li>.
     * </ol>
     * <p>
     * Latching: No latches should be held when this method is called.
     * The Bucket latch is obtained when adding the new BCB to the hash chain.
     * No latches held during IO.
     * No latches held when this method returns.
     *
     * @return The newly allocated BufferAccessBlock
     */
    private BufferAccessBlockImpl locatePage(PageId pageId, int hashCode,
            boolean isNew, int pagetype, int latchMode) {

        BufferControlBlock nextBcb = new BufferControlBlock(pageId);
        BufferHashBucket bucket = bufferHash[hashCode];
        
        boolean busy = false;
        do {
        	BufferControlBlock toWaitFor = null;
        	busy = false;
        	
			bucket.lockExclusive();
			try {

				/*
				 * 16-feb-03: As we do not hold any latches when this method is
				 * called, it is possible that some other thread has read
				 * the page we are trying to read. We therefore need to check whether the
				 * page is now available. 
				 * Dec-2006: We need to additionally check whether the page is currently
				 * being read or written, in which case, we need to wait for IO to be
				 * completed.
				 */

				for (BufferControlBlock bcb : bucket.chain) {
					bcb.lock();
					try {
						if (bcb.isValid() && bcb.getPageId().equals(pageId)) {
							/* 
							 * if the page is being read
							 * we need to wait for the read to be over.
							 */
							if (bcb.isBeingRead() || (bcb.isBeingWritten() && latchMode != LATCH_SHARED)) {
								if (log.isDebugEnabled()) {
									log.debug(this.getClass().getName(),
											"locatePage",
											"SIMPLEDBM-DEBUG: Another thread is attempting to read/write page "
													+ pageId
													+ ", therefore retrying");
								}
								toWaitFor = bcb;
								busy = true;
							} else {
								if (log.isDebugEnabled()) {
									log.debug(
										this.getClass().getName(),
										"locatePage",
										"SIMPLEDBM-DEBUG: Page "
										+ pageId
										+ " has been read in the meantime");
								}
								/*
								 * Okay, we have the page we want, so we are done.
								 */
								statistics.cachehits++;
								return useBCB(pageId, latchMode, bcb);
							}
						}
					} finally {
						bcb.unlock();
					}
				}

				if (!busy) {
					/* This means we haven't found the page in the hash table.
					 * frameIndex set to -1 indicates that the page is being
					 * read in.
					 */
					nextBcb.setFrameIndex(-1);

					bucket.chain.addFirst(nextBcb);
					/*
					 * At this point, the new BCB is in the hash table but not
					 * in the LRU list. The BCB should have frameIndex set to -1
					 * and fixcount = 1. This will prevent the page from being
					 * evicted.
					 */
					assert nextBcb.isBeingRead();
					assert nextBcb.isInUse();
					assert nextBcb.isValid();
					assert nextBcb.isNewBuffer();
				}
			} finally {
				bucket.unlockExclusive();
			}
			
			/*
			 * If the page we are looking for is being read/written, then
			 * we need to wait for the IO to be completed. We have to do this
			 * here because we must not hold any latches during the wait.
			 * We will retry once the IO wait is over.
			 */
			if (toWaitFor != null && busy) {
				waitIfBusy(toWaitFor);
			}
			
		} while (busy);
        

        /*
         * If any other thread attempts to read the same page, they will find
         * the BCB we have just inserted. However, they will notice
         * that frameIndex == -1 and wait for us to finish IO.
         */

        /* Get an empty buffer pool slot */
        int frameNo = getFrame();

        if (frameNo == -1) {
            /*
             * Failed to obtain a frame, a fatal error.
             * This can happen if there aren't enough pages in the bufferpool
             * to satisfy all concurrent requests or if the system is leaking
             * pages (i.e., failing to unfix() pages are use).
             */
            setStop();
            exceptionHandler.errorThrow(this.getClass().getName(), "locatePage", 
            		new BufferManagerException(mcat.getMessage("EM0004", pageId)));
        }

        assert bufferpool[frameNo] == null;

        if (isNew) {
            /*
             *  If it is a new page, we do not need to read the page.
             */
            bufferpool[frameNo] = pageFactory.getInstance(pagetype, pageId);
        } else {
            boolean readOk = false;
            try {
                /*
                 * Note that while reading the page, we do not hold any
                 * latches/locks.
                 */
                bufferpool[frameNo] = pageFactory.retrieve(pageId);
                readOk = true;
            } finally {
                if (!readOk) {
                    /*
                     * This is not neat, but we want to leave things in a tidy
                     * state. We want to remove the invalid BCB from the hash chain,
                     * and return the buffer pool frame to the freelist. 
                     */
                    log.error(this.getClass().getName(), "locatePage", mcat
                        .getMessage("EM0002", pageId));
                    bucket.lockExclusive();
                    try {
                        bucket.chain.remove(nextBcb);
                        synchronized (freeFrames) {
                            freeFrames[++nextAvailableFrame] = frameNo;
                        }
                    } finally {
                        bucket.unlockExclusive();
                    }
                }
            }
        }

        assert nextBcb.getPageId().equals(bufferpool[frameNo].getPageId());
        /*
         * Read completed, at this point, we set the frameIndex,
         * which indicates to other threads that the page is now ready.
         */
        nextBcb.lock();
        try {
            nextBcb.setFrameIndex(frameNo);
            nextBcb.signalIOCompleted();
            /*
             * We are done, so return a BufferAccessControl wrapper.
             */
            return useBCB(pageId, latchMode, nextBcb);
        } finally {
            nextBcb.unlock();
        }
    }

    /**
     * Check that the Buffer Manager is still valid.
     */
    private void checkStatus() {
        if (stop) {
            exceptionHandler.errorThrow(this.getClass().getName(), "checkStatus", 
            		new BufferManagerException(mcat.getMessage("EM0005")));
        }
    }

    /**
     * Pause for a short while.
     */
    private void pause() {
//      Thread.yield();
        try { Thread.sleep(1, 0);
        } catch (InterruptedException e) {}
    }
    
    /**
     * Search for a page in the buffer cache. If not found,
     * read it from disk by calling {@link #locatePage(PageId, int, boolean, int, int) locatePage()}.
     * 
     * @param pageid ID of the page being searched
     * @param isNew A flag to indicate that the page should not be read from disk
     * @param pagetype The typecode for the page, so that the correct page can be instantiated
     * @param latchMode The desired latch mode
     * @return
     */
    private BufferAccessBlockImpl getBCB(PageId pageid, boolean isNew,
			int pagetype, int latchMode) {

		int h = (pageid.hashCode() & 0x7FFFFFFF) % bufferHash.length;
		BufferHashBucket bucket = bufferHash[h];

		/*
		 * During the search we may find that the page we are looking for is in the
		 * process of being read from disk or written to disk. When this
		 * happens, we have to wait for the pending IO to be completed
		 * before resuming the search. 
		 */
		boolean pendingIO = false;
		do {
			pendingIO = false;
			BufferControlBlock toWaitFor = null;
			bucket.lockShared();
			try {
				for (BufferControlBlock bcb : bucket.chain) {
					bcb.lock();
					try {
						/*
						 * Ignore invalid pages.
						 */
						if (bcb.isValid() && bcb.getPageId().equals(pageid)) {
							/*
							 * If the page is being read or written we must wait for
							 * the IO to be completed.
							 */
//							if (!bcb.isBeingRead() && !bcb.isBeingWritten()) {
//								statistics.cachehits++;
//								return useBCB(pageid, latchMode, bcb);
//							} else {
//								toWaitFor = bcb;
//								pendingIO = true;
//							}
							if (bcb.isBeingRead() || (bcb.isBeingWritten() && latchMode != LATCH_SHARED)) {
								toWaitFor = bcb;
								pendingIO = true;
							}
							else {
								statistics.cachehits++;
								return useBCB(pageid, latchMode, bcb);
							}
							break;
						}
					} finally {
						bcb.unlock();
					}
				}
			} finally {
				bucket.unlockShared();
			}
			if (pendingIO) {
				/*
				 * Wait for the pending IO to complete
				 */
				waitIfBusy(toWaitFor);
			}
		} while (pendingIO);

		/*
		 * Page not found in the memory cache, therefore must be
		 * read in.
		 */
		return locatePage(pageid, h, isNew, pagetype, latchMode);
	}

    /**
     * Prepare this BCB for use - and wrap it in a BufferAccessBlock.
     * <p>Latching: The BCB must be locked prior to this call.
     * 
     * @param pageid ID of the page we are returning
     * @param latchMode The latch mode
     * @param nextBcb The BCB that is being returned
     */
	private BufferAccessBlockImpl useBCB(PageId pageid, int latchMode,
			BufferControlBlock nextBcb) {
		BufferAccessBlockImpl bab;

		if (nextBcb.isNewBuffer()) {
			/*
			 * Page has just been read, so we do not
			 * need to increment fix count. Reset
			 * this flag for next time.
			 */
			nextBcb.setNewBuffer(false);
		} else {
			/*
			 * Increment fix count while holding the
			 * bucket lock.
			 */
			nextBcb.incrementFixCount();
		}

		assert nextBcb.getFrameIndex() != -1;
		assert nextBcb.getPageId().equals(pageid);
		assert nextBcb.isInUse();

		/*
		 * ISSUE: 17-Sep-05 We set the recoveryLsn
		 * if an update mode access is requested
		 * because we do not know whether the page
		 * will subsequently be latched exclusively
		 * or not.
		 */
		if (latchMode == LATCH_EXCLUSIVE
				|| latchMode == LATCH_UPDATE) {
			if (nextBcb.getRecoveryLsn().isNull()
					&& logMgr != null) {
				nextBcb.setRecoveryLsn(logMgr
						.getMaxLsn());
			}
		}
		/*
		 * Allocate a Buffer Access Block and
		 * initialize it
		 */
		bab = new BufferAccessBlockImpl(this,
				nextBcb, bufferpool[nextBcb
						.getFrameIndex()]);
		return bab;
	}
    
    /**
     * Fix and lock a page in buffer pool. 
     * <p>Algorithm: 
     * <ol>
     * <li>Call {@link getBCB} to search for the page in the buffer pool and
     * read it in if necessary.</li>
     * <li>Make the BCB the MRU page in the LRU chain</li>
     * <li>Acquire page latch as requested.</li>
     * </ol>
     * {@inheritDoc}
     */
    private BufferAccessBlock fix(PageId pageid, boolean isNew, int pagetype,
            int latchMode, int hint) {

    	statistics.fixcount++;
    	
        checkStatus();

        if (storageManager != null) {
            /*
             * Validate that we are fixing page for a container that exists.
             */
            StorageContainer sc = storageManager.getInstance(pageid
                .getContainerId());
            assert sc != null;
        }

        BufferAccessBlockImpl bab = getBCB(pageid, isNew, pagetype, latchMode);
        BufferControlBlock nextBcb = bab.bcb;
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
			nextBcb.lock();
			try {
				if (hint == 0) {
					if (lru.getLast() != nextBcb) {
						if (nextBcb.isMemberOf(lru)) {
							lru.remove(nextBcb);
						}
						lru.addLast(nextBcb);
					}
				} else {
					if (lru.getFirst() != nextBcb) {
						if (nextBcb.isMemberOf(lru)) {
							lru.remove(nextBcb);
						}
						lru.addFirst(nextBcb);
					}
				}
			} finally {
				nextBcb.unlock();
			}
		} finally {
			lruLatch.writeLock().unlock();
		}

        /*
         * All latches must be released before we acquire the user requested
         * page latch.
         * Latch the page as requested.
         */
        if (latchMode == LATCH_EXCLUSIVE) {
            bab.latchExclusively();
        } else if (latchMode == LATCH_UPDATE) {
            bab.latchForUpdate();
        } else {
            bab.latchShared();
        }

        assert bab.bcb.getPageId().equals(bab.page.getPageId());

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

    private void incrementDirtyBuffersCount() {
        dirtyBuffersCount.incrementAndGet();
        statistics.dirtybuffers++;
    }

    private void decrementDirtyBuffersCount() {
        dirtyBuffersCount.decrementAndGet();
        statistics.dirtybuffers--;
    }

    /**
     * Unfix a page. 
     * <p>Algorithm:
     * <ol>
     * <li>Release page latch.</li>
     * <li>Set dirty flag.</li>
     * <li>Decrement fix count.</li>
     * </ol>
     */
    void unfix(BufferAccessBlockImpl bab) {

        /*
         * Release the page latch as early as possible.
         */
        bab.unlatch();

        BufferControlBlock bcb = bab.bcb;
        bcb.lock();
        try {
            /*
             * Note that if the page is being written out, we must not set the dirty
             * flag. 
             * TEST CASE: Test the situation where a page is unfixed while it
             * is being written out.
             * FIXME: The test for isBeingWritten() is probably redundant as the
             * Buffer Writer will only steal pages with fixcount == 0.
             */
            if (bab.dirty && !bcb.isBeingWritten() && !bcb.isDirty()
                && bcb.isValid()) {
                bcb.setDirty(true);
                incrementDirtyBuffersCount();
            }
            bcb.decrementFixCount();
        } finally {
            bcb.unlock();
        }

        // checkStatus();
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
				bcb.lock();
				try {
					DirtyPageInfo dp = bcb.getDirtyPageInfo();
					if (dp != null) {
						dplist.add(dp);
					}
				} finally {
					bcb.unlock();
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
                    if (dp.getPageId().equals(bcb.getPageId())) {
                        bcb.setRecoveryLsn(dp.getRealRecoveryLsn());
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

        while (true) {
            int writeWaits = 0;
			lruLatch.readLock().lock();
			try {
				for (BufferControlBlock bcb : lru) {
					bcb.lock();
					try {
						if (bcb.isValid()
								&& bcb.getPageId().getContainerId() == containerId) {
							bcb.setInvalid(true);
							if (log.isDebugEnabled()) {
								log.debug(this.getClass().getName(),
										"invalidateContainer",
										"SIMPLEDBM-DEBUG: Invalidating page "
												+ bcb.getPageId()
												+ " for container "
												+ containerId);
							}
						}
						if (bcb.getPageId().getContainerId() == containerId && bcb.isBeingWritten()) {
							writeWaits++;
						}
					} finally {
						bcb.unlock();
					}
				}
			} finally {
				lruLatch.readLock().unlock();
			}
			if (writeWaits == 0) {
				break;
			}
			pause();
		}
    }

    void dumpBuffers(final PrintStream stream) {
    	/*
    	 * TODO - the format of the output needs to be reviewed
    	 */
		/*
		 * First make a list of all the dirty pages. By making a copy we avoid
		 * having to lock the LRU list for long.
		 */
		lruLatch.readLock().lock();
		try {
			/*
			 * We scan the LRU list and make a note of the dirty pages.
			 */
			for (BufferControlBlock bcb : lru) {
				int h = (bcb.pageId.hashCode() & 0x7FFFFFFF)
						% bufferHash.length;
				stream.println(bcb + ", buffer hash: " + h);
			}
		} finally {
			lruLatch.readLock().unlock();
		}
	}

    /**
	 * Write a buffer page. No locks are held during IO. 
	 * 
	 * @param bcb
	 */
	private void flushUsingWriteAheadLogProtocol(BufferControlBlock bcb) {
		Page page = bufferpool[bcb.getFrameIndex()];
		assert bcb.getPageId().equals(page.getPageId());
		Lsn lsn = page.getPageLsn();
		if (logMgr != null && !lsn.isNull()) {
			/*
			 * The Write Ahead log protocol requires that the log be flushed
			 * prior to writing the page.
			 */
			logMgr.flush(lsn);
		}
		pageFactory.store(page);
		decrementDirtyBuffersCount();
	}
    
    
    /**
	 * Write a buffer page. No locks are held during IO. A flag is set to alert
	 * other processes that the page is being written.
	 * 
	 * @param bcb
	 */
	private void cleanBCB(BufferControlBlock bcb) {

		boolean doWrite = false;
		bcb.lock();
		try {
			/*
			 * Check that all pre-conditions are met.
			 */
			if (bcb.isValid() && !bcb.isBeingRead() && !bcb.isBeingWritten()
					&& bcb.isDirty() && !bcb.isInUse()) {
				/*
				 * Set a flag to indicate that the page is being written out.
				 * Pages can be accessed for reading (shared mode) when this is
				 * true, but not for writing (exclusive mode).
				 */
				bcb.setBeingWritten(true);
				doWrite = true;
				if (log.isTraceEnabled()) {
					log.trace(this.getClass().getName(), "writeBuffers",
							"SIMPLEDBM-DEBUG: WRITING Page " + bcb.getPageId());
				}
			}
		} finally {
			bcb.unlock();
		}

		if (doWrite) {
			flushUsingWriteAheadLogProtocol(bcb);
			bcb.lock();
			try {
				if (log.isTraceEnabled()) {
					log.trace(this.getClass().getName(), "writeBuffers",
							"SIMPLEDBM-DEBUG: COMPLETED WRITING Page "
									+ bcb.getPageId());
				}
				bcb.setRecoveryLsn(new Lsn());
				bcb.setDirty(false);
				bcb.setBeingWritten(false);
				bcb.signalIOCompleted();
			} finally {
				bcb.unlock();
			}		
		}
	}
    
    
    /**
	 * Writes dirty pages to disk. After each page is written, clients waiting
	 * for free frames are informed so that they can try again.
	 */
    public void writeBuffers() {

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
        	cleanBCB(bcb);
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
    static final class BufferControlBlock extends Linkable implements Dumpable {

        /**
         * Id of the page that this BCB holds information about.
         */
        private final PageId pageId;

        /**
         * Location of the page within the buffer pool array. When this is -1,
         * the system assumes that the page has yet not been read from disk.
         */
        private volatile int frameIndex = -1;

        /**
         * Indicates that the page is dirty and needs to be written out. Can
         * only be set when frameIndex != -1 and writeInProgress is not true.
         * This is because when a page has been read, it cannot be dirty, and
         * there is no point of setting this if a page is in the process of
         * being written out.
         */
        private volatile boolean dirty = false;

        /**
         * This flag is set when a container is deleted and its buffers
         * are invalidated.
         */
        private volatile boolean invalid = false;

        /*
         * 1-nov-07
         * Found a race condition when a page is read in. The fixcount 
         * was initialized to 0, which meant that after the page was read in,
         * frameIndex set, and bucket unlocked, there was a period of time
         * during which the page existed with fixcount = 0. This means the
         * BufferWriter could evict the page from right under the feet of
         * the fix() call. So we now initialize fixcount to 1. This means
         * we have to be careful when incrementing fixcount so that we do
         * not do it twice when the page has been just read. For this reason,
         * we use a new flad newBuffer to track the status of the page.
         */
        
        
        /**
         * Maintains count of the number of fixes. A fixed page cannot be be the
         * victim of the LRU replacement algorithm. A fixed page also cannot be
         * written out by the Buffer Writer, however, a page may be fixed in
         * shared mode after Buffer Writer has marked the page for writing.
         */
        private volatile int fixcount = 1;
        

        /**
         * LSN of the oldest log record that may have made changes to the
         * contents of the page. Set when the page is latched exclusively for
         * the first time. This is reset when the page has been written out.
         */
        private volatile Lsn recoveryLsn = new Lsn();

        /**
         * Indicates that a write is in progress. This prevents any exclusive
         * access to the page while it is being written out. Note that once this
         * is set, the page may be fixed for read access.
         */
        private volatile boolean writeInProgress = false;
        
        /**
         * Indicates that the page has just been read in. This is used to
         * determine whether to increment the fixcount or not.
         */
        private boolean newBuffer = true;


        /**
         * To protect access to this BCB.
         */
        private Lock lock = new ReentrantLock();
        
        /**
         * Condition to signal completion of IO.
         */
        private Condition ioDone = lock.newCondition();
        
        BufferControlBlock(PageId pageId) {
            this.pageId = pageId;
        }

        @Override
        public final String toString() {
            StringBuilder sb = new StringBuilder();
            return appendTo(sb).toString();
        }

        public StringBuilder appendTo(StringBuilder sb) {
            sb.append("BCB(pageid=");
            getPageId().appendTo(sb);
            sb.append(", frame=").append(getFrameIndex());
            sb.append(", isDirty=").append(dirty).append(", fixcount=").append(fixcount);
            sb.append(", isBeingWritten=").append(writeInProgress);
            sb.append(", recoveryLsn=");
            if (recoveryLsn == null) {
                sb.append("null");
            }
            else {
                recoveryLsn.appendTo(sb);
            }
            sb.append(", invalid=" + invalid);
            sb.append(")");
            return sb;
        }
        
        final boolean isInUse() {
            return fixcount > 0;
        }

        final void incrementFixCount() {
            fixcount++;
        }

        final void decrementFixCount() {
            fixcount--;
            assert fixcount >= 0;
        }

        final boolean isValid() {
            return !invalid;
        }

        final boolean isInvalid() {
            return invalid;
        }

        final void setInvalid(boolean value) {
            invalid = value;
        }

        final boolean isBeingRead() {
            return getFrameIndex() == -1;
        }

        final boolean isBeingWritten() {
            return writeInProgress;
        }

        final void setBeingWritten(boolean value) {
            writeInProgress = value;
        }

        final boolean isDirty() {
            return dirty;
        }

        final void setDirty(boolean value) {
            dirty = value;
        }

        final void setFrameIndex(int frameIndex) {
            this.frameIndex = frameIndex;
        }

        final int getFrameIndex() {
            return frameIndex;
        }

        final PageId getPageId() {
            return pageId;
        }

        final void setRecoveryLsn(Lsn recoveryLsn) {
            this.recoveryLsn = recoveryLsn;
        }

        final Lsn getRecoveryLsn() {
            return recoveryLsn;
        }

        final DirtyPageInfo getDirtyPageInfo() {
            if (isDirty() && isValid()) {
                assert !recoveryLsn.isNull();
                DirtyPageInfo dp = new DirtyPageInfo(
                    getPageId(),
                    getRecoveryLsn(),
                    getRecoveryLsn());
                return dp;
            }
            return null;
        }

        final void setNewBuffer(boolean newBuffer) {
            this.newBuffer = newBuffer;
        }
        
        final boolean isNewBuffer() {
            return newBuffer;
        }
        
        final void lock() {
        	lock.lock();
        }
        
        final void unlock() {
        	lock.unlock();
        }
        
        /**
         * Caller must hold the lock on the BCB before invoking this.
         * The wait is time limited and interruptible, therefore caller must
         * re-test conditions after this method returns.
         */
        final void awaitIOCompletion() {
        	try {
				ioDone.await(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
			}
        }
        
        /**
         * Caller must hold the lock on the BCB before invoking this.
         */
        final void signalIOCompleted() {
        	ioDone.signalAll();
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
        final private ReentrantReadWriteLock latch = new ReentrantReadWriteLock();

        /**
         * A linked list of hashed BCBs.
         * <p>
         * Although the java.util.LinkedList is inefficient for removal, 
         * we don't bother with a custom implementation here because the
         * hash bucket chains are expected to be small.
         */
        final LinkedList<BufferControlBlock> chain = new LinkedList<BufferControlBlock>();

        final void lockExclusive() {
            latch.writeLock().lock();
        }

        final void unlockExclusive() {
            latch.writeLock().unlock();
        }

        final boolean tryLockExclusive() {
            return latch.writeLock().tryLock();
        }
        
        final void lockShared() {
        	latch.readLock().lock();
        }
        
        final void unlockShared() {
        	latch.readLock().unlock();
        }
        
        final boolean tryLockShared() {
        	return latch.readLock().tryLock();
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

        BufferAccessBlockImpl(BufferManagerImpl bufMgr, BufferControlBlock bcb,
                Page page) {
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
            	bufMgr.exceptionHandler.errorThrow(this.getClass().getName(), 
            			"unlatch", new IllegalStateException(bufMgr.mcat
                    .getMessage("EM0007")));
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
            } else {
            	bufMgr.exceptionHandler.errorThrow(this.getClass().getName(), 
            			"setDirty", new IllegalStateException(bufMgr.mcat
                    .getMessage("EM0008")));
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
            	bufMgr.exceptionHandler.errorThrow(
                    this.getClass().getName(),
                    "upgradeUpdateLatch",new IllegalStateException(bufMgr.mcat
                    .getMessage("EM0009")));
            }
            page.upgradeUpdate();
            latchMode = BufferManagerImpl.LATCH_EXCLUSIVE;
        }

        /* (non-Javadoc)
         * @see org.simpledbm.bm.BufferAccessBlock#downgradeExclusiveLatch()
         */
        public void downgradeExclusiveLatch() {
            if (latchMode != BufferManagerImpl.LATCH_EXCLUSIVE) {
                bufMgr.exceptionHandler.errorThrow(
                    this.getClass().getName(),
                    "downgradeExclusiveLatch",
                    new IllegalStateException(bufMgr.mcat
                    .getMessage("EM0010")));
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

        final BufferManagerImpl bufmgr;

        BufferWriter(BufferManagerImpl bufmgr) {
            this.bufmgr = bufmgr;
        }

        public final void run() {
            bufmgr.log.info(this.getClass().getName(), "run", bufmgr.mcat
                .getMessage("IM0011"));
            for (;;) {
                LockSupport.parkNanos(TimeUnit.NANOSECONDS.convert(
                    bufmgr.bufferWriterSleepInterval,
                    TimeUnit.MILLISECONDS));
                try {
                    if (bufmgr.log.isTraceEnabled()) {
                    	bufmgr.log.trace(
                            this.getClass().getName(),
                            "run",
                            "SIMPLEDBM-DEBUG: Before Writing Buffers: Dirty Buffers Count = "
                                    + bufmgr.getDirtyBuffersCount());
                    }
                    long start = System.currentTimeMillis();
                    bufmgr.writeBuffers();
                    long end = System.currentTimeMillis();
                    if (bufmgr.log.isTraceEnabled()) {
                    	bufmgr.log.trace(
                            this.getClass().getName(),
                            "run",
                            "SIMPLEDBM-DEBUG: After Writing Buffers: Dirty Buffers Count = "
                                    + bufmgr.getDirtyBuffersCount());
                    	bufmgr.log
                            .trace(
                                this.getClass().getName(),
                                "run",
                                "SIMPLEDBM-DEBUG: BUFFER WRITER took "
                                        + (end - start)
                                        + " millisecs to complete writing pages to disk");
                    }
                } catch (Exception e) {
                	bufmgr.log.error(this.getClass().getName(), "run", bufmgr.mcat
                        .getMessage("EM0003"), e);
                    bufmgr.setStop();
                }
                if (bufmgr.stop) {
                    break;
                }
            }
            bufmgr.log.info(this.getClass().getName(), "run", bufmgr.mcat
                .getMessage("IM0012"));
        }
    }

    public void setBufferWriterSleepInterval(int bufferWriterSleepInterval) {
        this.bufferWriterSleepInterval = bufferWriterSleepInterval;
    }

    int getDirtyBuffersCount() {
        return dirtyBuffersCount.get();
    }
    
    final static class BufferManagerStatistics {
    	int bufferpoolsize;
    	volatile int dirtybuffers;
    	volatile int fixcount;
    	volatile int cachehits;
    	int writersleepinterval;
    	int hashTableSize;
    	
    	Properties getStatistics() {
    		Properties p = new Properties();
    		p.setProperty("bufferpoolsize", Integer.toString(bufferpoolsize));
    		p.setProperty("dirtybuffers", Integer.toString(dirtybuffers));
    		p.setProperty("fixcount", Integer.toString(fixcount));
    		p.setProperty("cachehits", Integer.toString(cachehits));
    		p.setProperty("writersleepinterval", Integer.toString(writersleepinterval));
    		p.setProperty("hashtablesize", Integer.toString(hashTableSize));
    		return p;
    	}
    }

    void dumpStatistics() {
    	System.err.println("BufferManager Statistics:");
    	System.err.println(statistics.getStatistics());
    }
    
}
