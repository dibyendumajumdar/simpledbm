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

import org.simpledbm.common.api.exception.ExceptionHandler;
import org.simpledbm.common.api.platform.Platform;
import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.api.registry.ObjectRegistry;
import org.simpledbm.common.util.ChecksumCalculator;
import org.simpledbm.common.util.TypeSize;
import org.simpledbm.common.util.logging.Logger;
import org.simpledbm.common.util.mcat.Message;
import org.simpledbm.common.util.mcat.MessageInstance;
import org.simpledbm.common.util.mcat.MessageType;
import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageException;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.pm.PageReadException;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageManager;

/**
 * The PageManager handles the reading and writing of pages. The actual reading/writing
 * of a particular Page type is delegated to appropriate PageFactory implementation.
 *  
 * @author Dibyendu Majumdar
 * @since 14-Aug-2005
 */
public final class PageManagerImpl implements PageManager {

    static final int MODULE_ID = 5;

    static final int TYPE_BASE = 10;
    static final int TYPE_RAW_PAGE = TYPE_BASE + 1;

    final Logger log;
    
    final ExceptionHandler exceptionHandler;

	/**
     * Default page size is 8 KB.
     */
    private static final int DEFAULT_PAGE_SIZE = 8 * 1024;

    /**
     * The size of all pages managed by the PageManager is fixed at
     * the time of construction. 
     */
    private final int pageSize; // default page size is 8K

    /**
     * An ObjectRegistry instance is required for obtaining access to PageFactory
     * implementations for various page types.
     */
    private final ObjectRegistry objectRegistry;

    /**
     * StorageManager provides access to the StorageContainers by their
     * container Id.
     */
    private final StorageManager storageManager;

    /**
     * LatchFactory is used to create latches that are assigned to pages.
     */
    private final LatchFactory latchFactory;

    // Page Manager messages
	static Message m_EP0001 = new Message('R', 
			'P',
			MessageType.ERROR,
			1,
			"Error occurred while reading page {0}: the number of bytes read is {1}; but expected {2} bytes");
	static Message m_EP0002 = new Message('R', 'P', MessageType.ERROR, 2,
			"Error occurred while reading page {0}: container not available");
	static Message m_EP0003 = new Message('R', 'P', MessageType.ERROR, 3,
			"Error occurred while writing page {0}: container not available");
	static Message m_EP0004 = new Message('R', 'P', MessageType.ERROR, 4,
			"Error occurred while reading page {0}: checksum invalid");
	static Message m_EP0005 = new Message('R', 'P', MessageType.ERROR, 5,
			"A PageFactory was not available to handle page type {0}");    
    
    public PageManagerImpl(Platform platform, int pageSize, ObjectRegistry objectRegistry,
            StorageManager storageManager, LatchFactory latchFactory, Properties p) {
    	PlatformObjects po = platform.getPlatformObjects(PageManager.LOGGER_NAME);
    	this.log = po.getLogger();
    	this.exceptionHandler = po.getExceptionHandler();
        this.pageSize = pageSize;
        this.objectRegistry = objectRegistry;
        this.storageManager = storageManager;
        this.latchFactory = latchFactory;
        objectRegistry.registerSingleton(TYPE_RAW_PAGE, new RawPage.RawPageFactory(this));
    }

    public PageManagerImpl(Platform platform, ObjectRegistry objectRegistry,
            StorageManager storageManager, LatchFactory latchFactory, Properties p) {
        this(platform, DEFAULT_PAGE_SIZE, objectRegistry, storageManager, latchFactory, p);
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.pm.PageManager#getPageSize()
     */
    public final int getPageSize() {
        return pageSize;
    }
    
    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.pm.PageManager#getUsablePageSize()
     */
    public final int getUsablePageSize() {
    	/*
    	 * The usable page size is somewhat less than the actual page size, because
    	 * we use 4 bytes to store a checksum in the page.
    	 */
    	return pageSize - TypeSize.LONG;
    }

    private PageFactory getPageFactory(int pagetype) {
    	PageFactory pf = (PageFactory) objectRegistry.getSingleton(pagetype);
    	if (null == pf) {
            exceptionHandler.errorThrow(this.getClass().getName(), "getPageFactory", 
            		new PageException(new MessageInstance(m_EP0005, pagetype)));
    	}
    	return pf;
    }
    
    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.pm.PageFactory#getInstance(int, org.simpledbm.rss.api.pm.PageId)
     */
    public final Page getInstance(int pagetype, PageId pageId) {
    	PageFactory pf = getPageFactory(pagetype);
    	Page page = pf.getInstance(pagetype, pageId);
        return page;
    }

    /**
     * Unmarshalls a byte stream to a page. 
     * First two bytes must contain the page type information for the page. 
     * This is used to obtain the correct PageFactory implementation
     * from the Object Registry. 
     * 
     * @param pageId The ID of the page to be unmarshalled
     * @param bb The ByteBuffer that provides access to the byte stream
     * @return Page instance initialized with the contents of the byte stream
     */
    private Page getInstance(PageId pageId, ByteBuffer bb) {
    	/*
    	 * Get the page type
    	 */
        bb.mark();
        short pagetype = bb.getShort();
        bb.reset();
        /*
         * Obtain the relevant PageFactory implementation
         */
    	PageFactory pf = getPageFactory(pagetype);
    	/*
    	 * Instantiate the page
    	 */
    	Page page = pf.getInstance(pageId, bb);
        return page;
    }
    
    
    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.pm.PageFactory#retrieve(org.simpledbm.rss.api.pm.PageId)
     */
    public final Page retrieve(PageId pageId) {
        StorageContainer container = storageManager.getInstance(pageId
            .getContainerId());
        if (container == null) {
            exceptionHandler.errorThrow(this.getClass().getName(), "retrieve", 
            		new PageException(new MessageInstance(m_EP0002, pageId)));
        }
        long offset = pageId.getPageNumber() * pageSize;
        byte[] data = new byte[pageSize];
        int n = container.read(offset, data, 0, pageSize);
        if (n != pageSize) {
            exceptionHandler.errorThrow(this.getClass().getName(), "retrieve", 
            	new PageReadException(new MessageInstance(m_EP0001,
            			pageId,
            			n,
            			pageSize)));
        }
        /*
         * The first 4 bytes of a page contains a checksum calculated over the
         * rest of the page data. We need to validate that the checksum obtained
         * from the page matches the calculated checksum of the page data.
         */
        long checksumCalculated = ChecksumCalculator.compute(data, TypeSize.LONG, pageSize-TypeSize.LONG);
        ByteBuffer bb = ByteBuffer.wrap(data);
        long checksumOnPage = bb.getLong();
        if (checksumOnPage != checksumCalculated) {
        	exceptionHandler.errorThrow(this.getClass().getName(), "retrieve", 
        			new PageReadException(new MessageInstance(m_EP0004,
        					pageId)));
        }
        return getInstance(pageId, bb);
    }

	/* (non-Javadoc)
     * @see org.simpledbm.rss.api.pm.PageFactory#store(org.simpledbm.rss.api.pm.Page)
     */
    public final void store(Page page) {
        StorageContainer container = storageManager.getInstance(page
            .getPageId()
            .getContainerId());
        if (container == null) {
            exceptionHandler.errorThrow(this.getClass().getName(), "retrieve", 
            		new PageException(new MessageInstance(m_EP0003, page.getPageId())));
        }

        byte[] data = new byte[pageSize];
        ByteBuffer bb = ByteBuffer.wrap(data);
        bb.mark();
        /*
         * The first 4 bytes of a page contains a checksum calculated over the
         * rest of the page data. 
         * As we do not know the checksum yet, we insert a place holder.
         */
        bb.putLong(0);
        page.store(bb);
        /*
         * Calculate the checksum and store it in the first 4 bytes.
         */
        long checksum = ChecksumCalculator.compute(data, TypeSize.LONG, pageSize-TypeSize.LONG);      
        bb.reset();
        bb.putLong(checksum);
        /*
         * Now we can persist the page.
         */
        long offset = page.getPageId().getPageNumber() * pageSize;
        container.write(offset, data, 0, pageSize);
    }

    public int getRawPageType() {
        return TYPE_RAW_PAGE;
    }

    public LatchFactory getLatchFactory() {
		return latchFactory;
	}

}
