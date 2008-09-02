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

import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageException;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageFactoryHelper;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.pm.PageReadException;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageManager;
import org.simpledbm.rss.util.ChecksumCalculator;
import org.simpledbm.rss.util.TypeSize;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;

/**
 * Default implementation of PageFactory.
 * 
 * @author Dibyendu Majumdar
 * @since 14-Aug-2005
 */
public final class PageFactoryImpl implements PageFactory {

    static final int MODULE_ID = 5;

    static final int TYPE_BASE = 10;
    static final int TYPE_RAW_PAGE = TYPE_BASE + 1;

    private static Logger log = Logger.getLogger(PageFactoryImpl.class
        .getPackage()
        .getName());

    /**
     * Default page size is 8 KB.
     */
    private static final int DEFAULT_PAGE_SIZE = 8 * 1024;

    private final int pageSize; // default page size is 8K

    /**
     * An ObjectFactory instance is required for instantiating various page types.
     */
    private final ObjectRegistry objectFactory;

    private final StorageManager storageManager;

    private final LatchFactory latchFactory;

    public LatchFactory getLatchFactory() {
		return latchFactory;
	}

	private final MessageCatalog mcat = new MessageCatalog();

    public PageFactoryImpl(int pageSize, ObjectRegistry objectFactory,
            StorageManager storageManager, LatchFactory latchFactory, Properties p) {
        this.pageSize = pageSize;
        this.objectFactory = objectFactory;
        this.storageManager = storageManager;
        this.latchFactory = latchFactory;
        objectFactory.registerSingleton(TYPE_RAW_PAGE, new RawPage.RawPageFactory(this));
    }

    public PageFactoryImpl(ObjectRegistry objectFactory,
            StorageManager storageManager, LatchFactory latchFactory, Properties p) {
        this(DEFAULT_PAGE_SIZE, objectFactory, storageManager, latchFactory, p);
    }

    public final int getPageSize() {
        return pageSize;
    }
    
    public final int getUsablePageSize() {
    	return pageSize - TypeSize.LONG;
    }

    /* (non-Javadoc)
     * @see org.simpledbm.rss.api.pm.PageFactory#getInstance(int, org.simpledbm.rss.api.pm.PageId)
     */
    public final Page getInstance(int pagetype, PageId pageId) {
/*        Page page = (Page) objectFactory.getInstance(pagetype);
        page.setType(pagetype);
//        page.setPageFactory(this);
        page.setLatch(latchFactory.newReadWriteUpdateLatch());
        page.setPageId(pageId);
//        page.init();
 * 
 */
    	PageFactoryHelper pf = (PageFactoryHelper) objectFactory.getInstance(pagetype);
    	Page page = pf.getInstance(pagetype, pageId);
        return page;
    }

    /**
     * Converts a byte stream to a page. 
     * First two bytes must contain the type
     * information for the page. This is used to obtain the correct Page implementation
     * from the Object Registry. 
     * 
     * @param bb The ByteBuffer that provides access to the byte stream
     * @return Page instance initialized with the contents of the byte stream
     */
    private Page getInstance(PageId pageId, ByteBuffer bb) {
        bb.mark();
        short pagetype = bb.getShort();
        bb.reset();
/*
        Page page = (Page) objectFactory.getInstance(pagetype, bb);
//        page.setPageFactory(this);
        page.setLatch(latchFactory.newReadWriteUpdateLatch());
//        page.init();
//        page.retrieve(bb);
        page.setPageId(pageId);
*/
    	PageFactoryHelper pf = (PageFactoryHelper) objectFactory.getInstance(pagetype);
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
            log.error(this.getClass().getName(), "retrieve", mcat.getMessage(
                "EP0002",
                pageId));
            throw new PageException(mcat.getMessage("EP0002", pageId));
        }
        long offset = pageId.getPageNumber() * pageSize;
        byte[] data = new byte[pageSize];
        int n = container.read(offset, data, 0, pageSize);
        if (n != pageSize) {
            log.error(this.getClass().getName(), "retrieve", mcat.getMessage(
                "EP0001",
                pageId,
                n,
                pageSize));
            throw new PageReadException(mcat.getMessage(
                "EP0001",
                pageId,
                n,
                pageSize));
        }
        long checksumCalculated = ChecksumCalculator.compute(data, TypeSize.LONG, pageSize-TypeSize.LONG);
        ByteBuffer bb = ByteBuffer.wrap(data);
        long checksumOnPage = bb.getLong();
        //System.out.println("Calculated checksum = " + checksumCalculated);
        //System.out.println("Retrieved checksum = " + checksumOnPage);
        if (checksumOnPage != checksumCalculated) {
        	log.error(this.getClass().getName(),"retrieve", mcat.getMessage("EP0004",
                    pageId));
            throw new PageReadException(mcat.getMessage(
                    "EP0004",
                    pageId));
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
            log.error(this.getClass().getName(), "retrieve", mcat.getMessage(
                "EP0003",
                page.getPageId()));
            throw new PageException(mcat.getMessage("EP0003", page.getPageId()));
        }
        long offset = page.getPageId().getPageNumber() * pageSize;
        byte[] data = new byte[pageSize];
        ByteBuffer bb = ByteBuffer.wrap(data);
        bb.mark();
        bb.putLong(0);
        page.store(bb);
        long checksum = ChecksumCalculator.compute(data, TypeSize.LONG, pageSize-TypeSize.LONG);
        //System.out.println("Stored checksum = " + checksum);        
        bb.reset();
        bb.putLong(checksum);
        container.write(offset, data, 0, pageSize);
    }

    public int getRawPageType() {
        return TYPE_RAW_PAGE;
    }

}
