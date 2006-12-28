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
package org.simpledbm.rss.impl.pm;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageException;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.pm.PageReadException;
import org.simpledbm.rss.api.registry.ObjectRegistry;
import org.simpledbm.rss.api.st.StorageContainer;
import org.simpledbm.rss.api.st.StorageManager;

/**
 * Default implementation of PageFactory.
 * 
 * @author Dibyendu Majumdar
 * @since 14-Aug-2005
 */
public final class PageFactoryImpl implements PageFactory {

    static final int MODULE_ID = 5;
    
    static final int TYPE_BASE = 5 * 100;
    static final int TYPE_RAW_PAGE = TYPE_BASE + 1;
    
    
	/**
	 * Default page size is 8 KB.
	 */
	private static final int DEFAULT_PAGE_SIZE = 8 * 1024;
	
	private final int pageSize;	// default page size is 8K

	/**
	 * An ObjectFactory instance is required for instantiating various page types.
	 */
	private final ObjectRegistry objectFactory;
	
	private final StorageManager storageManager;
	
	private final LatchFactory latchFactory;
	
	public PageFactoryImpl(int pageSize, ObjectRegistry objectFactory, StorageManager storageManager, LatchFactory latchFactory) {
		this.pageSize = pageSize;
		this.objectFactory = objectFactory;
		this.storageManager = storageManager;
		this.latchFactory = latchFactory;
        objectFactory.register(TYPE_RAW_PAGE, RawPage.class.getName());
	}

	public PageFactoryImpl(ObjectRegistry objectFactory, StorageManager storageManager, LatchFactory latchFactory) {
        this(DEFAULT_PAGE_SIZE, objectFactory, storageManager, latchFactory);
	}
	
	public final int getPageSize() {
		return pageSize;
	}

//	public final Page getInstance(String pagetype, PageId pageId) {
//		Page page = (Page) objectFactory.getInstance(pagetype);
//		page.setType(objectFactory.getTypeCode(pagetype));
//        page.setPageFactory(this);
//        page.setLatch(latchFactory.newReadWriteUpdateLatch());
//		page.setPageId(pageId);
//		page.init();
//		return page;
//	}

	public final Page getInstance(int pagetype, PageId pageId) {
		Page page = (Page) objectFactory.getInstance(pagetype);
		page.setType(pagetype);
        page.setPageFactory(this);
        page.setLatch(latchFactory.newReadWriteUpdateLatch());
		page.setPageId(pageId);
		page.init();
		return page;
	}

	private Page getInstance(ByteBuffer bb) {
		bb.mark();
		short pagetype = bb.getShort();
		bb.reset();
		Page page = (Page) objectFactory.getInstance(pagetype);
        page.setPageFactory(this);
        page.setLatch(latchFactory.newReadWriteUpdateLatch());
        page.init();
		page.retrieve(bb);
		return page;
	}

	public final Page retrieve(PageId pageId) throws PageException {
		StorageContainer container = storageManager.getInstance(pageId
				.getContainerId());
		long offset = pageId.getPageNumber() * pageSize;
		byte[] data = new byte[pageSize];
		int n = container.read(offset, data, 0, pageSize);
		if (n != pageSize) {
			throw new PageReadException();
		}
		ByteBuffer bb = ByteBuffer.wrap(data);
		return getInstance(bb);
	}
	
	public final void store(Page page) throws PageException {
		StorageContainer container = storageManager.getInstance(page
				.getPageId().getContainerId());
		if (container == null) {
			throw new PageException("Error writing page " + page
					+ " as container is not available");
		}
		long offset = page.getPageId().getPageNumber() * pageSize;
		byte[] data = new byte[pageSize];
		ByteBuffer bb = ByteBuffer.wrap(data);
		page.store(bb);
		container.write(offset, data, 0, pageSize);
	}

    public int getRawPageType() {
        return TYPE_RAW_PAGE;
    }
    
}
