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

import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageManager;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageId;

/**
 * A basic implementation of a page.
 * 
 * @author Dibyendu Majumdar
 * @since 14-Aug-2005
 */
public final class RawPage extends Page {

//    public RawPage(PageFactory pageFactory) {
//        super(pageFactory);
//    }
//    
//    public RawPage(PageFactory pageFactory, ByteBuffer buf) {
//    	super(pageFactory, buf);
//    }
    
//    @Override
//    public final void init() {
//        // does nothing
//    }

    RawPage(PageManager pageFactory, int type, PageId pageId) {
		super(pageFactory, type, pageId);
	}

	RawPage(PageManager pageFactory, PageId pageId, ByteBuffer bb) {
		super(pageFactory, pageId, bb);
	}


	static final class RawPageFactory implements PageFactory {

    	final PageManager pageFactory;
    	
    	public RawPageFactory(PageManager pageFactory) {
    		this.pageFactory = pageFactory;
    	}
//    	
//		public Class<?> getType() {
//			return RawPage.class;
//		}
//
//		public Object newInstance() {
//			return new RawPage(pageFactory);
//		}
//
//		public Object newInstance(ByteBuffer buf) {
//			return new RawPage(pageFactory, buf);
//		}
		public Page getInstance(int type, PageId pageId) {
			return new RawPage(pageFactory, type, pageId);
		}

		public Page getInstance(PageId pageId, ByteBuffer bb) {
			return new RawPage(pageFactory, pageId, bb);
		}

		public int getPageType() {
			return PageFactoryImpl.TYPE_RAW_PAGE;
		}
    	
    }
    
}
