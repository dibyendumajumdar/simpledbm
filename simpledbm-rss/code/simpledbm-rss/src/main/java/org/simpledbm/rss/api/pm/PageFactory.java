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
package org.simpledbm.rss.api.pm;

import java.nio.ByteBuffer;

/**
 * The PageFactory handles instantiation of pages, both in memory and from
 * bytestreams wrapped in ByteBuffer instances. Each Page type must have an
 * associated PageFactory implementation, which should be registered with
 * ObjectRegistry. The PageManager looks up PageFactory implementations in the
 * ObjectRegistry.
 * 
 * @author dibyendumajumdar
 */
public interface PageFactory  {

	/**
	 * Create a new Page of the desired type.
	 */
	Page getInstance(int type, PageId pageId);

	/**
	 * Create a Page instance from the supplied bytestream.
	 */
	Page getInstance(PageId pageId, ByteBuffer bb);

	/**
	 * Return the page type associated with this PageFactory.
	 */
	int getPageType();
	
}
