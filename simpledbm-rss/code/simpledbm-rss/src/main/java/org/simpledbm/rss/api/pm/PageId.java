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
package org.simpledbm.rss.api.pm;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.st.Storable;

/**
 * Each page in the database is uniquely identified by a pageid consisting of 
 * storage container id and the page number.
 * 
 * @author Dibyendu Majumdar
 * @since 19-Aug-2005
 */
public final class PageId implements Comparable<PageId>, Storable {

    /**
     * Size of PageId in bytes. 
     */
	public final static int SIZE =  (Integer.SIZE / Byte.SIZE) * 2;
	
	private int containerId;
	private int pageNumber;
	
	public PageId() {
		containerId = -1;
		pageNumber = -1;
	}
	
	public PageId(int containerId, int pageNumber) {
		this.containerId = containerId;
		this.pageNumber = pageNumber;
	}
	
	public PageId(PageId pageId) {
		this.containerId = pageId.containerId;
		this.pageNumber = pageId.pageNumber;
	}

	@Override
	public final boolean equals(Object obj) {
		if (obj instanceof PageId) {
			PageId pageId = (PageId) obj;
			return containerId == pageId.containerId && pageNumber == pageId.pageNumber;
		}
		return false;
	}

	public final int compareTo(PageId pageId) {
		if (containerId == pageId.containerId) {
			if (pageNumber == pageId.pageNumber)
				return 0;
			else if (pageNumber > pageId.pageNumber)
				return 1;
			else
				return -1;
		} else if (containerId > pageId.containerId)
			return 1;
		else
			return -1;
	}
	
	
	public final void retrieve(ByteBuffer bb) {
		containerId = bb.getInt();
		pageNumber = bb.getInt();
	}

	public final void store(ByteBuffer bb) {
		bb.putInt(containerId);
		bb.putInt(pageNumber);
	}

	public final int getStoredLength() {
		return SIZE;
	}

	public final int getContainerId() {
		return containerId;
	}

	public final int getPageNumber() {
		return pageNumber;
	}
	
	public final boolean isNull() {
		return containerId == -1 && pageNumber == -1;
	}
	
	@Override
	public final String toString() {
		return "PageId(" + containerId + "," + pageNumber + ")";
	}

	@Override
	public final int hashCode() {
		return containerId ^ pageNumber;
	}
	
}
