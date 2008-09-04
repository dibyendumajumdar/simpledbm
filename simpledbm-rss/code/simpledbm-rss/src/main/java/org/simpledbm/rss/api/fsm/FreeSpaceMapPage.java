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
package org.simpledbm.rss.api.fsm;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageFactory;
import org.simpledbm.rss.api.pm.PageId;

/**
 * Interface definition for Space Map pages. 
 */
public abstract class FreeSpaceMapPage extends Page {
	
	protected FreeSpaceMapPage(PageFactory pageFactory, int type,
			PageId pageId) {
		super(pageFactory, type, pageId);
	}

	protected FreeSpaceMapPage(PageFactory pageFactory, PageId pageId, ByteBuffer bb) {
		super(pageFactory, pageId, bb);
	}

	/**
     * Retrieves the current space value associated with the specified page. For a
     * single bit space map, the possible values are 1 and 0. For 2 bit space map,
     * the values range from 0 to 3.
     *   
     * @param pageNumber The page number whose space allocation value is to be retrieved.
     * @throws FreeSpaceManagerException Thrown if the specified page is not handled by this space map page.
     */
    public abstract int getSpaceBits(int pageNumber);

    /**
     * Updates the current space value associated with the specified page. For a
     * single bit space map, the possible values are 1 and 0. For 2 bit space map,
     * the values range from 0 to 3.
     * 
     * @param pageNumber The page number whose space allocation value is to be updated.
     * @param value New space allocation value
     * @throws FreeSpaceManagerException Thrown if the specified page is not handled by this space map page.
     */
    public abstract void setSpaceBits(int pageNumber, int value);

}
