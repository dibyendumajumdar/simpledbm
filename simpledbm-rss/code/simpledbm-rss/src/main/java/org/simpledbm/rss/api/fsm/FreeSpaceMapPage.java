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
package org.simpledbm.rss.api.fsm;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.pm.Page;
import org.simpledbm.rss.api.pm.PageId;
import org.simpledbm.rss.api.pm.PageManager;

/**
 * Abstract base class for Free Space Map pages. 
 */
public abstract class FreeSpaceMapPage extends Page {
	
	protected FreeSpaceMapPage(PageManager pageFactory, int type,
			PageId pageId) {
		super(pageFactory, type, pageId);
	}

	protected FreeSpaceMapPage(PageManager pageFactory, PageId pageId, ByteBuffer bb) {
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
