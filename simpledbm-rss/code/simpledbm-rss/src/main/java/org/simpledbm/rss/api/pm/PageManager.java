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

import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.st.StorageException;

/**
 * The PageFactory is responsible for instantiating new Pages of various 
 * types, as well as for storing and retrieving pages from storage containers.
 * 
 * @author Dibyendu Majumdar
 * @since 14-Aug-2005
 */
public interface PageManager {

    /**
     * Gets the on-disk page size of all pages managed by this factory. Page sizes can be
     * upto 32K. 
     */
    int getPageSize();
    
    /**
     * Get the page size that is usable by pages. This is likely to be less
     * than or equal to {@link #getPageSize()} which returns the actual 
     * physical page size on disk.
     */
    int getUsablePageSize();

    /**
     * Instantiate a Page of the specified type, and initialize it with the 
     * PageID.
     *  
     * @param typecode The Page type code as registered in the Object Registry
     * @param pageId The ID of the Page
     */
    Page getInstance(int typecode, PageId pageId);

    /**
     * Retrieves specified Page from the Storage Container. Note that the correct
     * page type will be automatically created as long as the type has been registered with
     * the Object Registry. The appropriate Storage Container should have been opened
     * and registered with the Storage Manager prior to calling this method.
     *  
     * @param pageId ID of the Page to be retrieved
     * @throws StorageException Thrown if there is an error while retrieving the Page
     */
    Page retrieve(PageId pageId);

    /**
     * Saves the specified Page to the appropriate Storage Container. The appropriate
     * Storage Container must be open and registered with the Storage Manager.
     * 
     * @param page Page to be saved
     * @throws StorageException Thrown if there is an error while writing the page.
     */
    void store(Page page);

    /**
     * Gets the type code for a raw page which is just a blob of bytes.
     */
    int getRawPageType();

    /**
     * Returns the LatchFactory associated with this PageFactory. The LatchFactory is
     * used to create latches used by pages for mutual exclusion.
     */
    LatchFactory getLatchFactory();
    
}
