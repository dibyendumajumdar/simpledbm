/**
 * DO NOT REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Contributor(s):
 *
 * The Original Software is SimpleDBM (www.simpledbm.org).
 * The Initial Developer of the Original Software is Dibyendu Majumdar.
 *
 * Portions Copyright 2005-2014 Dibyendu Majumdar. All Rights Reserved.
 *
 * The contents of this file are subject to the terms of the
 * Apache License Version 2 (the "APL"). You may not use this
 * file except in compliance with the License. A copy of the
 * APL may be obtained from:
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the APL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the APL, the GPL or the LGPL.
 *
 * Copies of GPL and LGPL may be obtained from:
 * http://www.gnu.org/licenses/license-list.html
 */
package org.simpledbm.rss.api.pm;

import org.simpledbm.rss.api.latch.LatchFactory;
import org.simpledbm.rss.api.st.StorageException;

/**
 * The PageManager is responsible for instantiating new Pages of various types,
 * as well as for storing and retrieving pages from storage containers. Each
 * page type must have a PageFactory registered with the ObjectRegistry. The
 * PageManager delegates the actual reading/writing of pages to the PageFactory
 * implementations. This division of responsibility allows the PageManager to
 * manage all pages without knowing how to instantiate individual page types. It
 * allows new page types to be added without requiring changes to the
 * PageManager.
 * 
 * @author Dibyendu Majumdar
 * @since 14-Aug-2005
 */
public interface PageManager {

    public final String LOGGER_NAME = "org.simpledbm.pagemgr";

    /**
     * Gets the on-disk page size of all pages managed by this PageManager. Page
     * sizes can be upto 32K.
     */
    int getPageSize();

    /**
     * Get the page size that is usable by pages. This is likely to be less than
     * or equal to {@link #getPageSize()}.
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
     * Retrieves specified Page from the Storage Container. Note that the
     * correct page type will be automatically created as long as the type has
     * been registered with the Object Registry. The appropriate Storage
     * Container should have been opened and registered with the Storage Manager
     * prior to calling this method.
     * 
     * @param pageId ID of the Page to be retrieved
     * @throws StorageException Thrown if there is an error while retrieving the
     *             Page
     */
    Page retrieve(PageId pageId);

    /**
     * Saves the specified Page to the appropriate Storage Container. The
     * appropriate Storage Container must be open and registered with the
     * Storage Manager.
     * 
     * @param page Page to be saved
     * @throws StorageException Thrown if there is an error while writing the
     *             page.
     */
    void store(Page page);

    /**
     * Gets the type code for a raw page which is just a blob of bytes.
     */
    int getRawPageType();

    /**
     * Returns the LatchFactory associated with this PageFactory. The
     * LatchFactory is used to create latches used by pages for mutual
     * exclusion.
     */
    LatchFactory getLatchFactory();

}
