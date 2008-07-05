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

import org.simpledbm.rss.api.tx.Transaction;

/**
 * Interface for searching and updating free space information within a Storage Container.
 * Maintains a "current" space map page, and provides methods for updating space allocation
 * data within the current space map page.
 */
public interface FreeSpaceCursor {

    /**
     * Finds the next available page that satisfies the requirements of
     * SpaceChecker, and then latches the concerned Space Map Page
     * exclusively. This is meant to be followed by a call to {@link #updateAndLogRedoOnly(Transaction, int, int)}
     * or {@link #updateAndLogUndoably(Transaction, int, int)} and then by {@link #unfixCurrentSpaceMapPage()}.
     * The fixed page becomes the current space map page.
     * <p>
     * For the sake of efficient searches, this method is allowed to cache data,
     * such as last used space map page. While this method must search all the available
     * space map pages before giving up, the order in which the space map pages are 
     * searched is not defined. 
     * 
     * @param checker SpaceChecker instance
     * @return -1 if page was not found, else page number.
     */
    public int findAndFixSpaceMapPageExclusively(FreeSpaceChecker checker);

    /**
     * Fixes specified Space Map Page exclusively. Note that the space map
     * page must be eventually unfixed by calling {@link #unfixCurrentSpaceMapPage()}. 
     * The fixed page becomes the current space map page.
     */
    public void fixSpaceMapPageExclusively(int spaceMapPageNumber,
            int pageNumber);

    /**
     * Returns the currently fixed space map page.
     */
    public FreeSpaceMapPage getCurrentSpaceMapPage();

    /**
     * Updates space allocation data for specfified page within the 
     * current space map page, and generates a Redo-only log record for the
     * change.
     */
    public void updateAndLogRedoOnly(Transaction trx, int pageNumber, int value);

    /**
     * Updates space allocation data for specfified page within the 
     * current space map page, and generates a Redo-Undo log record for the
     * change.
     */
    public void updateAndLogUndoably(Transaction trx, int pageNumber, int value);

    /**
     * Unfixes the current space map page.
     */
    public void unfixCurrentSpaceMapPage();
}
