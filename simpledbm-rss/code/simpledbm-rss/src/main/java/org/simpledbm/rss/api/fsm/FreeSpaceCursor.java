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
package org.simpledbm.rss.api.fsm;

import org.simpledbm.rss.api.tx.Transaction;

/**
 * Interface for searching and updating free space information within a Storage
 * Container. Maintains a "current" space map page, and provides methods for
 * updating space allocation data within the current space map page.
 */
public interface FreeSpaceCursor {

    /**
     * Finds the next available page that satisfies the requirements of
     * {@link FreeSpaceChecker}, and then latches the concerned Space Map Page
     * exclusively. This is meant to be followed by a call to
     * {@link #updateAndLogRedoOnly(Transaction, int, int)
     * updateAndLogRedoOnly()} or
     * {@link #updateAndLogUndoably(Transaction, int, int)
     * updateAndLogUndoably()} and then by {@link #unfixCurrentSpaceMapPage()}.
     * <p>
     * The fixed page becomes the current space map page.
     * <p>
     * For the sake of efficient searches, this method is allowed to cache data,
     * such as last used space map page. While this method must search all the
     * available space map pages before giving up, the order in which the space
     * map pages are searched is not defined.
     * 
     * @param checker FreeSpaceChecker instance
     * @return -1 if page was not found, else page number.
     */
    public int findAndFixSpaceMapPageExclusively(FreeSpaceChecker checker);

    /**
     * Finds the next available page that satisfies the requirements of
     * FreeSpaceChecker, and then latches the concerned Space Map Page in shared
     * mode.
     * <p>
     * The fixed page becomes the current space map page.
     * <p>
     * For the sake of efficient searches, this method is allowed to cache data,
     * such as last used space map page. While this method must search all the
     * available space map pages before giving up, the order in which the space
     * map pages are searched is not defined.
     * 
     * @param checker FreeSpaceChecker instance
     * @return -1 if page was not found, else page number.
     */
    public int findAndFixSpaceMapPageShared(FreeSpaceChecker checker);

    /**
     * Fixes specified Space Map Page exclusively. Note that the space map page
     * must be eventually unfixed by calling {@link #unfixCurrentSpaceMapPage()}
     * .
     * <p>
     * The fixed page becomes the current space map page.
     */
    public void fixSpaceMapPageExclusively(int spaceMapPageNumber,
            int pageNumber);

    /**
     * Returns the currently fixed space map page.
     */
    public FreeSpaceMapPage getCurrentSpaceMapPage();

    /**
     * Updates space allocation data for specified page within the current space
     * map page, and generates a Redo-only log record for the change.
     */
    public void updateAndLogRedoOnly(Transaction trx, int pageNumber, int value);

    /**
     * Updates space allocation data for specified page within the current space
     * map page, and generates a Redo-Undo log record for the change.
     */
    public void updateAndLogUndoably(Transaction trx, int pageNumber, int value);

    /**
     * Unfixes the current space map page.
     */
    public void unfixCurrentSpaceMapPage();

    /**
     * Gets the container Id associated with this cursor.
     */
    public int getContainerId();
}
