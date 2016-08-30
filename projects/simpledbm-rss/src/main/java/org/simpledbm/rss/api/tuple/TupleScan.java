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
package org.simpledbm.rss.api.tuple;

import org.simpledbm.rss.api.loc.Location;

/**
 * Specifies the interface for tuple scans. A TupleScan visits all the non-empty
 * pages in the container and returns tuples in those pages. Note that unlike
 * IndexScans, TupleScans do not support next-key locking, hence cannot ensure
 * repeatable reads.
 * 
 * @see org.simpledbm.rss.api.tuple.TupleContainer#openScan(org.simpledbm.rss.api.tx.Transaction,
 *      boolean)
 * @author Dibyendu Majumdar
 */
public interface TupleScan {

    /**
     * Attempts to position the scan cursor on the next available tuple. Tuple
     * will be locked in the mode specified when the scan was opened. If there
     * are no more tuples to be retrieved, this method will return false.
     */
    boolean fetchNext();

    /**
     * Retrieves the contents of the current tuple. Valid only after a call to
     * {@link #fetchNext()}.
     */
    byte[] getCurrentTuple();

    /**
     * Returns the current tuple location. Valid only after a call to
     * {@link #fetchNext()}.
     */
    Location getCurrentLocation();

    /**
     * Returns true if EOF has been reached.
     */
    boolean isEof();

    /**
     * Closes the scan and releases resources acquired for the scan. Note that
     * locks obtained during the scan are not released here.
     */
    void close();
}
