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
package org.simpledbm.rss.api.im;

import org.simpledbm.common.api.key.IndexKey;
import org.simpledbm.rss.api.loc.Location;

/**
 * An IndexScan is an implementation of a forward scan on the Index. The scan
 * fetches the next key until it reaches the logical EOF.
 * 
 * @see IndexContainer#openScan(org.simpledbm.rss.api.tx.Transaction, IndexKey,
 *      Location, boolean)
 * @author Dibyendu Majumdar
 */
public interface IndexScan {

    /**
     * Fetches the next available key from the Index. Handles the situation
     * where current key has been deleted. Note that prior to returning the key
     * the Location object associated with the key is locked.
     * <p>
     * After fetching an index row, typically, data must be fetched from
     * associated tuple container. Locks obtained by the fetch protect such
     * access. After tuple has been fetched, caller must invoke
     * {@link #fetchCompleted(boolean)} to ensure that locks are released in
     * certain lock isolation modes. Failure to do so will cause extra locking.
     */
    public boolean fetchNext();

    /**
     * In certain isolation modes, releases locks acquired by
     * {@link #fetchNext()}. Must be invoked after the data from associated
     * tuple container has been fetched.
     * <p>
     * If the argument matched is set to false, the scan is assumed to have
     * reached end of file. The next call to fetchNext() will return false.
     * 
     * @param matched If set to true indicates that the key satisfies search
     *            query
     * @deprecated
     */
    public void fetchCompleted(boolean matched);

    /**
     * Returns the IndexKey on which the scan is currently positioned.
     * 
     * @see #getCurrentLocation()
     */
    public IndexKey getCurrentKey();

    /**
     * Returns the Location associated with the current IndexKey.
     * 
     * @see #getCurrentKey()
     */
    public Location getCurrentLocation();

    /**
     * After the scan is completed, the close method should be called to release
     * all resources acquired by the scan.
     */
    public void close();

    /**
     * Returns the End of File status of the scan. Once the scan has gone past
     * the last available key in the Index, this will return true.
     */
    public boolean isEof();
}
