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
package org.simpledbm.rss.api.im;

import org.simpledbm.rss.api.loc.Location;

/**
 * An IndexScan is an implementation of a forward scan on the
 * Index. The scan fetches the next key until it reaches the logical
 * EOF. 
 * 
 * @see IndexContainer#openScan(org.simpledbm.rss.api.tx.Transaction, IndexKey, Location, boolean)
 * @author Dibyendu Majumdar
 */
public interface IndexScan {

    /**
     * Fetches the next available key from the Index. Handles the situation
     * where current key has been deleted. Note that prior to returning the
     * key the Location object associated with the key is locked.
     * <p>After fetching an index row, typically, data must be fetched from
     * associated tuple container. Locks obtained by the fetch protect such
     * access. After tuple has been fetched, caller must invoke {@link #fetchCompleted()}
     * to ensure that locks are released in certain lock isolation modes. Failure
     * to do so will cause extra locking.
     */
    public boolean fetchNext();

    /**
     * In certain isolation modes, releases locks acquired by {@link #fetchNext()}.
     * Must be invoked after the data from associated tuple container has been
     * fetched.
     * <p>If the argument matched is set to false, the scan is assumed to have reached
     * eof of file. The next call to fetchNext() will return false.
     * 
     * @param matched If set to true indicates that the key satisfies search query
     */
    public void fetchCompleted(boolean matched);

    /**
     * Returns the IndexKey on which the scan is currently positioned.
     * @see #getCurrentLocation()
     */
    public IndexKey getCurrentKey();

    /**
     * Returns the Location associated with the current IndexKey.
     * @see #getCurrentKey()
     */
    public Location getCurrentLocation();

    /**
     * After the scan is completed, the close method should be called to
     * release all resources acquired by the scan.
     */
    public void close();

    /**
     * Returns the End of File status of the scan. Once the scan has gone past
     * the last available key in the Index, this will return true.  
     */
    public boolean isEof();
}
