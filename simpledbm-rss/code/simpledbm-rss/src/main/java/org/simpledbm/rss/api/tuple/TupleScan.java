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
package org.simpledbm.rss.api.tuple;

import org.simpledbm.rss.api.loc.Location;

/**
 * Specifies the interface for tuple scans. A TupleScan visits all the non-empty
 * pages in the container and returns tuples in those pages. Note that unlike
 * IndexScans, TupleScans do not support next-key locking, hence cannot ensure
 * repeatable reads.
 * 
 * @see org.simpledbm.rss.api.tuple.TupleContainer#openScan(org.simpledbm.rss.api.tx.Transaction, boolean)
 * @author Dibyendu Majumdar
 */
public interface TupleScan {

    /**
     * Attempts to position the scan cursor on the next available tuple.
     * Tuple will be locked in the mode specified when the scan was opened.
     * If there are no more tuples to be retrieved, this method will return false.
     */
    boolean fetchNext();

    /**
     * Retrieves the contents of the current tuple. Valid only after a 
     * call to {@link #fetchNext()}.
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
     * Closes the scan and releases resources acquired for the scan.
     * Note that locks obtained during the scan are not released here.
     */
    void close();
}
