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
 *    Email  : dibyendu@mazumdar.demon.co.uk
 */
package org.simpledbm.rss.api.tuple;

import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.st.Storable;
import org.simpledbm.rss.api.tx.Transaction;

/**
 * TupleContainer is a transactional container for tuples. Tuples are essentially
 * blobs of data that can span multiple pages. The container does not care about the
 * contents of tuples.
 * <p>
 * Tuples can be inserted, deleted or read from a container.
 * <p>
 * The Tuple Container is meant to be the basis for implementing Relational
 * Tables. 
 * 
 * @author Dibyendu Majumdar
 * @since 07-Dec-2005
 */
public interface TupleContainer {

    /**
     * Inserts a new tuple and allocates a Location for the tuple.
     * Location is locked exclusively before this method returns.
     * The tuple insertion must be completed by calling {@link TupleInserter#completeInsert()}.
     * <p>
     * Inserting a tuple is a three step process. In the first step,
     * a Location is allocated for the tuple, and tentatively, space
     * reserved in one of the pages of the container. This step also
     * obtains an exclusive lock on the location. 
     * <p> In the second step, the caller is 
     * expected to perform other actions such as creating primary 
     * and secondary index keys.</p> 
     * <p>The last (third) step is to complete the data insert. This
     * is achieved by invoking {@link TupleInserter#completeInsert()}.</p>
     */
    TupleInserter insert(Transaction trx, Storable tuple);

    /**
     * Updates the tuple at specified location replacing old contents with the
     * new tuple. Location is locked in exclusive mode.
     */
    void update(Transaction trx, Location location, Storable newTuple);

    /**
     * Deletes the tuple at specified location.
     * Locks the location in exclusive mode.
     */
    void delete(Transaction trx, Location location);

    /**
     * Reads a tuple and returns the contents of the tuple as a byte array.
     * Note that the byte array may contain padding bytes, it is upto the client
     * to ensure that it knows how to parse and interpret the tuple data.
     * <p>
     * No locking is performed in this method; it is assumed that the client
     * has already locked the Location in either Shared or Update mode.
     */
    byte[] read(Location location);

    /**
     * Opens a tuple scan that reads all used pages within the container
     * and returns tuples sequentially. Each tuple is locked in UPDATE mode if
     * forUpdate is true, else in SHARED mode.
     */
    TupleScan openScan(Transaction trx, boolean forUpdate);
}
