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

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.rss.api.loc.Location;
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
