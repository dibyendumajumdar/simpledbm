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

import org.simpledbm.common.api.registry.Storable;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.tx.Transaction;

/**
 * TupleContainer is a transactional container for tuples. Tuples are
 * essentially blobs of data that can span multiple pages. The container does
 * not care about the contents of tuples.
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
     * Inserts a new tuple and allocates a Location for the tuple. Location is
     * locked exclusively before this method returns. The tuple insertion must
     * be completed by calling {@link TupleInserter#completeInsert()}.
     * <p>
     * Inserting a tuple is a three step process. In the first step, a Location
     * is allocated for the tuple, and tentatively, space reserved in one of the
     * pages of the container. This step also obtains an exclusive lock on the
     * location.
     * <p>
     * In the second step, the caller is expected to perform other actions such
     * as creating primary and secondary index keys.
     * </p>
     * <p>
     * The last (third) step is to complete the data insert. This is achieved by
     * invoking {@link TupleInserter#completeInsert()}.
     * </p>
     */
    TupleInserter insert(Transaction trx, Storable tuple);

    /**
     * Updates the tuple at specified location replacing old contents with the
     * new tuple. Location is locked in exclusive mode.
     */
    void update(Transaction trx, Location location, Storable newTuple);

    /**
     * Deletes the tuple at specified location. Locks the location in exclusive
     * mode.
     */
    void delete(Transaction trx, Location location);

    /**
     * Reads a tuple and returns the contents of the tuple as a byte array. Note
     * that the byte array may contain padding bytes, it is upto the client to
     * ensure that it knows how to parse and interpret the tuple data.
     * <p>
     * No locking is performed in this method; it is assumed that the client has
     * already locked the Location in either Shared or Update mode.
     */
    byte[] read(Location location);

    /**
     * Opens a tuple scan that reads all used pages within the container and
     * returns tuples sequentially. Each tuple is locked in UPDATE mode if
     * forUpdate is true, else in SHARED mode.
     */
    TupleScan openScan(Transaction trx, boolean forUpdate);
}
