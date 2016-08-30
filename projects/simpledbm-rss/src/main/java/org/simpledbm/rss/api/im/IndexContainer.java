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
import org.simpledbm.common.api.locking.LockMode;
import org.simpledbm.common.api.tx.IsolationMode;
import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.tx.Transaction;

/**
 * Defines the interface for manipulating an Index.
 * 
 * @author Dibyendu Majumdar
 */
public interface IndexContainer {

    /**
     * Inserts a new key and location. If the Index is unique, only one instance
     * of key is allowed. In non-unique indexes, multiple instances of the same
     * key may exist, but only one instance of the combination of key/location
     * is allowed.
     * <p>
     * The caller must obtain a Shared lock on the Index Container prior to this
     * call.
     * <p>
     * The caller must acquire an Exclusive lock on Location before this call.
     * 
     * @param trx Transaction managing the insert
     * @param key Key to be inserted
     * @param location Location associated with the key
     */
    public void insert(Transaction trx, IndexKey key, Location location);

    /**
     * Deletes specified key and location.
     * <p>
     * The caller must obtain a Shared lock on the Index Container prior to this
     * call.
     * <p>
     * The caller must acquire an Exclusive lock on Location before this call.
     * 
     * @param trx Transaction managing the delete
     * @param key Key to be deleted
     * @param location Location associated with the key
     */
    public void delete(Transaction trx, IndexKey key, Location location);

    /**
     * Opens a new index scan. The Scan will fetch keys >= the specified key and
     * location. Before returning fetched keys, the associated Location objects
     * will be locked. The lock mode depends upon the forUpdate flag. The
     * {@link IsolationMode} of the transaction determines when lock are
     * released.
     * <p>
     * Caller must obtain a Shared lock on the Index Container prior to calling
     * this method.
     * 
     * @param trx Transaction that will manage locks obtained by the scan
     * @param key The starting key to be searched for.
     * @param location The starting location to be searched for.
     * @param forUpdate If this set {@link LockMode#UPDATE UPDATE} mode locks
     *            will be acquired, else {@link LockMode#SHARED SHARED} mode
     *            locks will be acquired.
     */
    public IndexScan openScan(Transaction trx, IndexKey key, Location location,
            boolean forUpdate);

}
