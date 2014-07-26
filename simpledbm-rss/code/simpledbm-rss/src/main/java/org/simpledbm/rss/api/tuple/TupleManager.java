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

import org.simpledbm.common.api.locking.LockMode;
import org.simpledbm.rss.api.loc.LocationFactory;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.api.tx.TransactionalModule;

/**
 * TupleManager sub-system provides an abstraction for managing data within
 * containers. A TupleContainer is specialized for storing tuples, which
 * correspond to blobs of data. Tuples can be of arbitrary length; although the
 * implementation may impose restrictions on maximum tuple size. Tuples can span
 * multiple pages, however, this is handled by the TupleManager sub-system
 * transparently to the caller.
 * <p>
 * Each tuple is uniquely identified by a Location. When a tuple is first
 * inserted, its Location is defined. The Tuple remains at the same Location for
 * the rest of its life.
 * 
 * @author Dibyendu Majumdar
 * @since 07-Dec-2005
 */
public interface TupleManager extends TransactionalModule {

    public final String LOGGER_NAME = "org.simpledbm.tuplemgr";

    /**
     * Locks a tuple container in specified mode for COMMIT duration.
     * 
     * @param trx Transaction acquiring the lock
     * @param containerId ID of the tuple container
     * @param mode The Lock mode
     */
    void lockTupleContainer(Transaction trx, int containerId, LockMode mode);

    /**
     * Creates a new Tuple Container.
     * 
     * @param trx Transaction to be used for creating the container
     * @param name Name of the container
     * @param containerId A numeric ID for the container - must be unique for
     *            each container
     * @param extentSize The number of pages that should be part of each extent
     *            in the container
     */
    void createTupleContainer(Transaction trx, String name, int containerId,
            int extentSize);

    /**
     * Gets an instance of TupleContainer. Specified container must already
     * exist. Caller must obtain SHARED lock on containerId prior to invoking
     * this method.
     * 
     * @param containerId ID of the container
     */
    //TupleContainer getTupleContainer(int containerId);
    /**
     * Gets an instance of TupleContainer. Specified container must already
     * exist. Obtains SHARED lock on specified containerId.
     * 
     * @param containerId ID of the container
     */
    TupleContainer getTupleContainer(Transaction trx, int containerId);

    /**
     * Returns the type ID of the location factory used by the tuple manager. An
     * instance of the {@link org.simpledbm.rss.api.loc.LocationFactory
     * LocationFactory} may be obtained from the
     * {@link org.simpledbm.rss.api.registry.ObjectRegistry ObjectRegistry}.
     * <p>
     * 
     * <pre>
     * TupleManager tupleManager = ...;
     * ObjectRegistry objectRegistry = ...;
     * LocationFactory locationFactory = (LocationFactory) objectRegistry.getInstance(tupleManager.getLocationFactoryType());
     * </pre>
     */
    int getLocationFactoryType();

    /**
     * Returns the location factory used by the tuple container.
     * 
     * @return LocationFactory instance
     */
    LocationFactory getLocationFactory();
}
