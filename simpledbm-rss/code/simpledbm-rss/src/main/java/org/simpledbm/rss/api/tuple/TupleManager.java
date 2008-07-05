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
package org.simpledbm.rss.api.tuple;

import org.simpledbm.rss.api.loc.LocationFactory;
import org.simpledbm.rss.api.locking.LockMode;
import org.simpledbm.rss.api.tx.Transaction;

/**
 * TupleManager sub-system provides an abstraction for managing
 * data within containers. A TupleContainer is specialized for storing tuples,
 * which correspond to blobs of data. Tuples can be of arbitrary length; although
 * the implementation may impose restrictions on maximum tuple size. Tuples can
 * span multiple pages, however, this is handled by the TupleManager sub-system
 * transparently to the caller.
 * <p>
 * Each tuple is uniquely identified by a Location. When a tuple is first inserted,
 * its Location is defined. The Tuple remains at the same Location for the 
 * rest of its life.
 *   
 * @author Dibyendu Majumdar
 * @since 07-Dec-2005
 */
public interface TupleManager {

	
	/**
	 * Locks a tuple container in specified mode for COMMIT duration.
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
     * @param containerId A numeric ID for the container - must be unique for each container
     * @param extentSize The number of pages that should be part of each extent in the container
     */
    void createTupleContainer(Transaction trx, String name, int containerId,
            int extentSize);

    /**
     * Gets an instance of TupleContainer. Specified container must already exist.
     * Caller must obtain SHARED lock on containerId prior to invoking this
     * method.
     * @param containerId ID of the container
     */
    //TupleContainer getTupleContainer(int containerId);
    /**
     * Gets an instance of TupleContainer. Specified container must already exist.
     * Obtains SHARED lock on specified containerId.
     * @param containerId ID of the container
     */
    TupleContainer getTupleContainer(Transaction trx, int containerId);

    /**
     * Returns the type ID of the location factory used by the tuple manager.
     * An instance of the {@link org.simpledbm.rss.api.loc.LocationFactory LocationFactory} may be
     * obtained from the {@link org.simpledbm.rss.api.registry.ObjectRegistry ObjectRegistry}.
     * <p>
     * <pre>
     * TupleManager tupleManager = ...;
     * ObjectRegistry objectRegistry = ...;
     * LocationFactory locationFactory = (LocationFactory) objectRegistry.getInstance(tupleManager.getLocationFactoryType());
     * </pre>
     */
    int getLocationFactoryType();

    /**
     * Returns the location factory used by the tuple container.
     * @return LocationFactory instance
     */
    LocationFactory getLocationFactory();
}
