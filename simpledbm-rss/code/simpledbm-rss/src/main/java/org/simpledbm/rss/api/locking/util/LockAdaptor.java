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
package org.simpledbm.rss.api.locking.util;

import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.tx.Lockable;

/**
 * A LockAdaptor encapsulates knowledge about different types of lockable
 * objects. It is responsible for ensuring that different lockable types have
 * different namespaces, for example, locations and containers. 
 * 
 * @author Dibyendu Majumdar
 * @since 14 Dec 2006
 */
public interface LockAdaptor {

    /**
     * Creates a lockable object for the specified container id.
     * All container ids should belong to a distinct namespace.
     */
    Lockable getLockableContainerId(int containerId);

    /**
     * Creates a lockable object for the container id associated with the
     * specified location. Note that this method may exhibit implementation
     * specific behaviour. 
     * @param location Location object to be mapped to a container id
     * @throws IllegalArgumentException Thrown if the location argument cannot be converted to
     * 	a container id
     */
    Lockable getLockableContainerId(Location location);

}
