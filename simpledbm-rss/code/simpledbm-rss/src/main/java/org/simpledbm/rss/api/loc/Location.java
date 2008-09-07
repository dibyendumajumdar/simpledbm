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
package org.simpledbm.rss.api.loc;

import org.simpledbm.rss.api.st.Storable;
import org.simpledbm.rss.api.tx.Lockable;

/**
 * Location represents a pointer value that will be stored with
 * an IndexKey in an Index. The pointer is used to locate the
 * Tuple (row) that is associated with the IndexKey.
 * <p>Location objects should be immutable, so that they can be
 * safely passed around.
 * 
 * @author Dibyendu Majumdar
 * @since Oct-2005
 */
public interface Location extends Lockable, Storable, Comparable<Location> {
    /**
     * Return first part of the location if applicable. Meaningless other than 
     * as an aid to identifying the location.
     */
    int getX();
    
    /**
     * Return second part of the location if applicable. Meaningless other than 
     * as an aid to identifying the location.
     */
    int getY();
}
