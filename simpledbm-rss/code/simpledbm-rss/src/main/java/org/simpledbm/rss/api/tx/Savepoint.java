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
package org.simpledbm.rss.api.tx;

/**
 * A savepoint marks a point within the transaction to which subsequently
 * a rollback operation can be invoked.
 * 
 * @author Dibyendu Majumdar
 * @since 25-Aug-2005
 */
public interface Savepoint {

    /**
     * Saves a specified keyed value in the savepoint object. 
     */
    void saveValue(Object key, Object value);

    /**
     * Retrieves a previously saved keyed value.
     */
    Object getValue(Object key);

}
