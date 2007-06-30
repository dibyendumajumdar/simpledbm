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
/*
 * Created on: 09-Dec-2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.rss.api.tuple;

import org.simpledbm.rss.api.loc.Location;

/**
 * When a new tuple is inserted into a container, the {@link org.simpledbm.rss.api.tuple.TupleContainer#insert(Transaction, Storable)}
 * method returns a TupleInserter object which can be used to complete the Insert operation.
 * The insert operation is split up for reasons of efficiency; at the time of tuple insert, it is 
 * not known whether the tuple insert can proceed, for example, whether primary key constraints are
 * satisfied. The first part of the insert allocates a Location for the tuple and exclusively
 * locks it. Control is then returned to the caller to verify that the tuple insert can proceed.
 * Once it has been determined that the tuple insert can proceed, the {@link #completeInsert()} method
 * should be invoked. 
 */
public interface TupleInserter {

    /**
     * Returns the Location allocated to the current insert.
     */
    Location getLocation();

    /**
     * Completes the insert, by inserting the tuple data in
     * one or more pages. The insert process is not complete until
     * this method is called. 
     */
    void completeInsert();
}
