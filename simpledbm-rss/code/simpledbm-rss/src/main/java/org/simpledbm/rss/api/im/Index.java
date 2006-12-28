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
package org.simpledbm.rss.api.im;

import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.tx.Transaction;

/**
 * Defines the interface for manipulating an Index.
 * 
 * @author Dibyendu Majumdar
 */
public interface Index {

	/**
	 * Inserts a new key and location. If the Index is unique, only one
	 * instance of key is allowed. In non-unique indexes, multiple instances of the
	 * same key may exist, but only one instance of the combination of key/location
	 * is allowed.
	 * <p>
	 * The caller must obtain a Shared lock on the Index Container prior to this call.
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
	 * The caller must obtain a Shared lock on the Index Container prior to this call.
	 * <p>
	 * The caller must acquire an Exclusive lock on Location before this call.
	 * 
	 * @param trx Transaction managing the delete
	 * @param key Key to be deleted
	 * @param location Location associated with the key
	 */
	public void delete(Transaction trx, IndexKey key, Location location);
	
	/**
	 * Opens a new index scan. The Scan will fetch keys >= the specified key and location.
	 * Before returning fetched keys, the associated Location objects will be locked in
	 * specified mode.
	 * <p>
	 * Caller must obtain a Shared lock on the Index Container prior to calling this
	 * method.
	 * 
	 * @param key The key to be searched for.
	 * @param location The location to be searched for.
	 * @param mode The mode in which Location object should be locked prior to returning the key
	 */
	public IndexScan openScan(Transaction trx, IndexKey key, Location location, boolean forUpdate);	
	
}
