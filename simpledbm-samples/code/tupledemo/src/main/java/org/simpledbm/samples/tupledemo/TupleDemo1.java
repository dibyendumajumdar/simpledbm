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
package org.simpledbm.samples.tupledemo;

/**
 * This demo program illustrates following:
 * <ol>
 * <li>Creating a server.</li>
 * <li>Starting and stopping server instance.</li>
 * <li>Creating a tuple container.</li>
 * <li>Creating two indexes.</li>
 * <li>Inserting tuples and index keys.</li>
 * <li>Listing tuples by index order.</li>
 * </ol>
 * @author Dibyendu Majumdar
 *
 */
public class TupleDemo1 {

	public static void main(String[] args) {

		TupleDemoDb.createServer();
		
		TupleDemoDb db = new TupleDemoDb();
		db.startServer();
		try {
			db.addRow(1, "Rabindranath", "Tagore", "Shanti Niketan");
			db.addRow(2, "John", "Lennon", "New York");
			db.addRow(3, "Albert", "Einstein", "Princeton");
			db.addRow(4, "Mahatma", "Gandhi", "Delhi");
			
			System.out.println("Listing rows ordered by surname, name");
			db.listRowsByKey(TupleDemoDb.SKEY1_CONTNO);

			System.out.println("Listing rows ordered by ID");
			db.listRowsByKey(TupleDemoDb.PKEY_CONTNO);
		} 
		finally {
			db.shutdownServer();
		}
	}

}
