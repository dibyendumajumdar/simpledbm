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
package org.simpledbm.integrationtests.btree;

import java.nio.ByteBuffer;

import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.util.TypeSize;

public class RowLocation implements Location {

	int loc;

	public void setInt(int i) {
		this.loc = i;
	}
	
	public void parseString(String string) {
		loc = Integer.parseInt(string);
	}

	public void retrieve(ByteBuffer bb) {
		loc = bb.getInt();
	}

	public void store(ByteBuffer bb) {
		bb.putInt(loc);
	}

	public int getStoredLength() {
		return TypeSize.INTEGER;
	}

	public int compareTo(Location o) {
		if (o == this) {
			return 0;
		}
		if (o == null) {
			throw new IllegalArgumentException("Null argument");
		}
		if (!(o instanceof RowLocation)) {
			return -1;
		}
		RowLocation rl = (RowLocation) o;
		return loc - rl.loc;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this) {
			return true;
		}
		if (o == null) {
			throw new IllegalArgumentException("Null argument");
		}
		if (!(o instanceof Location)) {
			return false;
		}
		return compareTo((Location) o) == 0;
	}

	@Override
	public int hashCode() {
		return loc;
	}

	@Override
	public String toString() {
		return "RowLocation(" + loc + ")";
	}

	/**
	 * Unused at present
	 */
	public int getContainerId() {
		return 1;
	}

	public int getX() {
		return loc;
	}

	public int getY() {
		return 0;
	}
}
