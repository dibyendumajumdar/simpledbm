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
}
