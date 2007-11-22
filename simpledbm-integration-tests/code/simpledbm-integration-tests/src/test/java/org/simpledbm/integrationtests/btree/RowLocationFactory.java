package org.simpledbm.integrationtests.btree;

import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.loc.LocationFactory;

public class RowLocationFactory implements LocationFactory {
	public Location newLocation() {
		return new RowLocation();
	}
}