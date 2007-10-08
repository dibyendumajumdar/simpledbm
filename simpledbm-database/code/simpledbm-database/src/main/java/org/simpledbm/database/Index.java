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
package org.simpledbm.database;

import org.simpledbm.rss.api.im.IndexKey;
import org.simpledbm.rss.api.im.IndexKeyFactory;
import org.simpledbm.typesystem.api.Row;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;

public class Index {

	Table table;

	int containerId;

	String name;

	int columns[];

	TypeDescriptor[] rowType;

	boolean primary;

	boolean unique;

	public Index(Table table, int containerId, String name,
			int columns[], boolean primary, boolean unique) {
		this.table = table;
		this.containerId = containerId;
		this.name = name;
		this.columns = columns;
		this.primary = primary;
		if (primary) {
			this.unique = true;
		} else {
			this.unique = unique;
		}

		rowType = new TypeDescriptor[columns.length];
		for (int i = 0; i < columns.length; i++) {
			rowType[i] = table.getRowType()[columns[i]];
		}

		table.database.getRowFactory().registerRowType(containerId, rowType);
		table.indexes.add(this);
	}

	public Table getTable() {
		return table;
	}

	public int getContainerId() {
		return containerId;
	}

	public String getName() {
		return name;
	}

	public int[] getColumns() {
		return columns;
	}

	public TypeDescriptor[] getRowType() {
		return rowType;
	}

	public boolean isPrimary() {
		return primary;
	}

	public boolean isUnique() {
		return unique;
	}

	public Row getRow() {
		RowFactory rowFactory = table.database.getRowFactory();
		return rowFactory.newRow(containerId);
	}
	
    /**
     * Create a row with values that are less than any other row in the index.
     * 
     * @param containerId
     *            ID of the container
     * @return Appropriate row type
     */
    IndexKey makeMinRow(int containerId) {
        IndexKeyFactory rowFactory = table.database.getRowFactory();
        return rowFactory.minIndexKey(containerId);
    }
}
