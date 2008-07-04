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
package org.simpledbm.typesystem.api;

import org.simpledbm.rss.api.im.IndexKey;

/**
 * A Row is an array of DataValue objects, and can be used as multi-value record within
 * a table or an index.
 * 
 * @author Dibyendu Majumdar
 */
public interface Row extends IndexKey {

	/**
	 * Returns the number of fields in the row.
	 */
	int getNumberOfColumns();

	/**
	 * Returns a specific column's value
	 */
	DataValue getColumnValue(int i);

	/**
	 * Sets the specified column's value
	 */
	void setColumnValue(int i, DataValue field);
	
	/**
	 * Creates a copy of this row.
	 */
	Row cloneMe();	
}
