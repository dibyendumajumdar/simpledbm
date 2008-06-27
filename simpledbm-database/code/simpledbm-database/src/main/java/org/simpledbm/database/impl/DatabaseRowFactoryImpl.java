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
package org.simpledbm.database.impl;

import org.simpledbm.database.api.TableDefinition;
import org.simpledbm.typesystem.api.FieldFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.impl.GenericRowFactory;

/**
 * The DatabaseRowFactory extends the functionality of GenericRowFunctionality
 * by allowing the rowtype of a container to be retrieved from an external
 * data dictionary.
 * 
 * @author Dibyendu Majumdar
 * @since 29 Dec 2007
 */
public class DatabaseRowFactoryImpl extends GenericRowFactory {

    private final DatabaseImpl database;

    public DatabaseRowFactoryImpl(DatabaseImpl database, FieldFactory fieldFactory) {
        super(fieldFactory);
        this.database = database;
    }

    /* (non-Javadoc)
     * @see org.simpledbm.typesystem.impl.GenericRowFactory#getTypeDescriptor(int)
     */
    @Override
    protected TypeDescriptor[] getTypeDescriptor(int keytype) {
        TypeDescriptor rowType[] = super.getTypeDescriptor(keytype);
        if (rowType == null) {
            TableDefinition table = getDatabase().retrieveTableDefinition(keytype);
            rowType = table.getRowType();
        }
        return rowType;
    }

	/**
	 * @return the database
	 */
	DatabaseImpl getDatabase() {
		return database;
	}
}
