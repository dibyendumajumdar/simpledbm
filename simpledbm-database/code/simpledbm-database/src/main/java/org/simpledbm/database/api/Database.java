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
package org.simpledbm.database.api;

import org.simpledbm.rss.main.Server;
import org.simpledbm.typesystem.api.FieldFactory;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;

/**
 * A SimpleDBM Database is a collection of Tables. The Database runs as an embedded server, and 
 * provides an API for creating and maintaining tables.
 * <p>
 * A Database is created using {@link DatabaseFactory#create(java.util.Properties)}. An
 * existing Database can be instantiated using {@link DatabaseFactory#getDatabase(java.util.Properties)}.
 * 
 * @author dibyendu majumdar
 */
public interface Database {

	/**
	 * Constructs a new TableDefinition object. A TableDefinition object is used when
	 * creating new tables.
	 * 
	 * <pre>
	 * Database db = ...;
	 * FieldFactory ff = db.getFieldFactory();
	 * TypeDescriptor employee_rowtype[] = { 
	 *   ff.getIntegerType(),
	 *   ff.getVarcharType(20),
	 *   ff.getDateTimeType(), 
	 *   ff.getNumberType(2) 
	 *   };
	 * TableDefinition tableDefinition = 
	 *   db.newTableDefinition("employee", 1,
	 *      employee_rowtype); 
	 * </pre>
	 * 
	 * @param name Name of the table
	 * @param containerId ID of the container that will hold the table data
	 * @param rowType A row type definition. 
	 * @return A TableDefinition object.
	 * @see Database#getFieldFactory()
	 */
	public abstract TableDefinition newTableDefinition(String name,
			int containerId, TypeDescriptor[] rowType);

	/**
	 * Gets the table definition associated with the specified container ID.
	 * 
	 * @param containerId Id of the container
	 * @return TableDefinition
	 */
	public abstract TableDefinition getTableDefinition(int containerId);

	/**
	 * Starts the database instance.
	 */
	public abstract void start();

	/**
	 * Shuts down the database instance.
	 */
	public abstract void shutdown();

	/**
	 * Gets the SimpleDBM RSS Server object that is managing this database.
	 * @return SimpleDBM RSS Server object.
	 */
	public abstract Server getServer();

	/**
	 * Returns the FieldFactory instance associated with this database.
	 * The FieldFactory object can be used to create TypeDescriptors for various types that
	 * can become columns in a row.
	 * <pre>
	 * Database db = ...;
	 * FieldFactory ff = db.getFieldFactory();
	 * TypeDescriptor employee_rowtype[] = { 
	 *   ff.getIntegerType(),
	 *   ff.getVarcharType(20),
	 *   ff.getDateTimeType(), 
	 *   ff.getNumberType(2) 
	 *   };
	 * </pre>
	 * @return FieldFactory object
	 */
	public abstract FieldFactory getFieldFactory();

	/**
	 * Returns the RowFactory instance associated with this database.
	 * The RowFactory is used to generate rows.
	 * @return RowFactory instance.
	 */
	public abstract RowFactory getRowFactory();

	/**
	 * Creates a Table and associated indexes using the information in the supplied 
	 * TableDefinition object. Note that the table must have a primary index defined.
	 * The table creation is performed in a standalone transaction.
	 * @param tableDefinition The TableDefinition object that contains information about the table to be created.
	 */
	public abstract void createTable(TableDefinition tableDefinition);
	
	/**
	 * Obtains an instance of the Table associated with the supplied
	 * TableDefinition.
	 * 
	 * @param tableDefinition
	 * @return Table object representing the table
	 */
	public abstract Table getTable(TableDefinition tableDefinition);
}