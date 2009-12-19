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
 *    Linking this library statically or dynamically with other modules 
 *    is making a combined work based on this library. Thus, the terms and
 *    conditions of the GNU General Public License cover the whole
 *    combination.
 *    
 *    As a special exception, the copyright holders of this library give 
 *    you permission to link this library with independent modules to 
 *    produce an executable, regardless of the license terms of these 
 *    independent modules, and to copy and distribute the resulting 
 *    executable under terms of your choice, provided that you also meet, 
 *    for each linked independent module, the terms and conditions of the 
 *    license of that module.  An independent module is a module which 
 *    is not derived from or based on this library.  If you modify this 
 *    library, you may extend this exception to your version of the 
 *    library, but you are not obligated to do so.  If you do not wish 
 *    to do so, delete this exception statement from your version.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.database.api;

import org.simpledbm.common.api.platform.PlatformObjects;
import org.simpledbm.common.api.tx.IsolationMode;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.main.Server;
import org.simpledbm.typesystem.api.DictionaryCache;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TableDefinition;
import org.simpledbm.typesystem.api.TypeDescriptor;
import org.simpledbm.typesystem.api.TypeFactory;

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
	
	public final String LOGGER_NAME = "org.simpledbm.database";

	/**
	 * Constructs a new TableDefinition object. A TableDefinition object is used when
	 * creating new tables.
	 * 
	 * <pre>
	 * Database db = ...;
	 * TypeFactory ff = db.getTypeFactory();
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
	 * @see Database#getTypeFactory()
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
	 * Starts a new Transaction
	 */
	public abstract Transaction startTransaction(IsolationMode isolationMode);
	
	/**
	 * Returns the TypeFactory instance associated with this database.
	 * The TypeFactory object can be used to create TypeDescriptors for various types that
	 * can become columns in a row.
	 * <pre>
	 * Database db = ...;
	 * TypeFactory ff = db.getTypeFactory();
	 * TypeDescriptor employee_rowtype[] = { 
	 *   ff.getIntegerType(),
	 *   ff.getVarcharType(20),
	 *   ff.getDateTimeType(), 
	 *   ff.getNumberType(2) 
	 *   };
	 * </pre>
	 */
	public abstract TypeFactory getTypeFactory();

	/**
	 * Returns the RowFactory instance associated with this database.
	 * The RowFactory is used to generate rows.
	 * @return RowFactory instance.
	 */
	public abstract RowFactory getRowFactory();
	
	/**
	 * Returns the dictionary cache used by the database.
	 */
	public abstract DictionaryCache getDictionaryCache();

	/**
	 * Creates a Table and associated indexes using the information in the supplied 
	 * TableDefinition object. Note that the table must have a primary index defined.
	 * The table creation is performed in a stand alone transaction.
	 * @param tableDefinition The TableDefinition object that contains information about the table to be created.
	 */
	public abstract void createTable(TableDefinition tableDefinition);

    /**
     * Drops a Table and all its associated indexes.
     * 
     * @param tableDefinition
     *            The TableDefinition object that contains information about the
     *            table to be dropped.
     */
    public abstract void dropTable(TableDefinition tableDefinition);
	
	/**
	 * Gets the table associated with the specified container ID.
	 * 
	 * @param trx Transaction context
	 * @param containerId Id of the container
	 * @return Table
	 */
	public abstract Table getTable(Transaction trx, int containerId);
	
	public PlatformObjects getPlatformObjects();
}