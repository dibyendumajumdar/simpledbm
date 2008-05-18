package org.simpledbm.database.api;

import org.simpledbm.rss.main.Server;
import org.simpledbm.typesystem.api.FieldFactory;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;

/**
 * A SimpleDBM Database is a collection of Tables. The Database runs as an embedded server, and 
 * provides an API for creating and maintaining tables.
 * 
 * @author dibyendu majumdar
 */
public interface Database {

	/**
	 * Constructs a new TableDefinition object.
	 * @param name Name of the table
	 * @param containerId ID of the container that will hold the table data
	 * @param rowType A row type definition
	 * @return A TableDefinition object.
	 */
	public abstract TableDefinition newTableDefinition(String name,
			int containerId, TypeDescriptor[] rowType);

	/**
	 * Gets a table definition associated with the specified container ID.
	 * 
	 * @param containerId Id of the container
	 * @return TableDefinition
	 */
	public abstract TableDefinition getTableDefinition(int containerId);

	/**
	 * Starts the database.
	 */
	public abstract void start();

	/**
	 * Shuts down the database.
	 */
	public abstract void shutdown();

	/**
	 * Gets the SimpleDBM RSS Server object that is managing this database.
	 * @return SimpleDBM RSS Server object.
	 */
	public abstract Server getServer();

	/**
	 * Returns the FieldFactory instance associated with this database.
	 * The FieldFactory object can be used to create objects of various types that
	 * can become columns in a row.
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
	 * Creates a Table using the information in the supplied TableDefinition object.
	 * The table creation is performed in a standalone transaction.
	 * @param tableDefinition The TableDefinition object that contains information about the table to be created.
	 */
	public abstract void createTable(TableDefinition tableDefinition);
	
	/**
	 * Obtains an instance of the Table associated with the supplied
	 * TableDefinition.
	 * 
	 * @param tableDefinition
	 * @return
	 */
	public abstract Table getTable(TableDefinition tableDefinition);

}