package org.simpledbm.database.api;

import org.simpledbm.rss.main.Server;
import org.simpledbm.typesystem.api.FieldFactory;
import org.simpledbm.typesystem.api.RowFactory;
import org.simpledbm.typesystem.api.TypeDescriptor;

public interface Database {

	/**
	 * Allocates a new TableDefinition object.
	 * @param name
	 * @param containerId
	 * @param rowType
	 * @return
	 */
	public abstract TableDefinition newTableDefinition(String name,
			int containerId, TypeDescriptor[] rowType);

	/**
	 * Gets a table definition object from the cache.
	 * @param containerId
	 * @return
	 */
	public abstract TableDefinition getTableDefinition(int containerId);

	public abstract void start();

	public abstract void shutdown();

	public abstract Server getServer();

	public abstract FieldFactory getFieldFactory();

	public abstract RowFactory getRowFactory();

	public abstract void createTable(TableDefinition tableDefinition);
	
	public abstract Table getTable(TableDefinition tableDefinition);

}