package org.simpledbm.database.api;

import org.simpledbm.database.TableDefinitionImpl;
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
	public abstract TableDefinitionImpl newTableDefinition(String name,
			int containerId, TypeDescriptor[] rowType);

	/**
	 * Gets a table definition object from the cache.
	 * @param containerId
	 * @return
	 */
	public abstract TableDefinitionImpl getTableDefinition(int containerId);

	public abstract void start();

	public abstract void shutdown();

	public abstract Server getServer();

	public abstract FieldFactory getFieldFactory();

	public abstract RowFactory getRowFactory();

	public abstract void createTable(TableDefinitionImpl tableDefinition);

}