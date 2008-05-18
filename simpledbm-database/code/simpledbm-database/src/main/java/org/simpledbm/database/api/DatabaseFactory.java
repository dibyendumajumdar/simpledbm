package org.simpledbm.database.api;

import java.util.Properties;

import org.simpledbm.database.impl.DatabaseImpl;

/**
 * The DatabaseFactory class is responsible for creating and obtaining instances of 
 * Databases.
 * 
 * @author dibyendumajumdar
 */
public class DatabaseFactory {
	
	/**
	 * Creates a new SimpleDBM database based upon supplied properties.
	 * @param properties 
	 */
	public static void create(Properties properties) {
		DatabaseImpl.create(properties);
	}
	
	/**
	 * Obtains a database instance for an existing database.
	 * @param properties
	 * @return Database Instance
	 */
	public static Database getDatabase(Properties properties) {
		return new DatabaseImpl(properties);
	}
	

}
