package org.simpledbm.database.api;

import java.util.Properties;

import org.simpledbm.database.impl.DatabaseImpl;

/**
 * The DatabaseFactory class is responsible for creating and obtaining instances of 
 * Databases.
 * 
 * @author dibyendu majumdar
 */
public class DatabaseFactory {
	
	/**
	 * Creates a new SimpleDBM database based upon supplied properties.
	 * For details of available properties, please refer to the SimpleDBM 
	 * User Manual.
	 * @param properties Properties for SimpleDBM 
	 */
	public static void create(Properties properties) {
		DatabaseImpl.create(properties);
	}
	
	/**
	 * Obtains a database instance for an existing database.
	 * @param properties Properties for SimpleDBM
	 * @return Database Instance
	 */
	public static Database getDatabase(Properties properties) {
		return new DatabaseImpl(properties);
	}

}
