package org.simpledbm.database.api;

import java.util.Properties;

import org.simpledbm.database.impl.DatabaseImpl;

public class DatabaseFactory {
	
	public static void create(Properties properties) {
		DatabaseImpl.create(properties);
	}
	
	public static Database getDatabase(Properties properties) {
		return new DatabaseImpl(properties);
	}
	

}
