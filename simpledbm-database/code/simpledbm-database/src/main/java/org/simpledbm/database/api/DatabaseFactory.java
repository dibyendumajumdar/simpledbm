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
