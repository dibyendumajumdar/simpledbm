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
package org.simpledbm.rss.api.wal;

import java.util.Properties;

import org.simpledbm.rss.api.st.StorageContainerFactory;

/**
 * Defines the factory interface for creating and opening LogMgr objects.
 * 
 * @author Dibyendu Majumdar
 * @since 11 June 2005
 */
public interface LogFactory {

	/**
	 * Creates a new Log based upon parameters supplied.
	 *  
	 * @param props Set of properties for creating the Log
	 * @throws LogException Thrown if there is an error while creating the Log
	 * @throws LogException.StorageException Thrown if there is an IO error
	 */
	void createLog(Properties props) throws LogException;
	void createLog(StorageContainerFactory storageFactory, Properties props) throws LogException;
	
	/**
	 * Obtains an instance of an existing Log based upon parameters supplied.
	 *  
	 * @param props Set of properties for identifying the Log
	 * @throws LogException Thrown if there is an error while opening the Log
	 * @throws LogException.StorageException Thrown if there is an IO error
	 */
	LogManager getLog(Properties props) throws LogException;
	LogManager getLog(StorageContainerFactory storageFactory, Properties props) throws LogException;
	
}
