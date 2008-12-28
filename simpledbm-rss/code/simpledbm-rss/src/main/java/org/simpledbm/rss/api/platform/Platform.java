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
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.rss.api.platform;

import org.simpledbm.rss.api.exception.ExceptionHandler;
import org.simpledbm.rss.util.logging.Logger;
import org.simpledbm.rss.util.mcat.MessageCatalog;

/**
 * The Platform is an abstraction of the runtime environment.
 * It provides access to basic facilities such as logging, event publication,
 * exception management.
 * 
 * @author dibyendumajumdar
 */
public interface Platform {
	
	/**
	 * Retrieves a logger by name.
	 */
	Logger getLogger(String loggerName);
	
	/**
	 * Retrieves the message catalog.
	 */
	MessageCatalog getMessageCatalog();

	/**
	 * Returns an ExceptionHandler that is har wired to the logger
	 * object.
	 */
	ExceptionHandler getExceptionHandler(Logger log);

}
