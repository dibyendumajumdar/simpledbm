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
package org.simpledbm.rss.api.exception;

import org.simpledbm.rss.util.logging.Logger;

public class ExceptionHandler {

	final Logger log;
	
	ExceptionHandler(Logger log) {
		this.log = log;
	}
	
	/**
	 * Logs an error message and throws the supplied exception. The exception will be logged
	 * with the error message.
	 * @param sourceClass The name of the class where the exception was raised
	 * @param sourceMethod The method where the exception was raised
	 * @param e The exception to throw
	 */
	public void errorThrow(String sourceClass, String sourceMethod, RSSException e) {
		log.error(sourceClass, sourceMethod, e.getMessage(), e);
		throw e;
	}
	
	/**
	 * Logs a warning message and throws the supplied exception. The exception will be logged
	 * with the warning message.
	 * @param sourceClass The name of the class where the exception was raised
	 * @param sourceMethod The method where the exception was raised
	 * @param e The exception to throw
	 */
	public void warnAndThrow(String sourceClass, String sourceMethod, RSSException e) {
		log.warn(sourceClass, sourceMethod, e.getMessage(), e);
		throw e;
	}
	
	public static ExceptionHandler getExceptionHandler(Logger log) {
		return new ExceptionHandler(log);
	}
	
}
