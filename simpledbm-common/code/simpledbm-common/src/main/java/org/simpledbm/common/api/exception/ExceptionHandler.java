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
package org.simpledbm.common.api.exception;

import org.simpledbm.common.tools.diagnostics.Trace;
import org.simpledbm.common.util.logging.Logger;

/**
 * The ExceptionHandler mechanism allows the raising of exception to be handled
 * by a common piece of code. The primary advantage is that information can be dumped to
 * the logs automatically when an exception is raised.
 * 
 * @author dibyendu majumdar
 */
public class ExceptionHandler {

	/**
	 * A logger is always hard wired to an instance of an ExceptionHandler
	 */
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
	public void errorThrow(String sourceClass, String sourceMethod, SimpleDBMException e) {
		log.error(sourceClass, sourceMethod, e.getMessage(), e);
		throw e;
	}

	/**
	 * Logs an error message and throws the supplied exception. The exception will be logged
	 * with the error message. Also dumps {@link Trace} messages.
	 * @param sourceClass The name of the class where the exception was raised
	 * @param sourceMethod The method where the exception was raised
	 * @param e The exception to throw
	 */
	public void unexpectedErrorThrow(String sourceClass, String sourceMethod, SimpleDBMException e) {
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
	public void warnAndThrow(String sourceClass, String sourceMethod, SimpleDBMException e) {
		log.warn(sourceClass, sourceMethod, e.getMessage(), e);
		throw e;
	}
	
	/**
	 * Throws the supplied exception. The exception will normally not be logged.
	 * 
	 * @param sourceClass The name of the class where the exception was raised
	 * @param sourceMethod The method where the exception was raised
	 * @param e The exception to throw
	 */
	public void throwSilently(String sourceClass, String sourceMethod, SimpleDBMException e) {
		throw e;
	}
	
	/**
	 * Returns an ExceptionHandler object that is tied to the logger.
	 */
	public static ExceptionHandler getExceptionHandler(Logger log) {
		return new ExceptionHandler(log);
	}
	
}
