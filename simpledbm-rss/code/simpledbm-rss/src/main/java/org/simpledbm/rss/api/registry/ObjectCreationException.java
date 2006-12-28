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
package org.simpledbm.rss.api.registry;

import org.simpledbm.rss.api.exception.RSSException;

/**
 * Thrown when the Object Registry is unable to create an
 * object of specified type. This is a RuntimeException because
 * this error would indicate a bug in the system.
 * 
 * @author Dibyendu Majumdar
 * @since 07-Aug-05
 */
public class ObjectCreationException extends RSSException {

	private static final long serialVersionUID = 1L;

	ObjectCreationException() {
		super();
	}

	public ObjectCreationException(String message) {
		super(message);
	}

	public ObjectCreationException(String message, Throwable cause) {
		super(message, cause);
	}

	public ObjectCreationException(Throwable cause) {
		super(cause);
	}

	public static final class UnknownTypeException extends ObjectCreationException {

		private static final long serialVersionUID = 1L;

		public UnknownTypeException() {
			super();
		}

		public UnknownTypeException(String message) {
			super(message);
		}

		public UnknownTypeException(String message, Throwable cause) {
			super(message, cause);
		}

		public UnknownTypeException(Throwable cause) {
			super(cause);
		}
	}	
}
