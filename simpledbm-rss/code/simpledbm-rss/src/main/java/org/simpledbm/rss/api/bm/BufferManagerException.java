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
package org.simpledbm.rss.api.bm;

/**
 * Exceptions thrown by the Buffer Manager module are all
 * derived from this class.
 * 
 * @author Dibyendu Majumdar
 * @since 20-Aug-2005
 */
public class BufferManagerException extends Exception {

	private static final long serialVersionUID = 7479796029913331256L;

	public BufferManagerException() {
		super();
	}
	public BufferManagerException(String message) {
		super(message);
	}
	public BufferManagerException(String message, Throwable cause) {
		super(message, cause);
	}
	public BufferManagerException(Throwable cause) {
		super(cause);
	}

    public static final class StorageException extends BufferManagerException {

        private static final long serialVersionUID = 6508962282665664480L;

        public StorageException() {
            super();
        }
        public StorageException(String message, Throwable cause) {
            super(message, cause);
        }
        public StorageException(String message) {
            super(message);
        }
        public StorageException(Throwable cause) {
            super(cause);
        }
    }

    public static final class PageException extends BufferManagerException {

        private static final long serialVersionUID = -4058556154710644294L;

        public PageException() {
            super();
        }
        public PageException(String message, Throwable cause) {
            super(message, cause);
        }
        public PageException(String message) {
            super(message);
        }
        public PageException(Throwable cause) {
            super(cause);
        }
    }
}
