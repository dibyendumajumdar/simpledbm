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
package org.simpledbm.rss.api.fsm;

/**
 * Exception class for the Space Manager module.
 */
public class FreeSpaceManagerException extends Exception {

	private static final long serialVersionUID = 5065727917034813269L;

	public FreeSpaceManagerException() {
		super();
	}

	public FreeSpaceManagerException(String message) {
		super(message);
	}

	public FreeSpaceManagerException(String message, Throwable cause) {
		super(message, cause);
	}

	public FreeSpaceManagerException(Throwable cause) {
		super(cause);
	}

    public final static class PageNotFoundException extends FreeSpaceManagerException {

        private static final long serialVersionUID = -7652256777358071992L;

        public PageNotFoundException() {
            super();
        }

        public PageNotFoundException(String message, Throwable cause) {
            super(message, cause);
        }

        public PageNotFoundException(String message) {
            super(message);
        }

        public PageNotFoundException(Throwable cause) {
            super(cause);
        }
    }

    public final static class BufMgrException extends FreeSpaceManagerException {

        private static final long serialVersionUID = -7758126559592658877L;

        public BufMgrException() {
            super();
        }

        public BufMgrException(String message, Throwable cause) {
            super(message, cause);
        }

        public BufMgrException(String message) {
            super(message);
        }

        public BufMgrException(Throwable cause) {
            super(cause);
        }
        
    }

    public final static class TrxException extends FreeSpaceManagerException {

        private static final long serialVersionUID = -4846086162827557848L;

        public TrxException() {
            super();
        }

        public TrxException(String message, Throwable cause) {
            super(message, cause);
        }

        public TrxException(String message) {
            super(message);
        }

        public TrxException(Throwable cause) {
            super(cause);
        }
        
    }

    public final static class StorageException extends FreeSpaceManagerException {

        private static final long serialVersionUID = -303858911478075192L;

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

    public final static class LogException extends FreeSpaceManagerException {

        private static final long serialVersionUID = -5446631468947672300L;

        public LogException() {
            super();
        }

        public LogException(String message, Throwable cause) {
            super(message, cause);
        }

        public LogException(String message) {
            super(message);
        }

        public LogException(Throwable cause) {
            super(cause);
        }
    }
    
    public final static class PageException extends FreeSpaceManagerException {

        private static final long serialVersionUID = -6877029951756113448L;

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
    
    public final static class TestException extends FreeSpaceManagerException {

        public TestException() {
            super();
        }

        public TestException(String message, Throwable cause) {
            super(message, cause);
        }

        public TestException(String message) {
            super(message);
        }

        public TestException(Throwable cause) {
            super(cause);
        }
    }    
}


