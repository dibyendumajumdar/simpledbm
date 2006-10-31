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
package org.simpledbm.rss.api.tx;

/**
 * @author Dibyendu Majumdar
 * @since 23-Aug-2005
 */
public class TransactionException extends Exception {

	private static final long serialVersionUID = 6692905740021823587L;

	public TransactionException() {
		super();
	}

	public TransactionException(String arg0) {
		super(arg0);
	}

	public TransactionException(String arg0, Throwable arg1) {
		super(arg0, arg1);
	}

	public TransactionException(Throwable arg0) {
		super(arg0);
	}

    public static final class LogException extends TransactionException {
        private static final long serialVersionUID = -1286075264777910227L;

        public LogException() {
            super();
        }
        public LogException(String arg0, Throwable arg1) {
            super(arg0, arg1);
        }
        public LogException(String arg0) {
            super(arg0);
        }
        public LogException(Throwable arg0) {
            super(arg0);
        }
    }

    public static final class LockException extends TransactionException {
        private static final long serialVersionUID = -7116066209929052129L;
        public LockException() {
            super();
        }
        public LockException(String arg0, Throwable arg1) {
            super(arg0, arg1);
        }
        public LockException(String arg0) {
            super(arg0);
        }
        public LockException(Throwable arg0) {
            super(arg0);
        }
    }
    
    public final static class BufMgrException extends TransactionException {
        private static final long serialVersionUID = 4552461420559062089L;
        public BufMgrException() {
            super();
        }
        public BufMgrException(String arg0, Throwable arg1) {
            super(arg0, arg1);
        }
        public BufMgrException(String arg0) {
            super(arg0);
        }
        public BufMgrException(Throwable arg0) {
            super(arg0);
        }
    }

    public final static class StorageException extends TransactionException {
        private static final long serialVersionUID = -5614164282576931281L;
        public StorageException() {
            super();
        }
        public StorageException(String arg0, Throwable arg1) {
            super(arg0, arg1);
        }
        public StorageException(String arg0) {
            super(arg0);
        }
        public StorageException(Throwable arg0) {
            super(arg0);
        }
    }

    public final static class PageException extends TransactionException {
        private static final long serialVersionUID = -2841347920138974854L;
        public PageException() {
            super();
        }
        public PageException(String arg0, Throwable arg1) {
            super(arg0, arg1);
        }
        public PageException(String arg0) {
            super(arg0);
        }
        public PageException(Throwable arg0) {
            super(arg0);
        }
    }
}
