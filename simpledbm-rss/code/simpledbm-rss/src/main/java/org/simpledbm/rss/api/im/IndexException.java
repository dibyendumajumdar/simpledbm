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
package org.simpledbm.rss.api.im;

/**
 * @author Dibyendu Majumdar
 * @since 12-Oct-2005
 */
public class IndexException extends Exception {

	private static final long serialVersionUID = 2680497332718941694L;

	public IndexException() {
		super();
	}
	public IndexException(String message) {
		super(message);
	}
	public IndexException(String message, Throwable cause) {
		super(message, cause);
	}
	public IndexException(Throwable cause) {
		super(cause);
	}

    public final static class BufMgrException extends IndexException {

        private static final long serialVersionUID = 7860589926090015679L;

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

    public final static class TrxException extends IndexException {

        private static final long serialVersionUID = -4639331920907600587L;

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

    public final static class StorageException extends IndexException {

        private static final long serialVersionUID = -6813657266265374422L;

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

    public final static class LogException extends IndexException {

        private static final long serialVersionUID = 2649101827453057300L;

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
    
    public final static class PageException extends IndexException {

        private static final long serialVersionUID = 7471909395433987770L;

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

    public final static class SpaceMgrException extends IndexException {

        private static final long serialVersionUID = 7471909395433987770L;

        public SpaceMgrException() {
            super();
        }
        public SpaceMgrException(String message, Throwable cause) {
            super(message, cause);
        }
        public SpaceMgrException(String message) {
            super(message);
        }
        public SpaceMgrException(Throwable cause) {
            super(cause);
        }
    }

    public static final class LockException extends IndexException {

		public LockException() {
			super();
		}
		public LockException(String message, Throwable cause) {
			super(message, cause);
		}
		public LockException(String message) {
			super(message);
		}
		public LockException(Throwable cause) {
			super(cause);
		}
    }
    
}
