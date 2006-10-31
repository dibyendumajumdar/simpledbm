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
/*
 * Created on: 08-Dec-2005
 * Author: Dibyendu Majumdar
 */
package org.simpledbm.rss.api.tuple;

/**
 * Exceptions thrown by the Tuple Manager module are all sub-types of 
 * TupleException.
 */
public class TupleException extends Exception {

    public TupleException() {
        super();
    }
    public TupleException(String arg0) {
        super(arg0);
    }
    public TupleException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }
    public TupleException(Throwable arg0) {
        super(arg0);
    }
    
    public static class SpaceMgrException extends TupleException {
        public SpaceMgrException() {
            super();
        }
        public SpaceMgrException(String arg0) {
            super(arg0);
        }
        public SpaceMgrException(String arg0, Throwable arg1) {
            super(arg0, arg1);
        }
        public SpaceMgrException(Throwable arg0) {
            super(arg0);
        }
    }

    public static class BufMgrException extends TupleException {
        public BufMgrException() {
            super();
        }
        public BufMgrException(String arg0) {
            super(arg0);
        }
        public BufMgrException(String arg0, Throwable arg1) {
            super(arg0, arg1);
        }
        public BufMgrException(Throwable arg0) {
            super(arg0);
        }
    }

    public static class TrxException extends TupleException {
        public TrxException() {
            super();
        }
        public TrxException(String arg0) {
            super(arg0);
        }
        public TrxException(String arg0, Throwable arg1) {
            super(arg0, arg1);
        }
        public TrxException(Throwable arg0) {
            super(arg0);
        }
    }

    public static class LogException extends TupleException {
        public LogException() {
            super();
        }
        public LogException(String arg0) {
            super(arg0);
        }
        public LogException(String arg0, Throwable arg1) {
            super(arg0, arg1);
        }
        public LogException(Throwable arg0) {
            super(arg0);
        }
    }
    
    public static class StorageException extends TupleException {
        public StorageException() {
            super();
        }
        public StorageException(String arg0) {
            super(arg0);
        }
        public StorageException(String arg0, Throwable arg1) {
            super(arg0, arg1);
        }
        public StorageException(Throwable arg0) {
            super(arg0);
        }
    }
    
    public static class LockException extends TupleException {
        public LockException() {
            super();
        }
        public LockException(String arg0) {
            super(arg0);
        }
        public LockException(String arg0, Throwable arg1) {
            super(arg0, arg1);
        }
        public LockException(Throwable arg0) {
            super(arg0);
        }
    }
    
}
