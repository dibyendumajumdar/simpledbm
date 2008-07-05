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
package org.simpledbm.rss.api.locking;

/**
 * LockDeadLockException is thrown when the system detects that there is a 
 * Deadlock. Note that the implementation may simply use lock timeouts to detect
 * potential deadlock situations.
 * 
 * @author Dibyendu Majumdar
 * @since 27-Aug-2005
 */
public final class LockDeadlockException extends LockException {

    private static final long serialVersionUID = 1L;

    public LockDeadlockException() {
        super();
    }

    public LockDeadlockException(String arg0) {
        super(arg0);
    }

    public LockDeadlockException(String arg0, Throwable arg1) {
        super(arg0, arg1);
    }

    public LockDeadlockException(Throwable arg0) {
        super(arg0);
    }

}
