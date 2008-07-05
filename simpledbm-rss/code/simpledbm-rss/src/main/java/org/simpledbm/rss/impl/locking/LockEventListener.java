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
package org.simpledbm.rss.impl.locking;

import org.simpledbm.rss.api.locking.LockMode;

/**
 * LockEventListener allows interested clients to be notified for certain lock events.
 * @author Dibyendu Majumdar
 * @since 05 Nov 2006
 */
public interface LockEventListener {

    /** 
     * This event is generated prior to a lock request entering the wait state.
     * @param owner Prospective owner of the lock, the requester
     * @param lockable The object that is to be locked
     * @param mode The {@link LockMode} 
     */
    void beforeLockWait(Object owner, Object lockable, LockMode mode);

    /**
     * This event occurs when a lock request is granted.
     * @param owner Prospective owner of the lock, the requester
     * @param lockable The object that is to be locked
     * @param mode The {@link LockMode} 
     */
    // void lockGranted(Object owner, Object lockable, LockMode mode);
}
