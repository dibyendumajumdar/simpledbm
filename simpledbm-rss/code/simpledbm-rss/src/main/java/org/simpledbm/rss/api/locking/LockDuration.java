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
package org.simpledbm.rss.api.locking;

/**
 * LockDuration defines how long a lock is to be held. An {@link #INSTANT_DURATION} lock
 * is held only for an instant; its purpose being to delay the caller until the desired lock
 * is grantable. A {@link #MANUAL_DURATION} lock may be released prior to the transaction 
 * completing. Such locks maintain a reference count, so that they are only released
 * when the number of release attempts equals the number of acquire attempts. 
 * A {@link #COMMIT_DURATION} lock is held until the transaction completes and generally 
 * cannot be released early.
 * 
 * @author Dibyendu Majumdar
 * @since 26-July-2005
 */
public enum LockDuration {
	INSTANT_DURATION, MANUAL_DURATION, COMMIT_DURATION
}
