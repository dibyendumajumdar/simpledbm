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
package org.simpledbm.rss.impl.locking;

import java.util.Properties;

import org.simpledbm.rss.api.locking.LockManager;
import org.simpledbm.rss.api.locking.LockMgrFactory;

/**
 * Default implementation of a LockMgrFactory.
 * 
 * @author Dibyendu Majumdar
 */
public final class LockManagerFactoryImpl implements LockMgrFactory {

	/**
	 * Creates a new LockMgr object.
	 */
	public final LockManager create(Properties props) {
		int deadlockInterval = 15;
		if (props != null) {
			String s = props.getProperty("lock.deadlock.detection.interval",
					"15");
			deadlockInterval = Integer.parseInt(s);
		}
		LockManager lockmgr = new LockManagerImpl();
		lockmgr.setDeadlockDetectorInterval(deadlockInterval);
		return lockmgr;
	}

}
