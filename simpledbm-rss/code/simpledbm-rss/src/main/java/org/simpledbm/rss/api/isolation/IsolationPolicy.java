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
package org.simpledbm.rss.api.isolation;

import org.simpledbm.rss.api.loc.Location;
import org.simpledbm.rss.api.locking.LockDuration;
import org.simpledbm.rss.api.locking.LockMode;
import org.simpledbm.rss.api.tx.Transaction;
import org.simpledbm.rss.api.tx.TransactionException;

public interface IsolationPolicy {

	void lockLocation(Transaction trx, Location location, LockMode mode, LockDuration duration) throws TransactionException;
	void lockLocationNoWait(Transaction trx, Location location, LockMode mode, LockDuration duration) throws TransactionException;
	void lockContainer(Transaction trx, int containerId, LockMode mode, LockDuration duration) throws TransactionException;
	void unlockLocationAfterCursorMoved(Transaction trx, Location location) throws TransactionException;
	void unlockLocationAfterRead(Transaction trx, Location location) throws TransactionException;
	LockMode findLocationLock(Transaction trx, Location location);
	LockMode findContainerLock(Transaction trx, int containerId);
	
}
