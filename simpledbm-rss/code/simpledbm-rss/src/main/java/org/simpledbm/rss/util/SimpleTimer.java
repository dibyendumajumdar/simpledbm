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
package org.simpledbm.rss.util;

import java.util.concurrent.locks.LockSupport;

public class SimpleTimer {
	
	final long nanos;
	boolean expired = false;
	final long startTime;
	
	public SimpleTimer() {
		nanos = -1;
		startTime = System.nanoTime();
	}
	
	public SimpleTimer(long nanos) {
		startTime = System.nanoTime();
		this.nanos = nanos;
	}

	public boolean isExpired() {
		if (expired)
			return true;
		long now = System.nanoTime();
		long timeWaited = now - startTime;
		if (timeWaited >= nanos) {
			expired = true;
		}
		return expired;
	}
	
	public void await() {
		if (expired) {
			return;
		}
		if (nanos == -1) {
			LockSupport.park();
			expired = true;
			return;
		}
		long now = System.nanoTime();
		long timeWaited = now - startTime;
		if (timeWaited >= nanos) {
			expired = true;
			return;
		}
		long toWait = nanos - timeWaited;
		assert toWait > 0 && toWait <= nanos;
		LockSupport.parkNanos(toWait);
	}
}
