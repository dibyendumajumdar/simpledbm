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

/**
 * SimpleTimer enables bounded and unbounded waits. It uses the
 * {@link LockSupport#park()} and {@link LockSupport#parkNanos(long)}
 * methods for the waits. Both of these methods can return early, either
 * due to an outstanding unpark() signal, or spuriously. SimpleTimer 
 * allows the client to check if the timer has expired. If the timer
 * has not expired, the client can invoke await() again, and wait
 * for any remaining time.
 * <p>
 * Typical usage is as follows:
 * <p>
 * <pre>
 * SimpleTimer timer = new SimpleTimer(
 *   TimeUnit.NANOSECONDS.convert(
 *     10, TimeUnit.SECONDS)); // 10 seconds timer
 * while (some condition) {
 *   if (!timer.isExpired()) {
 *     timer.await();
 *   }
 *   else {
 *     break;
 *   }
 * }
 * </pre>
 * @author Dibyendu Majumdar
 */
public class SimpleTimer {
	
	/**
	 * Time to wait in nanoseconds, -1 means wait unconditionally.
	 */
	final long timeToWait;
	
	/**
	 * Has this timer expired. Timer expires when time waited &gt;
	 * timeToWait.
	 */
	boolean expired = false;
	
	/**
	 * When the wait started; this is set when the timer object is created.
	 */
	final long startTime;
	
	public SimpleTimer() {
		timeToWait = -1;
		startTime = System.nanoTime();
	}
	
	public SimpleTimer(long nanos) {
		startTime = System.nanoTime();
		this.timeToWait = nanos < 0 ? -1 : nanos;
	}

	/**
	 * Determines if the timer has expired. This is true if
	 * the time waited exceeds time to wait.
	 */
	public boolean isExpired() {
		if (expired)
			return true;
		long now = System.nanoTime();
		long timeWaited = now - startTime;
		if (timeWaited >= timeToWait) {
			expired = true;
		}
		return expired;
	}
	
	/**
	 * If !isExpired(), wait for time remaining, or until the thread is
	 * unparked. Time remaining is calculated as follows:
	 * <p>
	 * <ol>
	 * <li>If timeToWait is -1, wait unconditionally.</li>
	 * <li>Otherwise, timeToWait is calculated as the amount of time
	 * originally requested minus the time already waited.</li>
	 * </ol>
	 */
	public void await() {
		if (expired) {
			return;
		}
		if (timeToWait == -1) {
			LockSupport.park();
			expired = true;
			return;
		}
		long now = System.nanoTime();
		long timeWaited = now - startTime;
		if (timeWaited >= timeToWait) {
			expired = true;
			return;
		}
		long timeToWaitThisTime = timeToWait - timeWaited;
		assert timeToWaitThisTime > 0 && timeToWaitThisTime <= timeToWait;
		LockSupport.parkNanos(timeToWaitThisTime);
	}
}
