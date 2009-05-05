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
package org.simpledbm.common.api.thread;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SimpleDBMThread extends Thread {

	Lock threadLock = new ReentrantLock();
	Condition cond = threadLock.newCondition();
	boolean ticket = false;
	boolean waiting = false;
	
	public SimpleDBMThread() {
	}

	public SimpleDBMThread(Runnable arg0) {
		super(arg0);
	}

	public SimpleDBMThread(String arg0) {
		super(arg0);
	}

	public SimpleDBMThread(ThreadGroup arg0, Runnable arg1) {
		super(arg0, arg1);
	}

	public SimpleDBMThread(ThreadGroup arg0, String arg1) {
		super(arg0, arg1);
	}

	public SimpleDBMThread(Runnable arg0, String arg1) {
		super(arg0, arg1);
	}

	public SimpleDBMThread(ThreadGroup arg0, Runnable arg1, String arg2) {
		super(arg0, arg1, arg2);
	}

	public SimpleDBMThread(ThreadGroup arg0, Runnable arg1, String arg2,
			long arg3) {
		super(arg0, arg1, arg2, arg3);
	}

	public void park() {
		threadLock.lock();
		try {
			if (!ticket) {
				waiting = true;
				cond.await();
				waiting = false;
			}
			ticket = false;
		} catch (InterruptedException e) {
		} finally {
			threadLock.unlock();
		}
	}
	
	public void park(long nanos) {
		threadLock.lock();
		try {
			if (!ticket) {
				waiting = true;
				cond.await(nanos, TimeUnit.NANOSECONDS);
				waiting = false;
			}
			ticket = false;
		} catch (InterruptedException e) {
		} finally {
			threadLock.unlock();
		}
	}
	
	public void unpark() {
		threadLock.lock();
		try {
			if (!waiting) {
				ticket = true;
			}
			else {
				cond.signal();
			}
		} finally {
			threadLock.unlock();
		}
	}
}
