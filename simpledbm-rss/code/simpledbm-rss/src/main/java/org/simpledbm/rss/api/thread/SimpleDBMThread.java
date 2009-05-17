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
 *    Linking this library statically or dynamically with other modules 
 *    is making a combined work based on this library. Thus, the terms and
 *    conditions of the GNU General Public License cover the whole
 *    combination.
 *    
 *    As a special exception, the copyright holders of this library give 
 *    you permission to link this library with independent modules to 
 *    produce an executable, regardless of the license terms of these 
 *    independent modules, and to copy and distribute the resulting 
 *    executable under terms of your choice, provided that you also meet, 
 *    for each linked independent module, the terms and conditions of the 
 *    license of that module.  An independent module is a module which 
 *    is not derived from or based on this library.  If you modify this 
 *    library, you may extend this exception to your version of the 
 *    library, but you are not obligated to do so.  If you do not wish 
 *    to do so, delete this exception statement from your version.
 *
 *    Project: www.simpledbm.org
 *    Author : Dibyendu Majumdar
 *    Email  : d dot majumdar at gmail dot com ignore
 */
package org.simpledbm.rss.api.thread;

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
