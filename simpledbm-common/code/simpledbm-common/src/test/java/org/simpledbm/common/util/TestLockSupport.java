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
package org.simpledbm.common.util;

import junit.framework.TestCase;

public class TestLockSupport extends TestCase {

	public TestLockSupport(String name) {
		super(name);
	}
	
	public void testDummy() {
		
	}

//	public void testPark() {
//        long then = System.nanoTime();
//        long timeToWait = TimeUnit.NANOSECONDS.convert(
//                5,
//                TimeUnit.SECONDS);
//        long timeToPark = TimeUnit.NANOSECONDS.convert(
//                2,
//                TimeUnit.SECONDS);
//        System.out.println("New timetowait = " + timeToWait);
//        System.out.println("New timetopark = " + timeToPark);
//        LockSupport.parkNanos(-timeToPark);
//        long now = System.nanoTime();
//        if (timeToWait > 0) {
//        	timeToWait -= (now - then);
//            then = now;
//        }
//        System.out.println("New timetowait = " + timeToWait);
//        
// 	}
//	
//	public void testTimer() {
//        long then = System.nanoTime();
//        long timeToWait = TimeUnit.NANOSECONDS.convert(
//                5,
//                TimeUnit.SECONDS);
//        System.out.println("Timetowait = " + timeToWait);
//        SimpleTimer timer = new SimpleTimer(timeToWait);
//        timer.await();
//        long now = System.nanoTime();
//        System.out.println("Time waited = " + (now-then));
//        assertTrue(timer.isExpired());
// 	}
//	
//	static class MyRunnable implements Runnable {
//
//		long nanos;
//		
//		MyRunnable(long nanos) {
//			this.nanos = nanos;
//		}
//		
//		public void run() {
//			System.err.println("Starting first park");
//			SimpleDBMLockSupport.park();
//			System.err.println("Unparked");
//			try {
//				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//			}
//			System.err.println("Starting second park");
//			SimpleDBMLockSupport.park();
//			System.err.println("Unparked");
//		}
//		
//	}
//	
//	public void testLockSupport() {
//		Thread t = new Thread(new MyRunnable(5000000000L));
//		t.start();
//		try {
//			Thread.sleep(1000);
//		} catch (InterruptedException e) {
//		}
//		System.err.println("Unparking thread");
//		SimpleDBMLockSupport.unpark(t);
//		SimpleDBMLockSupport.unpark(t);
//		try {
//			Thread.sleep(5000);
//		} catch (InterruptedException e) {
//		}
//		System.err.println("Unparking thread");
//		SimpleDBMLockSupport.unpark(t);
//	}
}
