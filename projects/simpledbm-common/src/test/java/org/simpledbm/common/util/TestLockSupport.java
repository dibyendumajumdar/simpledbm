/**
 * DO NOT REMOVE COPYRIGHT NOTICES OR THIS HEADER.
 *
 * Contributor(s):
 *
 * The Original Software is SimpleDBM (www.simpledbm.org).
 * The Initial Developer of the Original Software is Dibyendu Majumdar.
 *
 * Portions Copyright 2005-2014 Dibyendu Majumdar. All Rights Reserved.
 *
 * The contents of this file are subject to the terms of the
 * Apache License Version 2 (the "APL"). You may not use this
 * file except in compliance with the License. A copy of the
 * APL may be obtained from:
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Alternatively, the contents of this file may be used under the terms of
 * either the GNU General Public License Version 2 or later (the "GPL"), or
 * the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
 * in which case the provisions of the GPL or the LGPL are applicable instead
 * of those above. If you wish to allow use of your version of this file only
 * under the terms of either the GPL or the LGPL, and not to allow others to
 * use your version of this file under the terms of the APL, indicate your
 * decision by deleting the provisions above and replace them with the notice
 * and other provisions required by the GPL or the LGPL. If you do not delete
 * the provisions above, a recipient may use your version of this file under
 * the terms of any one of the APL, the GPL or the LGPL.
 *
 * Copies of GPL and LGPL may be obtained from:
 * http://www.gnu.org/licenses/license-list.html
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
