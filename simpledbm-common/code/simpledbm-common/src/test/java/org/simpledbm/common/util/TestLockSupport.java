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
