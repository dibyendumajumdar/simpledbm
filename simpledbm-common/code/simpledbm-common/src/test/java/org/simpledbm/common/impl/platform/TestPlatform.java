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
package org.simpledbm.common.impl.platform;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.simpledbm.common.api.thread.Scheduler;
import org.simpledbm.common.api.thread.Scheduler.Priority;
import org.simpledbm.junit.BaseTestCase;

/**
 * Test cases for the Object Registry module.
 * 
 * @author Dibyendu Majumdar
 * @since 21-Aug-2005
 */
public class TestPlatform extends BaseTestCase {

    public TestPlatform() {
        super();
    }

    public TestPlatform(String arg0) {
        super(arg0);
    }

    static final class MyTask implements Runnable {
        static AtomicInteger TaskId = new AtomicInteger();
        int taskId = TaskId.incrementAndGet();
        final int delay;

        MyTask(int delay) {
            this.delay = delay;
        }

        public void run() {
            System.err.println("I am running " + this);
        }

        public String toString() {
            return "MyTask(id=" + taskId + ", delay=" + delay + ")";
        }

    }

    public void testScheduler() throws InterruptedException {
        Scheduler scheduler = platform.getScheduler();
        ScheduledFuture<?> t1 = scheduler.scheduleWithFixedDelay(Priority.NORMAL, new MyTask(5), 5, 5,
                TimeUnit.SECONDS);
        ScheduledFuture<?> t2 = scheduler.scheduleWithFixedDelay(Priority.NORMAL, new MyTask(3), 3, 3,
                TimeUnit.SECONDS);
        ScheduledFuture<?> t3 = scheduler.scheduleWithFixedDelay(Priority.NORMAL, new MyTask(10), 10,
                10, TimeUnit.SECONDS);
        ScheduledFuture<?> t4 = scheduler.scheduleWithFixedDelay(Priority.SERVER_TASK, new MyTask(1),
                1, 1, TimeUnit.SECONDS);

        try {
            Thread.sleep(30 * 1000);
        } catch (InterruptedException e) {
        }
//        Map<Thread, StackTraceElement[]> m = Thread.getAllStackTraces();
//        for (Thread t: m.keySet()) {
//            StackTraceElement[] se = m.get(t);
//            System.err.println(t);
//            for (StackTraceElement el: se) {
//                System.err.println(el);
//            }
//        }       
        t1.cancel(false);
        t2.cancel(false);
        t3.cancel(false);
        t4.cancel(false);
        scheduler.shutdown();
    }

}
