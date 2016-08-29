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
