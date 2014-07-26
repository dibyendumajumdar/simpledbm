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
package org.simpledbm.common.impl.thread;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.simpledbm.common.api.thread.Scheduler;

/**
 * Implements the Scheduler interface.
 * 
 * @author dibyendumajumdar
 */
public class SimpleScheduler implements Scheduler {

    /*
     * Three different thread pools are used:
     * 
     * ScheduledThreadPoolExecutor is used to trigger tasks that are required to
     * run repeatedly. This threadpool does not execute any tasks itself;
     * instead it delegates to one or the other threadpools, depending upon
     * priority of the task.
     * 
     * Two ThreadPoolExecutors are used for running tasks, one for priority
     * tasks and the other for normal tasks. There is no actual difference in
     * thread priority, but the high priority pool is meant to execute a
     * restricted set of tasks that should not be blocked because of normal
     * priority tasks. It is okay for a task to be blocked by another at the
     * same priority level.
     */

    final ThreadPoolExecutor priorityThreadPool;
    final ThreadPoolExecutor normalThreadPool;
    final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    public SimpleScheduler(Properties properties) {
        int priorityThreadPoolSize = Integer.valueOf(properties.getProperty(
                "scheduler.priorityThreadPoolSize", "5"));
        int priorityCoreThreads = Integer.valueOf(properties.getProperty(
                "scheduler.priorityCoreThreads", "2"));
        int priorityKeepAlive = Integer.valueOf(properties.getProperty(
                "scheduler.priorityKeepAliveTime", "60"));
        int normalThreadPoolSize = Integer.valueOf(properties.getProperty(
                "scheduler.normalThreadPoolSize", "5"));
        int normalCoreThreads = Integer.valueOf(properties.getProperty(
                "scheduler.normalCoreThreads", "1"));
        int normalKeepAlive = Integer.valueOf(properties.getProperty(
                "scheduler.normalKeepAliveTime", "60"));

        priorityThreadPool = new ThreadPoolExecutor(priorityCoreThreads,
                priorityThreadPoolSize, priorityKeepAlive, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        normalThreadPool = new ThreadPoolExecutor(normalCoreThreads,
                normalThreadPoolSize, normalKeepAlive, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());
        // The scheduler pool always has 1 core thread
        scheduledThreadPoolExecutor = (ScheduledThreadPoolExecutor) Executors
                .newScheduledThreadPool(1);
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(Priority priority,
            Runnable command, long initialDelay, long delay, TimeUnit unit) {
        if (priority == Priority.NORMAL)
            return scheduledThreadPoolExecutor.scheduleWithFixedDelay(
                    new ScheduleRunnable(this.normalThreadPool, command),
                    initialDelay, delay, unit);
        else {
            return scheduledThreadPoolExecutor.scheduleWithFixedDelay(
                    new ScheduleRunnable(this.priorityThreadPool, command),
                    initialDelay, delay, unit);
        }
    }

    public void execute(Priority priority, Runnable command) {
        if (priority == Priority.NORMAL)
            normalThreadPool.execute(command);
        else {
            priorityThreadPool.execute(command);
        }
    }

    public void shutdown() {
        scheduledThreadPoolExecutor.shutdownNow();
        try {
            scheduledThreadPoolExecutor.awaitTermination(60L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
        normalThreadPool.shutdownNow();
        try {
            normalThreadPool.awaitTermination(60L, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
        }
        priorityThreadPool.shutdownNow();
        try {
            priorityThreadPool.awaitTermination(60L, TimeUnit.SECONDS);
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
    }

    static final class ScheduleRunnable implements Runnable {
        final ExecutorService executor;
        final Runnable runnable;

        ScheduleRunnable(ExecutorService executor, Runnable runnable) {
            this.executor = executor;
            this.runnable = runnable;
        }

        public void run() {
            executor.submit(runnable);
        }
    }
}
