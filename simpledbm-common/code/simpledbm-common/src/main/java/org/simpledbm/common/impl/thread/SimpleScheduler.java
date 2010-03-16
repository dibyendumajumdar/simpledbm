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
	 * ScheduledThreadPoolExecutor is used to trigger tasks that are
	 * required to run repeatedly. This threadpool does not execute any tasks
	 * itself; instead it delegates to one or the other threadpools, depending upon
	 * priority of the task.
	 * 
	 * Two ThreadPoolExecutors are used for running tasks, one for priority
	 * tasks and the other for normal tasks. There is no actual difference in
	 * thread priority, but the high priority pool is meant to execute a restricted
	 * set of tasks that should not be blocked because of normal priority tasks.
	 * It is okay for a task to be blocked by another at the same priority level.
	 */
	
	final ThreadPoolExecutor priorityThreadPool;
	final ThreadPoolExecutor normalThreadPool;
	final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

	public SimpleScheduler(Properties properties) {
		int priorityThreadPoolSize = Integer.valueOf(properties.getProperty("scheduler.priorityThreadPoolSize", "10"));
		int priorityCoreThreads = Integer.valueOf(properties.getProperty("scheduler.priorityCoreThreads", "1"));
		int priorityKeepAlive = Integer.valueOf(properties.getProperty("scheduler.priorityKeepAliveTime", "180"));
		int normalThreadPoolSize = Integer.valueOf(properties.getProperty("scheduler.normalThreadPoolSize", "25"));
		int normalCoreThreads = Integer.valueOf(properties.getProperty("scheduler.normalCoreThreads", "1"));
		int normalKeepAlive = Integer.valueOf(properties.getProperty("scheduler.normalKeepAliveTime", "60"));
		
		priorityThreadPool = new ThreadPoolExecutor(priorityCoreThreads, priorityThreadPoolSize, priorityKeepAlive, TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>());
		normalThreadPool = new ThreadPoolExecutor(normalCoreThreads, normalThreadPoolSize, normalKeepAlive, TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>());
		// The scheduler pool always has 1 core thread
		scheduledThreadPoolExecutor = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1);		
	}

	public ScheduledFuture<?> scheduleWithFixedDelay(Priority priority, Runnable command,
			long initialDelay, long delay, TimeUnit unit) {
		if (priority == Priority.NORMAL)
			return scheduledThreadPoolExecutor.scheduleWithFixedDelay(new ScheduleRunnable(this.normalThreadPool, command), initialDelay, delay, unit);
		else {
			return scheduledThreadPoolExecutor.scheduleWithFixedDelay(new ScheduleRunnable(this.priorityThreadPool, command), initialDelay, delay, unit);			
		}
	}
	
	public void execute(Priority priority, Runnable command) {
		if (priority == Priority.NORMAL)
			normalThreadPool.execute(command);
		else 
			priorityThreadPool.execute(command);
	}		

	public void shutdown() {
		scheduledThreadPoolExecutor.shutdown();
		try {
			scheduledThreadPoolExecutor.awaitTermination(60L, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		}
		normalThreadPool.shutdown();
		try {
			normalThreadPool.awaitTermination(60L, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		}
		priorityThreadPool.shutdown();
		try {
			priorityThreadPool.awaitTermination(60L, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
		}		
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
