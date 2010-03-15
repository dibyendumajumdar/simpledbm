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

public class SimpleScheduler implements Scheduler {
	
	final ThreadPoolExecutor priorityThreadPool;
	final ThreadPoolExecutor normalThreadPool;
	final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

	public SimpleScheduler(Properties properties) {
		int priorityThreadPoolSize = Integer.valueOf(properties.getProperty("scheduler.priorityThreadPoolSize", "5"));
		int normalThreadPoolSize = Integer.valueOf(properties.getProperty("scheduler.normalThreadPoolSize", "25"));
		
		priorityThreadPool = new ThreadPoolExecutor(1, priorityThreadPoolSize, 180L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>());
		normalThreadPool = new ThreadPoolExecutor(1, normalThreadPoolSize, 60L, TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>());
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
//			System.err.println("Submitting task " + runnable);
			executor.submit(runnable);
//			System.err.println("Pool size " + ((ThreadPoolExecutor)executor).getPoolSize());
		}
	}
}
