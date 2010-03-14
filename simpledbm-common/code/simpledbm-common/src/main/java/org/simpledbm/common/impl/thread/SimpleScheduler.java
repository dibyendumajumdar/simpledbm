package org.simpledbm.common.impl.thread;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.simpledbm.common.api.thread.Scheduler;

public class SimpleScheduler extends ScheduledThreadPoolExecutor implements Scheduler {
	
	final ExecutorService cachedThreadPoolService;

	public SimpleScheduler(ExecutorService cachedThreadPoolService,
			int corePoolSize,
			RejectedExecutionHandler handler) {
		super(corePoolSize, handler);
		this.cachedThreadPoolService = cachedThreadPoolService;
	}

	public SimpleScheduler(ExecutorService cachedThreadPoolService, int corePoolSize,
			ThreadFactory threadFactory, RejectedExecutionHandler handler) {
		super(corePoolSize, threadFactory, handler);
		this.cachedThreadPoolService = cachedThreadPoolService;
	}

	public SimpleScheduler(ExecutorService cachedThreadPoolService, int corePoolSize,
			ThreadFactory threadFactory) {
		super(corePoolSize, threadFactory);
		this.cachedThreadPoolService = cachedThreadPoolService;
	}

	public SimpleScheduler(ExecutorService cachedThreadPoolService, int corePoolSize) {
		super(corePoolSize);
		this.cachedThreadPoolService = cachedThreadPoolService;
	}

	@Override
	public <V> ScheduledFuture<V> schedule(Callable<V> callable,
			long delay, TimeUnit unit) {
		return super.schedule(new ScheduleCallable<V>(this.cachedThreadPoolService, callable), delay, unit);
	}

	@Override
	public ScheduledFuture<?> schedule(Runnable command, long delay,
			TimeUnit unit) {
		return super.schedule(new ScheduleRunnable(this.cachedThreadPoolService, command), delay, unit);
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(Runnable command,
			long initialDelay, long period, TimeUnit unit) {
		return super.scheduleAtFixedRate(new ScheduleRunnable(this.cachedThreadPoolService, command), initialDelay, period, unit);
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command,
			long initialDelay, long delay, TimeUnit unit) {
		return super.scheduleWithFixedDelay(new ScheduleRunnable(this.cachedThreadPoolService, command), initialDelay, delay, unit);
	}
	
	static final class ScheduleRunnable implements Runnable {
		final ExecutorService executor;
		final Runnable runnable;
		ScheduleRunnable(ExecutorService executor, Runnable runnable) {
			this.executor = executor;
			this.runnable = runnable;
		}
		public void run() {
			System.err.println("Submitting task " + runnable);
			executor.submit(runnable);
			System.err.println("Pool size " + ((ThreadPoolExecutor)executor).getPoolSize());
		}
	}

	static final class ScheduleCallable<V> implements Callable<V> {

		final ExecutorService executor;
		final Callable<V> runnable;
		
		ScheduleCallable(ExecutorService executor, Callable<V> runnable) {
			this.executor = executor;
			this.runnable = runnable;
		}
		public V call() throws Exception {
			executor.submit(runnable);
			return null;
		}
	}	
	
}
