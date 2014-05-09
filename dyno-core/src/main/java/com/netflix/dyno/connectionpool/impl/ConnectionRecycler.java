package com.netflix.dyno.connectionpool.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.joda.time.DateTime;

import com.netflix.dyno.connectionpool.Connection;

public class ConnectionRecycler {
	
	private final ExecutorService threadPool = Executors.newFixedThreadPool(1);
	private final LinkedBlockingQueue<RecycleTask> queue = new LinkedBlockingQueue<RecycleTask>();
	
	private final ConcurrentHashMap<Connection<?>, DateTime> lastRecycledMap = new ConcurrentHashMap<Connection<?>, DateTime>();
	private final AtomicLong lastMapClearTimestamp = new AtomicLong(System.currentTimeMillis());
	
	private final AtomicBoolean stop = new AtomicBoolean(false);
	
	private static final ConnectionRecycler Instance = new ConnectionRecycler();
	
	public static ConnectionRecycler getInstance() {
		return Instance;
	}
	
	private ConnectionRecycler() {
		
	}

	public Future<Connection<?>> submitForRecycle(Connection<?> connToBeRemoved, Callable<Void> closeConn, Callable<Connection<?>> createConn) {
		
		RecycleTask task = new RecycleTask(connToBeRemoved, closeConn, createConn);
		queue.add(task);
		return task.futureTask;
	}

	public void start() {

		threadPool.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {

				while (!stop.get() && !Thread.currentThread().isInterrupted()) {
					try  { 
						RecycleTask task = queue.take();
						
						Connection<?> connToRemove = task.connectionToBeRemoved; 
						
						if (allowRecycle(connToRemove)) {
							
							lastRecycledMap.put(connToRemove, DateTime.now());
							try {
								task.closeConnectionTask.call();
							} catch (Exception e) {
							}
							try {
								task.futureTask.run();
								
							} catch (Exception e) {
							}
						} else {
							task.createConn.set(false);
							task.futureTask.run();
						}
						
						clearMapIfNeeded();
						
					} catch (InterruptedException e) {

					}
				}
				
				List<RecycleTask> pendingTasks = new ArrayList<RecycleTask>();
				queue.drainTo(pendingTasks);
				
				for (RecycleTask pendingTask : pendingTasks) {
					pendingTask.futureTask.cancel(true);
				}
				
				return null;
			}

		});
	}
	
	public void stop() {
		stop.set(true);
		threadPool.shutdownNow();
	}

	private void clearMapIfNeeded() {
		
		long now = System.currentTimeMillis();
		if ((now - lastMapClearTimestamp.get()) > 3600*1000) {
			lastRecycledMap.clear();
			lastMapClearTimestamp.set(now);
		}
	}

	
	private boolean allowRecycle(Connection<?> connection) {
		
		if (connection == null) {
			return false;
		}
		DateTime date = lastRecycledMap.get(connection);
		return date == null;  // allow recycle only if we haven't seen this connection before
	}
	
	class RecycleTask {
		
		private final Connection<?> connectionToBeRemoved; 
		private final Callable<Void> closeConnectionTask;
		private final Callable<Connection<?>> createConnectionTask;
		
		private final AtomicBoolean createConn = new AtomicBoolean(true);
		
		private final FutureTask<Connection<?>> futureTask;
		
		RecycleTask(Connection<?> connToBeRemoved, Callable<Void> closeConneTask, Callable<Connection<?>> createConnTask) {
			
			this.connectionToBeRemoved = connToBeRemoved;
			this.closeConnectionTask = closeConneTask;
			this.createConnectionTask = createConnTask;
			
			futureTask  = new FutureTask<Connection<?>>(new Callable<Connection<?>>() {

				@Override
				public Connection<?> call() throws Exception {
					if (createConn.get()) {
						return createConnectionTask.call();
					} else {
						return null;
					}
				}
				
			});
		}
	}

}
