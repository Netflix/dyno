package com.netflix.dyno.connectionpool;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DecoratingListenableFuture<V> implements ListenableFuture<V> {

	private final Future<V> innerFuture; 
	
	public DecoratingListenableFuture(Future<V> future) {
		innerFuture = future;
	}
	
	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return innerFuture.cancel(mayInterruptIfRunning);
	}

	@Override
	public boolean isCancelled() {
		return innerFuture.isCancelled();
	}

	@Override
	public boolean isDone() {
		return innerFuture.isDone();
	}

	@Override
	public V get() throws InterruptedException, ExecutionException {
		return innerFuture.get();
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return innerFuture.get(timeout, unit);
	}

	@Override
	public void addListener(Runnable listener, Executor executor) {
		throw new RuntimeException("Not Implemented");
	}
}
