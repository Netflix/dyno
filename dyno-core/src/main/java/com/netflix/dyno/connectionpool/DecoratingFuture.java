package com.netflix.dyno.connectionpool;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DecoratingFuture<V> implements Future<V> {

	private final ListenableFuture<OperationResult<V>> innerFuture; 
	
	public DecoratingFuture(final ListenableFuture<OperationResult<V>> listenableFuture) {
		innerFuture = listenableFuture;
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
		OperationResult<V> opResult = innerFuture.get();
		return opResult.getResult();
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		OperationResult<V> opResult = innerFuture.get(timeout, unit);
		return opResult.getResult();
	}
}
