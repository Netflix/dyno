package com.netflix.dyno.connectionpool.impl;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.OperationMonitor;
import com.netflix.dyno.connectionpool.OperationResult;

public class FutureOperationalResultImpl<R> implements Future<OperationResult<R>> {
	
	private final Future<R> future; 
	private final OperationResultImpl<R> opResult; 
	private final long startTime;
	private final AtomicBoolean timeRecorded = new AtomicBoolean(false);
	
	public FutureOperationalResultImpl(String opName, Future<R> rFuture, long start, OperationMonitor opMonitor) {
		this.future = rFuture;
		this.opResult = new OperationResultImpl<R>(opName, rFuture, opMonitor).attempts(1);
		this.startTime = start;
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return future.cancel(mayInterruptIfRunning);
	}

	@Override
	public boolean isCancelled() {
		return future.isCancelled();
	}

	@Override
	public boolean isDone() {
		return future.isDone();
	}

	@Override
	public OperationResult<R> get() throws InterruptedException, ExecutionException {
		try {
			future.get();
			return opResult;
		} finally {
			recordTimeIfNeeded();
		}
	}

	private void recordTimeIfNeeded() {
		if (timeRecorded.get()) {
			return;
		}
		if (timeRecorded.compareAndSet(false, true)) {
			opResult.setLatency(System.currentTimeMillis()-startTime, TimeUnit.MILLISECONDS);
		}
	}

	@Override
	public OperationResult<R> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		try {
			future.get(timeout, unit);
			return opResult;
		} finally {
			recordTimeIfNeeded();
		}
	}
	
	public FutureOperationalResultImpl<R> node(Host node) {
		opResult.setNode(node);
		return this;
	}
	
	public OperationResultImpl<R> getOpResult() {
		return opResult;
	}
}
