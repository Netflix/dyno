package com.netflix.dyno.connectionpool.impl;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.OperationMonitor;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.exception.DynoException;

public class OperationResultImpl<R> implements OperationResult<R> {

	private final String opName;
	private final R result; 
	private final Future<R> futureResult;
	private Host host = null;
	private long duration = 0;
	private int attempts = 0;
	private final OperationMonitor opMonitor; 
	
	public OperationResultImpl(String name, R r, OperationMonitor monitor) {
		opName = name;
		result = r;
		futureResult = null;
		opMonitor = monitor;
	}
	
	public OperationResultImpl(String name, Future<R> future, OperationMonitor monitor) {
		opName = name;
		result = null;
		futureResult = future;
		opMonitor = monitor;
	}

	@Override
	public Host getNode() {
		return host;
	}

	@Override
	public R getResult() {
		try {
			return futureResult != null ? futureResult.get() : result;
		} catch (Exception e) {
			throw new DynoException(e);
		}
	}

	@Override
	public long getLatency() {
		return duration;
	}

	@Override
	public long getLatency(TimeUnit units) {
		return units.convert(duration, TimeUnit.MILLISECONDS);
	}

	@Override
	public int getAttemptsCount() {
		return attempts;
	}

	@Override
	public void setAttemptsCount(int count) {
		attempts = count;
	}

	public OperationResultImpl<R> node(Host h) {
		host = h;
		return this;
	}
	
	public OperationResultImpl<R> attempts(int count) {
		attempts = count;
		return this;
	}
	
	public OperationResultImpl<R> latency(long time) {
		this.duration = time;
		if (opMonitor != null) {
			opMonitor.recordLatency(opName, time, TimeUnit.MILLISECONDS);
		}
		return this;
	}
	
	@Override
	public void setLatency(long time, TimeUnit unit) {
		this.duration = TimeUnit.MILLISECONDS.convert(time, unit);
		if (opMonitor != null) {
			opMonitor.recordLatency(opName, time, unit);
		}
	}
}
