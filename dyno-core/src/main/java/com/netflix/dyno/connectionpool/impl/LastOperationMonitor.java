package com.netflix.dyno.connectionpool.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.dyno.connectionpool.OperationMonitor;

public class LastOperationMonitor implements OperationMonitor {

	private final ConcurrentHashMap<String, Long> latestTimings = new ConcurrentHashMap<String, Long>();
	private final ConcurrentHashMap<String, AtomicInteger> opCounters = new ConcurrentHashMap<String, AtomicInteger>();
	private final ConcurrentHashMap<String, AtomicInteger> opFailureCounters = new ConcurrentHashMap<String, AtomicInteger>();
	
	@Override
	public void recordLatency(String opName, long duration, TimeUnit unit) {
		latestTimings.put(opName, TimeUnit.MILLISECONDS.convert(duration, unit));
	}

	@Override
	public void recordSuccess(String opName) {
		AtomicInteger count = opCounters.get(opName);
		if (count == null) {
			opCounters.put(opName, new AtomicInteger(1));
		} else {
			count.incrementAndGet();
		}
	}

	@Override
	public void recordFailure(String opName, String reason) {
		AtomicInteger count = opFailureCounters.get(opName);
		if (count == null) {
			opFailureCounters.put(opName, new AtomicInteger(1));
		} else {
			count.incrementAndGet();
		}
	}

}
