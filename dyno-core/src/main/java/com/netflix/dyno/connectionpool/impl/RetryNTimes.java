package com.netflix.dyno.connectionpool.impl;

import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.dyno.connectionpool.RetryPolicy;

public class RetryNTimes implements RetryPolicy {

	private int n; 
	private final AtomicInteger count = new AtomicInteger(0);
	private final boolean allowRemoteDCFallback; 
	
	public RetryNTimes(int n, boolean allowFallback) {
		this.n = n;
		this.allowRemoteDCFallback = allowFallback;
	}

	@Override
	public void begin() {
	}

	@Override
	public void success() {
		count.incrementAndGet();
	}

	@Override
	public void failure(Exception e) {
		count.incrementAndGet();
	}

	@Override
	public boolean allowRetry() {
		return count.get() <= n;
	}

	@Override
	public int getAttemptCount() {
		return count.get();
	}

	@Override
	public boolean allowRemoteDCFallback() {
		return allowRemoteDCFallback;
	}
	
	public static class RetryFactory implements RetryPolicyFactory {
		
		int n; 
		boolean allowDCFallback;
		public RetryFactory(int n, boolean allowFallback) {
			this.n = n;
			this.allowDCFallback = allowFallback;
		}
		
		@Override
		public RetryPolicy getRetryPolicy() {
			return new RetryNTimes(n, allowDCFallback);
		}
	}
}
