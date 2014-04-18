package com.netflix.dyno.connectionpool.impl;

import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.dyno.connectionpool.RetryPolicy;

public class RetryNTimes implements RetryPolicy {

	private int n; 
	private final AtomicInteger count = new AtomicInteger(0);
	
	public RetryNTimes(int n) {
		this.n = n;
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
		return count.get() < n;
	}

	@Override
	public int getAttemptCount() {
		return count.get();
	}
	
	public static class RetryFactory implements RetryPolicyFactory {
		
		int n; 
		public RetryFactory(int n) {
			this.n = n;
		}
		
		@Override
		public RetryPolicy getRetryPolicy() {
			return new RetryNTimes(n);
		}
	}
}
