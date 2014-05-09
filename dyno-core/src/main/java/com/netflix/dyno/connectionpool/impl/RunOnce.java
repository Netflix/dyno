package com.netflix.dyno.connectionpool.impl;

import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.dyno.connectionpool.RetryPolicy;

public class RunOnce implements RetryPolicy {

	private AtomicInteger attempts = new AtomicInteger(0);
	
	@Override
	public void begin() {
	}

	@Override
	public void success() {
		attempts.incrementAndGet();
	}

	@Override
	public void failure(Exception e) {
		attempts.incrementAndGet();
	}

	@Override
	public boolean allowRetry() {
		return false;
	}

	@Override
	public int getAttemptCount() {
		return attempts.get();
	}
	
	public static class RetryFactory implements RetryPolicyFactory {

		@Override
		public RetryPolicy getRetryPolicy() {
			return new RunOnce();
		}
	}

}
