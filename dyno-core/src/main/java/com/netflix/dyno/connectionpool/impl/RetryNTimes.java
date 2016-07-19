/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.dyno.connectionpool.impl;

import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.dyno.connectionpool.RetryPolicy;

/**
 * Simple implementation of {@link RetryPolicy} that ensures an operation can be re tried at most N times. 
 * 
 * Note that RetryNTimes (2) means that a total of 2 + 1 = 3 attempts will be allowed before giving up.
 * 
 * @author poberai
 *
 */
public class RetryNTimes implements RetryPolicy {

	private int n; 
	private final AtomicInteger count = new AtomicInteger(0);
	private final boolean allowCrossZoneFallback;
	
	public RetryNTimes(int n, boolean allowFallback) {
		this.n = n;
		this.allowCrossZoneFallback = allowFallback;
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
	public boolean allowCrossZoneFallback() {
		return allowCrossZoneFallback;
	}
	
	public static class RetryFactory implements RetryPolicyFactory {
		
		int n; 
		boolean allowCrossZoneFallback;
		
		public RetryFactory(int n) {
			this(n, true);
		}
		
		public RetryFactory(int n, boolean allowFallback) {
			this.n = n;
			this.allowCrossZoneFallback = allowFallback;
		}
		
		@Override
		public RetryPolicy getRetryPolicy() {
			return new RetryNTimes(n, allowCrossZoneFallback);
		}

		@Override
		public String toString() {
			return "RetryFactory{" +
					"n=" + n +
					", allowCrossZoneFallback=" + allowCrossZoneFallback +
					'}';
		}
	}
}
