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

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.RetryPolicy;

/**
 * Simple impl that allows at most one attempt which essentially mean no retries. 
 * 
 * @author poberai
 *
 */
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
		return attempts.get() == 0;
	}

	@Override
	public int getAttemptCount() {
		return attempts.get() > 0 ? 1 : 0;
	}
	
	public static class RetryFactory implements RetryPolicyFactory {

		@Override
		public RetryPolicy getRetryPolicy() {
			return new RunOnce();
		}
	}

	@Override
	public boolean allowRemoteDCFallback() {
		return false;
	}

	public static class UnitTest {
		
		@Test
		public void testRetry() throws Exception {
			
			RunOnce retry = new RunOnce();
			
			RuntimeException e = new RuntimeException("failure");
			retry.begin();
			
			Assert.assertTrue(retry.allowRetry());
			
			retry.failure(e);
			Assert.assertFalse(retry.allowRetry());
			retry.failure(e);
			Assert.assertFalse(retry.allowRetry());
			
			Assert.assertEquals(1, retry.getAttemptCount());
		}
	}

}
