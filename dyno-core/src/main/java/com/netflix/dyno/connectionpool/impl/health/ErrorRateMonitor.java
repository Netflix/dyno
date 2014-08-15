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
package com.netflix.dyno.connectionpool.impl.health;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.ErrorRateMonitorConfig;
import com.netflix.dyno.connectionpool.ErrorRateMonitorConfig.ErrorThreshold;
import com.netflix.dyno.connectionpool.impl.health.RateTracker.Bucket;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.RateLimitUtil;

/**
 * Class that can be used to track the error rates for {@link ConnectionPoolHealthTracker}
 * It makes use of {@link RateTracker} to track the error rates and then periodically applies the 
 * {@link ErrorRateMonitorConfig} to apply error rate check policies to determine error threshold violations.
 * 
 * @author poberai
 *
 */
public class ErrorRateMonitor {

	private final List<ErrorCheckPolicy> policies = new ArrayList<ErrorCheckPolicy>();
	private final AtomicLong lastCheckTimestamp = new AtomicLong(0L);
	private final AtomicLong suppressCheckTimestamp = new AtomicLong(0L);
	
	private final AtomicReference<String> errorCheckLock = new AtomicReference<String>(null);
	
	private final long errorCheckFrequencySeconds; 
	private final RateTracker rateTracker;
	private final int suppressErrorWindow;

	public ErrorRateMonitor(int windowSize, int checkFrequency, int suppressWindow) {
		
		this.rateTracker = new RateTracker(windowSize);
		this.errorCheckFrequencySeconds = checkFrequency;
		this.lastCheckTimestamp.set(System.currentTimeMillis()/1000);
		this.suppressErrorWindow = suppressWindow;
	}
	
	public ErrorRateMonitor(ErrorRateMonitorConfig config) {
		
		this(config.getWindowSizeSeconds(), config.getCheckFrequencySeconds(), config.getCheckSuppressWindowSeconds());
		for (ErrorThreshold threshold : config.getThresholds()) {
			addPolicy(new SimpleErrorCheckPolicy(threshold));
		}
	}

	public void addPolicy(ErrorCheckPolicy policy) {
		policies.add(policy);
	}
	
	public boolean trackErrorRate(int count) {
		
		long timestamp = System.currentTimeMillis()/1000;
		
		this.rateTracker.trackRate(count);
		
		if ((timestamp - lastCheckTimestamp.get()) >= errorCheckFrequencySeconds) {
			
			if ((timestamp - suppressCheckTimestamp.get()) <= suppressErrorWindow) {
				// don't check error. This is to prevent repeatedly firing alerts 
				return true; 
			}

			String expected = errorCheckLock.get();
			boolean casWon = errorCheckLock.compareAndSet(expected, UUID.randomUUID().toString());
			
			if (casWon) {
				// record that we checked
				lastCheckTimestamp.set(timestamp);
				
				boolean failure = false;
				List<Bucket> buckets = rateTracker.getAllBuckets();
				
				for (ErrorCheckPolicy policy : policies) {
					failure = policy.checkErrorRate(buckets);
					if (failure) {
						break;
					}
				}
				
				if (failure) {
					// Set the timestamp to suppress subsequent alerts for the configured time period
					suppressCheckTimestamp.set(timestamp);
				}
				return !failure;
			}
		}
		
		return true;
	}
	
	public interface ErrorCheckPolicy {
		
		public boolean checkErrorRate(List<Bucket> buckets);
	}
	
	public static class SimpleErrorCheckPolicy implements ErrorCheckPolicy {

		private final int perBucketThreshold; 
		private final int windowSize;
		private final int bucketCoveragePercentage;
		
		public SimpleErrorCheckPolicy(int bucketThreshold, int numBuckets, int bucketCoverage) {
			this.perBucketThreshold = bucketThreshold;
			this.windowSize = numBuckets;
			this.bucketCoveragePercentage = bucketCoverage;
		}
		
		public SimpleErrorCheckPolicy(ErrorThreshold threshold) {
			this(threshold.getThresholdPerSecond(), threshold.getWindowSeconds(), threshold.getWindowCoveragePercentage());
		}
		
		@Override
		public boolean checkErrorRate(List<Bucket> buckets) {
			
			int minViolationBucketThreshold = (windowSize * bucketCoveragePercentage)/100;
			
			int numBucketsOverThreshold = 0;
			for (Bucket b : buckets) {
				if (b.count() >= perBucketThreshold) {
					numBucketsOverThreshold++;
				}
			}
			
			return numBucketsOverThreshold >= minViolationBucketThreshold;
		}
	}
	
	public static class UnitTest {
		
		@Test 
		public void testSimpleErrorCheckPolicy() throws Exception {
			
			List<Bucket> buckets = getBuckets(116, 120, 121, 120, 130, 125, 130, 120, 120, 120);

			SimpleErrorCheckPolicy policy = new SimpleErrorCheckPolicy(120, 10, 80);
			Assert.assertTrue(policy.checkErrorRate(buckets));
			
			policy = new SimpleErrorCheckPolicy(121, 10, 80);
			Assert.assertFalse(policy.checkErrorRate(buckets));
			
			policy = new SimpleErrorCheckPolicy(130, 10, 20);
			Assert.assertTrue(policy.checkErrorRate(buckets));
			
		}
		
		private List<Bucket> getBuckets(Integer ... values) {
			List<Bucket> buckets = new ArrayList<Bucket>();
			
			for (Integer i : values) {
				Bucket b = new Bucket();
				b.track(i);
				buckets.add(b);
			}
			
			return buckets;
		}
		
		@Test
		public void testNoErrorCheckTriggers() throws Exception {
			
			final ErrorRateMonitor errorMonitor = new ErrorRateMonitor(20, 1, 10);
			errorMonitor.addPolicy(new SimpleErrorCheckPolicy(130, 8, 80));   // 80% of 10 seconds, if error rate > 120 then alert 
			errorMonitor.addPolicy(new SimpleErrorCheckPolicy(200, 4, 80));  // 80% of 5 seconds, if error rate > 200 then alert 
			
			List<Integer> rates = CollectionUtils.newArrayList(90, 120, 180);
			int errorCount = runTest(9, errorMonitor, rates);
			
			Assert.assertEquals(0, errorCount);
		}

		@Test
		public void testSustainedErrorTriggers() throws Exception {
			
			final ErrorRateMonitor errorMonitor = new ErrorRateMonitor(20, 1, 10);
			errorMonitor.addPolicy(new SimpleErrorCheckPolicy(130, 8, 80));   // 80% of 10 seconds, if error rate > 120 then alert 
			errorMonitor.addPolicy(new SimpleErrorCheckPolicy(200, 4, 80));  // 80% of 5 seconds, if error rate > 200 then alert 
			
			List<Integer> rates = CollectionUtils.newArrayList(130, 140, 180);
			int errorCount = runTest(9, errorMonitor, rates);
			
			Assert.assertEquals(1, errorCount);
		}
		
		@Test
		public void testOnlyLargeSpikeTriggers() throws Exception {
			
			final ErrorRateMonitor errorMonitor = new ErrorRateMonitor(20, 1, 10);
			errorMonitor.addPolicy(new SimpleErrorCheckPolicy(130, 10, 80));   // 80% of 10 seconds, if error rate > 120 then alert 
			errorMonitor.addPolicy(new SimpleErrorCheckPolicy(200, 4, 80));  // 80% of 5 seconds, if error rate > 200 then alert 
			
			List<Integer> rates = new ArrayList<Integer>(); rates.add(110); rates.add(250);
			int errorCount = runTest(10, errorMonitor, rates);
			
			Assert.assertEquals(1, errorCount);
		}
		
		private int runTest(int totalTestRunTimeSeconds, final ErrorRateMonitor errorMonitor, final List<Integer> rates) throws Exception {
			
			int numThreads = 5; 
			ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
			
			final AtomicReference<RateLimitUtil> limiter = new AtomicReference<RateLimitUtil>(RateLimitUtil.create(rates.get(0)));
			final AtomicBoolean stop = new AtomicBoolean(false);
			
			final CyclicBarrier barrier = new CyclicBarrier(numThreads+1);
			final CountDownLatch latch = new CountDownLatch(numThreads);
			
			final AtomicInteger errorCount = new AtomicInteger(0);
			
			for (int i=0; i<numThreads; i++) {
				
				threadPool.submit(new Callable<Void>() {

					@Override
					public Void call() throws Exception {
						
						barrier.await();
						while (!stop.get() && !Thread.currentThread().isInterrupted()) {
							if (limiter.get().acquire()) {
								boolean success = errorMonitor.trackErrorRate(1);
								if (!success) {
									errorCount.incrementAndGet();
								}
							}
						}
						latch.countDown();
						return null;
					}
				});
			}

			barrier.await();

			int numIterations = rates.size();
			int sleepPerIteration = totalTestRunTimeSeconds/numIterations;
			
			int round = 1; 
			do {
				Thread.sleep(sleepPerIteration*1000);
				if (round < rates.size()) {
					System.out.println("Changing rate to " + rates.get(round));
					limiter.set(RateLimitUtil.create(rates.get(round)));
				}
				round++;
			} while (round <= numIterations);
			
			stop.set(true);
			latch.await();
			threadPool.shutdownNow();
			
			List<Bucket> buckets = errorMonitor.rateTracker.getAllBuckets();
			for (Bucket b : buckets) {
				System.out.print("  " + b.count());
			}
			System.out.println("\n=========TEST DONE==============");
			
			return errorCount.get();
		}
	}
}
