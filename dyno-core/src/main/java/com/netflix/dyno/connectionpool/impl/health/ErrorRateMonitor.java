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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.dyno.connectionpool.ErrorRateMonitorConfig;
import com.netflix.dyno.connectionpool.ErrorRateMonitorConfig.ErrorThreshold;
import com.netflix.dyno.connectionpool.impl.health.RateTracker.Bucket;

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
	
	// used for unit tests
	RateTracker getRateTracker() {
		return rateTracker;
	}
}
