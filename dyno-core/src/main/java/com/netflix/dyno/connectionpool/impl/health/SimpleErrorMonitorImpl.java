/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.connectionpool.impl.health;

import java.util.concurrent.atomic.AtomicInteger;

	public class SimpleErrorMonitorImpl implements ErrorMonitor {

		private final AtomicInteger errorCount = new AtomicInteger(0);
		private final int threshold;
		private final int maxValue;

		public SimpleErrorMonitorImpl(int maxValue, int numErrorThreshold) {
			this.maxValue = maxValue;
			threshold = numErrorThreshold;
		}

		@Override
		public boolean trackError(int numErrors) {

			int currentCount = errorCount.addAndGet(numErrors);
			if (currentCount >= threshold ||
					(maxValue > 0 && currentCount == maxValue)) {
				// Reset the count
				boolean success = errorCount.compareAndSet(currentCount, 0);
				if (success) {
					return false;  // ERROR above threshold!
				} else {
					return true;   // all OK. Someone else beat us to reporting the errors as above threshold
				}
			}
			return true; // Errors NOT above threshold
		}

	
	public static class SimpleErrorMonitorFactory implements ErrorMonitorFactory {
		private int maxValue;
		private int threshold; 
		
		public SimpleErrorMonitorFactory() {
			this(0, 10); // default
		}

		public SimpleErrorMonitorFactory(int maxValue, int simpleErrorThreshold) {
			this.maxValue = maxValue;
			this.threshold = simpleErrorThreshold;
		}

		@Override
		public ErrorMonitor createErrorMonitor() {
			return new SimpleErrorMonitorImpl(maxValue, threshold);
		}

		@Override
		public ErrorMonitor createErrorMonitor(int maxValue) {
			return new SimpleErrorMonitorImpl(maxValue, threshold);
		}

		// TODO add setter and keep error threshold in sync with maxConnsPerHost OR switch to error rate monitor
	}
}
