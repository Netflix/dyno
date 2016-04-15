package com.netflix.dyno.connectionpool.impl.health;

import java.util.concurrent.atomic.AtomicInteger;

public class SimpleErrorMonitorImpl implements ErrorMonitor {

	private final AtomicInteger errorCount = new AtomicInteger(0);
	private final int threshold;
	
	public SimpleErrorMonitorImpl(int numErrorThreshold) {
		threshold = numErrorThreshold;
	}
	
	@Override
	public boolean trackError(int numErrors) {
		
		int currentCount = errorCount.addAndGet(numErrors);
		if (currentCount >= threshold) {
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

		private int threshold; 
		
		public SimpleErrorMonitorFactory() {
			this(10); // default
		}
		
		public SimpleErrorMonitorFactory(int simpleErrorThreshold) {
			threshold = simpleErrorThreshold;
		}
		
		@Override
		public ErrorMonitor createErrorMonitor() {
			return new SimpleErrorMonitorImpl(threshold);
		}

		// TODO add setter and keep error threshold in sync with maxConnsPerHost OR switch to error rate monitor
	}
}
