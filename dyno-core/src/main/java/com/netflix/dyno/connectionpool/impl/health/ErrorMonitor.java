package com.netflix.dyno.connectionpool.impl.health;


public interface ErrorMonitor {
	
	/**
	 * Monitor errors
	 * @param numErrors
	 * @return true/false indicating whether the error are within the threshold. 
	 *         True: Errors still ok. False: errors have crossed the threshold
	 */
	boolean trackError(int numErrors);

	
	public static interface ErrorMonitorFactory {
		
		public ErrorMonitor createErrorMonitor();
	}
}
