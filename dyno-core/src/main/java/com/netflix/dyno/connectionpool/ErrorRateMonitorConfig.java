package com.netflix.dyno.connectionpool;

import java.util.List;

public interface ErrorRateMonitorConfig {

	public int getWindowSizeSeconds();
	
	public int getCheckFrequencySeconds();
	
	public int getCheckSuppressWindowSeconds();
	
	public List<ErrorThreshold> getThresholds();
	
	public interface ErrorThreshold {
		
		public int getThresholdPerSecond(); 
		
		public int getWindowSeconds();
		
		public int getWindowCoveragePercentage();
	}
	
}
