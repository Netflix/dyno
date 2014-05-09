package com.netflix.dyno.connectionpool;

import java.util.concurrent.TimeUnit;

public interface OperationMonitor {

	public void recordLatency(String opName, long duration, TimeUnit unit);
	
	public void recordSuccess(String opName);

	public void recordFailure(String opName, String reason);
}
