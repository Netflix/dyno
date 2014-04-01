package com.netflix.dyno.demo;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.monitor.Timer;

public class DynoStats {

	private static final DynoStats Instance = new DynoStats();
	
	private final AtomicInteger success = new AtomicInteger(0);
	private final AtomicInteger failure = new AtomicInteger(0);
	private final AtomicInteger cacheHits = new AtomicInteger(0);
	private final AtomicInteger cacheMiss = new AtomicInteger(0);

	private final Timer writeTimer = Monitors.newTimer("writeLatency", TimeUnit.MILLISECONDS);
	private final Timer readTimer = Monitors.newTimer("readLatency", TimeUnit.MILLISECONDS);

	public static DynoStats getInstance() {
		return Instance;
	}
	
	private DynoStats() {
		DefaultMonitorRegistry.getInstance().register(Monitors.newObjectMonitor(this));
	}
	
	public void success() {
		success.incrementAndGet();
	}

	@Monitor(name="success", type=DataSourceType.COUNTER)
	public int getSucces() {
		return success.get();
	}

	public void failure() {
		failure.incrementAndGet();
	}

	@Monitor(name="failure", type=DataSourceType.COUNTER)
	public int getFailure() {
		return success.get();
	}

	public void cacheHit() {
		cacheHits.incrementAndGet();
	}

	@Monitor(name="cacheHit", type=DataSourceType.COUNTER)
	public int getCacheHits() {
		return success.get();
	}

	public void cacheMiss() {
		cacheMiss.incrementAndGet();
	}
	
	@Monitor(name="cacheMiss", type=DataSourceType.COUNTER)
	public int getCacheMiss() {
		return success.get();
	}

	private double getCacheHitRatio() {
		int hits = cacheHits.get();
		int miss = cacheMiss.get();
		
		if (hits + miss == 0) {
			return 0;
		}
		
		return (double)((double)(hits*100)/(double)(hits+miss));
	}

	public void recordReadLatency(long duration) {
		readTimer.record(duration, TimeUnit.MILLISECONDS);
	}
	
	public void recordWriteLatency(long duration) {
		writeTimer.record(duration, TimeUnit.MILLISECONDS);
	}

	@Monitor(name="cacheHitRatio", type=DataSourceType.COUNTER)
	public int getCacheHitRatioInt() {
		return (int)getCacheHitRatio();
	}
	
	public String getStatus() {
		return "CacheStats: success: " + success.get() + " failure: " + failure.get() + 
				" hits: " + cacheHits.get() + " miss: " + cacheMiss.get() + " ratio: " + getCacheHitRatio() + "\n";
	}
	
	public String toString() {
		return getStatus();
	}
}
