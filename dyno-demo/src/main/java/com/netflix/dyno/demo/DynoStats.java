package com.netflix.dyno.demo;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;

public class DynoStats {

	private static final DynoStats Instance = new DynoStats();
	
	private final AtomicLong success = new AtomicLong(0L);
	private final AtomicLong failure = new AtomicLong(0L);
	private final AtomicLong cacheHits = new AtomicLong(0L);
	private final AtomicLong cacheMiss = new AtomicLong(0L);

//	private final Timer writeTimer = Monitors.newTimer("writeLatency", TimeUnit.MILLISECONDS);
//	private final Timer readTimer = Monitors.newTimer("readLatency", TimeUnit.MILLISECONDS);

	private final DynoTimer writeTimer = new DynoTimer(1000, 20, 1200);
	private final DynoTimer readTimer = new DynoTimer(1000, 20, 1200);

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
	public long getSucces() {
		return success.get();
	}

	public void failure() {
		failure.incrementAndGet();
	}

	@Monitor(name="failure", type=DataSourceType.COUNTER)
	public long getFailure() {
		return failure.get();
	}

	public void cacheHit() {
		cacheHits.incrementAndGet();
	}

	@Monitor(name="cacheHit", type=DataSourceType.COUNTER)
	public long getCacheHits() {
		return cacheHits.get();
	}

	public void cacheMiss() {
		cacheMiss.incrementAndGet();
	}
	
	@Monitor(name="cacheMiss", type=DataSourceType.COUNTER)
	public long getCacheMiss() {
		return cacheMiss.get();
	}

	@Monitor(name="readLatAvg", type=DataSourceType.GAUGE)
	public double getReadLatAvg() {
		return readTimer.getAvgMillis();
	}

	@Monitor(name="readLatP50", type=DataSourceType.GAUGE)
	public double getReadLatP50() {
		return readTimer.getP50Millis();
	}

	@Monitor(name="readLatP99", type=DataSourceType.GAUGE)
	public double getReadLatP99() {
		return readTimer.getP99Millis();
	}

	@Monitor(name="readLatP995", type=DataSourceType.GAUGE)
	public double getReadLatP995() {
		return readTimer.getP995Millis();
	}

	@Monitor(name="readLatP999", type=DataSourceType.GAUGE)
	public double getReadLatP999() {
		return readTimer.getP999Millis();
	}

	
	@Monitor(name="writeLatAvg", type=DataSourceType.GAUGE)
	public double getWriteLatAvg() {
		return writeTimer.getAvgMillis();
	}

	@Monitor(name="writeLatP50", type=DataSourceType.GAUGE)
	public double getWriteLatP50() {
		return writeTimer.getP50Millis();
	}

	@Monitor(name="writeLatP99", type=DataSourceType.GAUGE)
	public double getWriteLatP99() {
		return writeTimer.getP99Millis();
	}

	@Monitor(name="writeLatP995", type=DataSourceType.GAUGE)
	public double getWriteLatP995() {
		return writeTimer.getP995Millis();
	}

	@Monitor(name="writeLatP999", type=DataSourceType.GAUGE)
	public double getWriteLatP999() {
		return writeTimer.getP999Millis();
	}

	private float getCacheHitRatio() {
		long hits = cacheHits.get();
		long miss = cacheMiss.get();
		
		if (hits + miss == 0) {
			return 0;
		}
		
		return (float)((float)(hits*100L)/(float)(hits+miss));
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
