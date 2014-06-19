package com.netflix.dyno.demo;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;

public class DynoStats {

	private static final DynoStats Instance = new DynoStats();
	
	private final AtomicLong readSuccess = new AtomicLong(0L);
	private final AtomicLong readFailure = new AtomicLong(0L);
	private final AtomicLong writeSuccess = new AtomicLong(0L);
	private final AtomicLong writeFailure = new AtomicLong(0L);
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
	
	public void readSuccess() {
		readSuccess.incrementAndGet();
	}
	public void readFailure() {
		readFailure.incrementAndGet();
	}
	public void writeSuccess() {
		writeSuccess.incrementAndGet();
	}
	public void writeFailure() {
		writeFailure.incrementAndGet();
	}
	
	@Monitor(name="readSuccess", type=DataSourceType.COUNTER)
	public long getReadSuccess() {
		return readSuccess.get();
	}

	@Monitor(name="readFailure", type=DataSourceType.COUNTER)
	public long getReadFailure() {
		return readFailure.get();
	}

	@Monitor(name="writeSuccess", type=DataSourceType.COUNTER)
	public long getWriteSuccess() {
		return writeSuccess.get();
	}

	@Monitor(name="writeFailure", type=DataSourceType.COUNTER)
	public long getWriteFailure() {
		return writeFailure.get();
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
	public long getReadLatAvg() {
		return (long)(readTimer.getAvgMillis()*1000);
	}

	@Monitor(name="readLatP50", type=DataSourceType.GAUGE)
	public long getReadLatP50() {
		return (long)(readTimer.getP50Millis()*1000);
	}

	@Monitor(name="readLatP99", type=DataSourceType.GAUGE)
	public long getReadLatP99() {
		return (long)(readTimer.getP99Millis()*1000);
	}

	@Monitor(name="readLatP995", type=DataSourceType.GAUGE)
	public long getReadLatP995() {
		return (long)(readTimer.getP995Millis()*1000);
	}

	@Monitor(name="readLatP999", type=DataSourceType.GAUGE)
	public long getReadLatP999() {
		return (long)(readTimer.getP999Millis()*1000);
	}

	
	@Monitor(name="writeLatAvg", type=DataSourceType.GAUGE)
	public long getWriteLatAvg() {
		return (long)(writeTimer.getAvgMillis()*1000);
	}

	@Monitor(name="writeLatP50", type=DataSourceType.GAUGE)
	public long getWriteLatP50() {
		return (long)(writeTimer.getP50Millis()*1000);
	}

	@Monitor(name="writeLatP99", type=DataSourceType.GAUGE)
	public long getWriteLatP99() {
		return (long)(writeTimer.getP99Millis()*1000);
	}

	@Monitor(name="writeLatP995", type=DataSourceType.GAUGE)
	public long getWriteLatP995() {
		return (long)(writeTimer.getP995Millis()*1000);
	}

	@Monitor(name="writeLatP999", type=DataSourceType.GAUGE)
	public long getWriteLatP999() {
		return (long)(writeTimer.getP999Millis()*1000);
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
		return "CacheStats: success: " + readSuccess.get() + " failure: " + readFailure.get() + 
				" hits: " + cacheHits.get() + " miss: " + cacheMiss.get() + " ratio: " + getCacheHitRatio() + "\n";
	}
	
	public String toString() {
		return getStatus();
	}
}
