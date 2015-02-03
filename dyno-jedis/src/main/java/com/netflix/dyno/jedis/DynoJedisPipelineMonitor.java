package com.netflix.dyno.jedis;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.netflix.dyno.connectionpool.impl.utils.EstimatedHistogram;
import com.netflix.dyno.contrib.EstimatedHistogramBasedCounter.EstimatedHistogramMean;
import com.netflix.dyno.contrib.EstimatedHistogramBasedCounter.EstimatedHistogramPercentile;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.BasicCounter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.tag.BasicTag;

public class DynoJedisPipelineMonitor {

	private final ConcurrentHashMap<String, BasicCounter> counterMap = new ConcurrentHashMap<String, BasicCounter>();
	private final String appName;
	private final BasicCounter pipelineSync; 
	private final BasicCounter pipelineDiscard; 
	private final PipelineTimer timer;
	
	public DynoJedisPipelineMonitor(String applicationName) {
		
		appName = applicationName;
		pipelineSync = getNewPipelineCounter("SYNC");
		pipelineDiscard = getNewPipelineCounter("DISCARD");
		timer = new PipelineTimer(appName);
	}
	
	public void init() {
		// register the counters
		DefaultMonitorRegistry.getInstance().register(pipelineSync);
		DefaultMonitorRegistry.getInstance().register(pipelineDiscard);
		// register the pipeline timer
		DefaultMonitorRegistry.getInstance().register(timer.latMean);
		DefaultMonitorRegistry.getInstance().register(timer.lat99);
		DefaultMonitorRegistry.getInstance().register(timer.lat995);
		DefaultMonitorRegistry.getInstance().register(timer.lat999);
	}
	
	public void recordOperation(String opName) {
		getOrCreateCounter(opName).increment();
	}

	public void recordPipelineSync() {
		pipelineSync.increment();
	}
	
	public void recordPipelineDiscard() {
		pipelineDiscard.increment();
	}

	public void recordLatency(long duration, TimeUnit unit) {
		timer.recordLatency(duration, unit);
	}
	
	private BasicCounter getOrCreateCounter(String opName) {
		
		BasicCounter counter = counterMap.get(opName);
		if (counter != null) {
			return counter;
		}
		counter = getNewPipelineCounter(opName);
		BasicCounter prevCounter = counterMap.putIfAbsent(opName, counter);
		if (prevCounter != null) {
			return prevCounter;
		}
		DefaultMonitorRegistry.getInstance().register(counter);
		return counter; 
	}
	
	private BasicCounter getNewPipelineCounter(String opName) {
		
		String metricName = "Dyno__" + appName + "__PL__" + opName;
		MonitorConfig config = MonitorConfig.builder(metricName)
							 				.withTag(new BasicTag("dyno_pl_op", opName))
							 				.build();
		return new BasicCounter(config);
	}
	
	private class PipelineTimer {
		
		private final EstimatedHistogramMean latMean; 
		private final EstimatedHistogramPercentile lat99;
		private final EstimatedHistogramPercentile lat995;
		private final EstimatedHistogramPercentile lat999;
		
		private final EstimatedHistogram estHistogram; 
		
		private PipelineTimer(String appName) {

			estHistogram = new EstimatedHistogram();
			latMean = new EstimatedHistogramMean("Dyno__" + appName + "__PL__latMean", "PL", "dyno_pl_op", estHistogram);
			lat99 = new EstimatedHistogramPercentile("Dyno__" + appName + "__PL__lat990", "PL", "dyno_pl_op", estHistogram, 0.99);
			lat995 = new EstimatedHistogramPercentile("Dyno__" + appName + "__PL__lat995", "PL", "dyno_pl_op", estHistogram, 0.995);
			lat999 = new EstimatedHistogramPercentile("Dyno__" + appName + "__PL__lat999", "PL", "dyno_pl_op", estHistogram, 0.999);
		}
		
		public void recordLatency(long duration, TimeUnit unit) {
			long durationMicros = TimeUnit.MICROSECONDS.convert(duration, unit);
			estHistogram.add(durationMicros);
		}
	}

}
