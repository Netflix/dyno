package com.netflix.dyno.demo;

import static com.netflix.dyno.demo.DemoConfig.NumReaders;
import static com.netflix.dyno.demo.DemoConfig.NumWriters;
import static com.netflix.dyno.demo.DemoConfig.ReadEnabled;
import static com.netflix.dyno.demo.DemoConfig.WriteEnabled;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.dyno.memcache.DynoMCacheClient;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;

public class DynoDriver {

	private static final Logger Logger = LoggerFactory.getLogger(DynoDriver.class);
	
	private final List<DynoWorker> readWorkers = new ArrayList<DynoWorker>();
	private final List<DynoWorker> writeWorkers = new ArrayList<DynoWorker>();
	private final AtomicReference<ExecutorService> tpReadRef = new AtomicReference<ExecutorService>(null);
	private final AtomicReference<ExecutorService> tpWriteRef = new AtomicReference<ExecutorService>(null);
	
	private final AtomicBoolean readsStarted = new AtomicBoolean(false);
	private final AtomicBoolean writesStarted = new AtomicBoolean(false);
	
	public DynoDriver() {
		
		NumReaders.addCallback(new Runnable() {
			@Override
			public void run() {
				checkAndInitReads();
			}
		});

		NumWriters.addCallback(new Runnable() {
			@Override
			public void run() {
				checkAndInitWrites();
			}
		});
		
		DefaultMonitorRegistry.getInstance().register(Monitors.newObjectMonitor(new DynoDriverStats()));
	}
		
	public void checkAndInitReads() {
		
		if (readWorkers.size() != NumReaders.get()) {
			// First stop the old workers, if any
			stopReads();
		}
		
		startReads();
	}
	
	public void checkAndInitWrites() {
		
		if (writeWorkers.size() != NumWriters.get()) {
			// First stop the old workers, if any
			stopWrites();
		}
		
		startWrites();
	}

	/** FUNCTIONALITY FOR STARTING THE DYNO WORKERS */
	
	public void start() {
		Logger.info("Starting DynoDriver...");
		startWrites();
		startReads();
	}
	
	public void startReads() {
		
		if (readsStarted.get()) {
			Logger.info("Reads already started ... ignoring");
			return;
		}
		Logger.info("Starting DynoDriver reads...");
		startOperation(ReadEnabled,
				       NumReaders,
				       tpReadRef,
				       new DynoReadOperation());
		readsStarted.set(true);
	}
	
	public void startWrites() {
		
		if (writesStarted.get()) {
			Logger.info("Writes already started ... ignoring");
			return;
		}

		Logger.info("Starting DynoDriver writes...");
		startOperation(WriteEnabled,
				       NumWriters,
				       tpWriteRef,
				       new DynoWriteOperation());
		
		writesStarted.set(true);
	}

	public void startOperation(DynamicBooleanProperty operationEnabled, 
							   DynamicIntProperty numWorkers,
							   AtomicReference<ExecutorService> tpRef, 
							   final DynoOperation operation) {
		 
		if (!operationEnabled.get()) {
			Logger.info("Operation : " + operation.getClass().getSimpleName() + " not enabled, ignoring");
			return;
		}
		
		int totalWorkerPoolSize = numWorkers.get();
		ExecutorService threadPool = Executors.newFixedThreadPool(totalWorkerPoolSize);
		 
		boolean success = tpRef.compareAndSet(null, threadPool);
		if (!success) {
			throw new RuntimeException("Unknown threadpool when performing tpRef CAS operation");
		}
		
		final DynoStats stats = DynoStats.getInstance();
		final DynoMCacheClient client = DynoClientHolder.getInstance().get();
		
		for (int i=0; i<numWorkers.get(); i++) {
			
			threadPool.submit(new Callable<Void>() {

				@Override
				public Void call() throws Exception {

					Thread thread = Thread.currentThread();
					while (!thread.isInterrupted()) {
						operation.process(client, stats);
						//Thread.sleep(1000);
					}
					Logger.info("DynoWorker shutting down");
					return null;
				}
			});
		}
		
	}

	/** FUNCTIONALITY FOR STOPPING THE DYNO WORKERS */
	public void stop() {
		stopWrites();
		stopReads();
	}
	
	public void stopReads() {
		stopOperation(readWorkers, tpReadRef);
	}
	
	public void stopWrites() {
		stopOperation(writeWorkers, tpWriteRef);
	}
	
	public void stopOperation(List<DynoWorker> listWorkers, AtomicReference<ExecutorService> tpRef) {
		
		for (DynoWorker worker : listWorkers) {
			worker.shutdown();
		}
		listWorkers.clear();
		
		ExecutorService tp = tpRef.get();
		if (tp != null) {
			tp.shutdownNow();
			tpRef.set(null);
		}
	}

	private interface DynoOperation { 
		
		void process(DynoMCacheClient dynoClient, DynoStats stats); 
	}
	
	private class DynoReadOperation implements DynoOperation {

		@Override
		public void process(DynoMCacheClient dynoClient, DynoStats stats) {
			Long startTime = System.currentTimeMillis();
			try { 
				String value = (String) dynoClient.get(SampleData.getInstance().getRandomKey()).getResult();
				if (value != null) {
					stats.cacheHit();
				} else {
					stats.cacheMiss();
				}
				stats.success();
			} catch (Exception e) {
				stats.failure();
				Logger.error("Failed to process dyno read operation", e);
			} finally {
				stats.recordReadLatency(System.currentTimeMillis() - startTime);
			}
		}
	}
	
	private class DynoWriteOperation implements DynoOperation {

		@Override
		public void process(DynoMCacheClient dynoClient, DynoStats stats) {
			Long startTime = System.currentTimeMillis();
			try { 
				String key = SampleData.getInstance().getRandomKey();
				String value = SampleData.getInstance().getRandomValue();
				
				dynoClient.set(key, value);
				stats.success();
			} catch (Exception e) {
				stats.failure();
				Logger.error("Failed to process dyno write operation", e);
			} finally {
				stats.recordWriteLatency(System.currentTimeMillis() - startTime);
			}
		}
	}
	
	public String getStatus() {
		return "NumReaders: " + readWorkers.size() + " NumWriters: " + writeWorkers.size() + "\n" + 
				DynoStats.getInstance().getStatus();
	}
	
	public String toString() {
		return getStatus();
	}

	public void init() {
		DynoClientHolder.getInstance().get();
	}
	
	public String readSingle(String key) {
		
		DynoMCacheClient client = DynoClientHolder.getInstance().get();
		return client.<String>get(key).getResult();
	}
	

	public String writeSingle(String key, String value) {
		
		DynoMCacheClient client = DynoClientHolder.getInstance().get();
		client.set(key, value);
		return "done";
	}
	
	
	class DynoDriverStats {
		
		@Monitor(name="readers", type=DataSourceType.COUNTER)
		public int getNumReaders() {
			return readWorkers.size();
		}
		
		@Monitor(name="writers", type=DataSourceType.COUNTER)
		public int getNumWriters() {
			return writeWorkers.size();
		}
	}
	
	
	
//	public static void main(String args[]) {
//		
//		//DynoDriver.getInstance().backfillData();
//		
//		DynoDriver driver = new DynoDriver();
//		
//		driver.startReads();
//		
//		try {
//			Thread.sleep(10000);
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		
//		driver.stop();
//		
//		System.out.println(DynoStats.getInstance().getStatus());
//	}
}
