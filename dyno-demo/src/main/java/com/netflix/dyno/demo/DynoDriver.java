package com.netflix.dyno.demo;

import static com.netflix.dyno.demo.DemoConfig.NumReaders;
import static com.netflix.dyno.demo.DemoConfig.NumWriters;
import static com.netflix.dyno.demo.DemoConfig.ReadEnabled;
import static com.netflix.dyno.demo.DemoConfig.WriteEnabled;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;

public abstract class DynoDriver {

	private static final Logger Logger = LoggerFactory.getLogger(DynoDriver.class);
	
	private final AtomicInteger readWorkers = new AtomicInteger(0);
	private final AtomicInteger writeWorkers = new AtomicInteger(0);

	private final AtomicReference<ExecutorService> tpReadRef = new AtomicReference<ExecutorService>(null);
	private final AtomicReference<ExecutorService> tpWriteRef = new AtomicReference<ExecutorService>(null);
	
	private final AtomicBoolean readsStarted = new AtomicBoolean(false);
	private final AtomicBoolean writesStarted = new AtomicBoolean(false);
	
	protected DynoDriver() {
		
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
		
		if (readWorkers.get() != NumReaders.get()) {
			// First stop the old workers, if any
			stopReads();
			startReadsInternal();
		}
	}
	
	public void checkAndInitWrites() {
		
		if (writeWorkers.get() != NumWriters.get()) {
			// First stop the old workers, if any
			stopWrites();
			startWritesInternal();
		}
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
		startReadsInternal();
	}
	
	private void startReadsInternal() {
		
		Logger.info("Starting DynoDriver reads...");
		startOperation(ReadEnabled,
				       NumReaders,
				       readWorkers,
				       tpReadRef,
				       new DynoReadOperation());
		readsStarted.set(true);
	}
	
	public void startWrites() {
		
		if (writesStarted.get()) {
			Logger.info("Writes already started ... ignoring");
			return;
		}

		startWritesInternal();
	}

	private void startWritesInternal() {
		
		Logger.info("Starting DynoDriver writes...");
		startOperation(WriteEnabled,
				       NumWriters,
				       writeWorkers,
				       tpWriteRef,
				       new DynoWriteOperation());
		
		writesStarted.set(true);
	}

	public void startOperation(DynamicBooleanProperty operationEnabled, 
							   DynamicIntProperty numWorkersConfig,
							   AtomicInteger numWorkers,
							   AtomicReference<ExecutorService> tpRef, 
							   final DynoOperation operation) {
		 
		if (!operationEnabled.get()) {
			Logger.info("Operation : " + operation.getClass().getSimpleName() + " not enabled, ignoring");
			return;
		}
		
		int totalWorkerPoolSize = numWorkersConfig.get();
		ExecutorService threadPool = Executors.newFixedThreadPool(totalWorkerPoolSize);
		 
		boolean success = tpRef.compareAndSet(null, threadPool);
		if (!success) {
			throw new RuntimeException("Unknown threadpool when performing tpRef CAS operation");
		}
		
		final DynoStats stats = DynoStats.getInstance();
		
		System.out.println("\n\nStarting with threads: " + numWorkersConfig.get() + "\n\n");
		
		for (int i=0; i<numWorkersConfig.get(); i++) {
			
			threadPool.submit(new Callable<Void>() {

				@Override
				public Void call() throws Exception {

					Thread thread = Thread.currentThread();
					while (!thread.isInterrupted()) {
						operation.process(stats);
						//Thread.sleep(1000);
					}
					Logger.info("DynoWorker shutting down");
					return null;
				}
			});
			numWorkers.incrementAndGet();
		}
		
	}

	/** FUNCTIONALITY FOR STOPPING THE DYNO WORKERS */
	public void stop() {
		stopWrites();
		stopReads();
	}
	
	public void stopReads() {
		stopOperation(tpReadRef);
	}
	
	public void stopWrites() {
		stopOperation(tpWriteRef);
	}
	
	public void stopOperation(AtomicReference<ExecutorService> tpRef) {
		
		ExecutorService tp = tpRef.get();
		if (tp != null) {
			tp.shutdownNow();
			tpRef.set(null);
		}
		
		while(!tp.isShutdown()) {
			try {
				Logger.info("Waiting for worker pool to stop, sleeping for 1 sec");
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}
	}

	private interface DynoOperation { 
		
		boolean process(DynoStats stats); 
	}
	
	public interface DynoClient {
		
		void init();
		String get(String key) throws Exception;
		void set(String key, String value);
	}
	
	public abstract DynoClient getDynoClient();
	
	private class DynoReadOperation implements DynoOperation {

		@Override
		public boolean process(DynoStats stats) {
			Long startTime = System.currentTimeMillis();
			try { 
				String key = SampleData.getInstance().getRandomKey();
				String value = (String) getDynoClient().get(key);
				if (value != null) {
					stats.cacheHit();
				} else {
					System.out.println("Miss for key: " + key);
					stats.cacheMiss();
				}
				stats.success();
				return true;
			} catch (Exception e) {
				stats.failure();
				Logger.error("Failed to process dyno read operation", e);
				return false;
			} finally {
				stats.recordReadLatency(System.currentTimeMillis() - startTime);
			}
		}
	}
	
	private class DynoWriteOperation implements DynoOperation {

		@Override
		public boolean process(DynoStats stats) {
			Long startTime = System.currentTimeMillis();
			try { 
				String key = SampleData.getInstance().getRandomKey();
				String value = SampleData.getInstance().getRandomValue();
				
				getDynoClient().set(key, value);
				stats.success();
				return true;
			} catch (Exception e) {
				stats.failure();
				Logger.error("Failed to process dyno write operation", e);
				return false;
			} finally {
				stats.recordWriteLatency(System.currentTimeMillis() - startTime);
			}
		}
	}
	
	public String getStatus() {
		return "NumReaders: " + readWorkers.get() + " NumWriters: " + writeWorkers.get() + "\n" + 
				DynoStats.getInstance().getStatus();
	}
	
	public String toString() {
		return getStatus();
	}

	public void init() {
		getDynoClient().init();
	}
	
	public String readSingle(String key) throws Exception {
		return getDynoClient().get(key);
	}
	

	public String writeSingle(String key, String value) {
		getDynoClient().set(key, value);
		return "done";
	}
	
	
	class DynoDriverStats {
		
		@Monitor(name="readers", type=DataSourceType.COUNTER)
		public int getNumReaders() {
			return readWorkers.get();
		}
		
		@Monitor(name="writers", type=DataSourceType.COUNTER)
		public int getNumWriters() {
			return writeWorkers.get();
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
