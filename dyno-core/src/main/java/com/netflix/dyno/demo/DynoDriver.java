package com.netflix.dyno.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;

import static com.netflix.dyno.demo.DemoConfig.*;

public class DynoDriver {

	private static final Logger Logger = LoggerFactory.getLogger(DynoDriver.class);
	
	private final List<DynoWorker> readWorkers = new ArrayList<DynoWorker>();
	private final List<DynoWorker> writeWorkers = new ArrayList<DynoWorker>();
	private final AtomicReference<ExecutorService> tpReadRef = new AtomicReference<ExecutorService>(null);
	private final AtomicReference<ExecutorService> tpWriteRef = new AtomicReference<ExecutorService>(null);
	
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
		
		Logger.info("Starting DynoDriver reads...");
		startOperation(ReadEnabled,
				       NumReaders, readWorkers,
				       tpReadRef,
				       new DynoReadOperation());
	}
	
	public void startWrites() {
		
		Logger.info("Starting DynoDriver writes...");
		startOperation(WriteEnabled,
				       NumWriters, writeWorkers,
				       tpWriteRef,
				       new DynoWriteOperation());
	}

	public void startOperation(DynamicBooleanProperty operationEnabled, 
							   DynamicIntProperty numWorkers, List<DynoWorker> workerList,
							   AtomicReference<ExecutorService> tpRef, 
							   final DynoOperation operation) {
		 
		if (!operationEnabled.get()) {
			Logger.info("Operation : " + operation.getClass().getSimpleName() + " not enabled, ignoring");
			return;
		}
		
		ExecutorService threadPool = Executors.newFixedThreadPool(numWorkers.get());
		 
		boolean success = tpRef.compareAndSet(null, threadPool);
		if (!success) {
			throw new RuntimeException("Unknown threadpool when performing tpRef CAS operation");
		}
		
		for (int i=0; i<numWorkers.get(); i++) {
			
			final DynoWorker worker = new DynoWorker();
			workerList.add(worker);
			
			final DynoStats stats = DynoStats.getInstance();
			
			threadPool.submit(new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					
					Thread thread = Thread.currentThread();
					while (!thread.isInterrupted() && !worker.isShutdown()) {
						operation.process(worker, stats);
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
		
		void process(DynoWorker worker, DynoStats stats); 
	}
	
	private class DynoReadOperation implements DynoOperation {

		@Override
		public void process(DynoWorker worker, DynoStats stats) {
			Long startTime = System.currentTimeMillis();
			try { 
				String value = worker.read(SampleData.getInstance().getRandomKey());
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
		public void process(DynoWorker worker, DynoStats stats) {
			Long startTime = System.currentTimeMillis();
			try { 
				String key = SampleData.getInstance().getRandomKey();
				String value = SampleData.getInstance().getRandomValue();
				worker.write(key, value);
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
	
	public void backfillData() {
		
		int tenPercent = NumKeys.get()/10;
		
		DynoWorker worker = new DynoWorker();
		for (int i=0; i<NumKeys.get(); i++) {
			
			String key = String.valueOf(i);
			String value = SampleData.getInstance().getRandomValue();
			worker.write(key, value);
			
			if (i % tenPercent == 0) {
				System.out.println("Backfill progress: " + i + " out of " + NumKeys.get());
				Logger.info("Backfill progress: " + i + " out of " + NumKeys.get());
			}
		}
		worker.shutdown();
	}
	
	public static void main(String args[]) {
		
		//DynoDriver.getInstance().backfillData();
		
		DynoDriver driver = new DynoDriver();
		
		driver.startReads();
		
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		driver.stop();
		
		System.out.println(DynoStats.getInstance().getStatus());
	}
}
