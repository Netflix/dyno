package com.netflix.dyno.demo;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynoBackfill {

	private static final Logger Logger = LoggerFactory.getLogger(DynoBackfill.class);
	
	public DynoBackfill() {
		
	}
	
	public void backfill() {
		
		final int numThreads = 10; 
		final int numKeysPerThread = DemoConfig.NumKeys.get()/numThreads;
		
		ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
		
		final CountDownLatch latch = new CountDownLatch(numThreads);
		
		final AtomicInteger count = new AtomicInteger(0);
		
		for (int i=0; i<numThreads; i++) {
			
			final int threadId = i; 
			final int startKey = threadId*numKeysPerThread; 
			final int endKey = startKey + numKeysPerThread; 
			final DynoWorker worker = new DynoWorker();
			
			threadPool.submit(new Callable<Void>() {
				
				@Override
				public Void call() throws Exception {
					
					int k=startKey;
					
					while(k<endKey) {
						try {
							worker.write(""+k, SampleData.getInstance().getRandomValue());
							k++;
							count.incrementAndGet();
						} catch (Exception e) {
							Logger.error("Retrying after failure", e);
						}
					}
					
					worker.shutdown();
					latch.countDown();
					return null;
				}
			});
		}
		
		boolean done = false;
		
		while (!done) {
			
			Logger.info("Waiting on latch for 60 seconds..." + count.get());
			try {
				done = latch.await(60, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Logger.info("Interrupted returning");
				done = true;
			}
		}
		
		Logger.info("Latch is done");
		
		threadPool.shutdownNow();
	}
}
