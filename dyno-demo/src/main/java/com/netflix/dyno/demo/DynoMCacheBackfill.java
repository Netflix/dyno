package com.netflix.dyno.demo;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.memcache.DynoMCacheClient;

public class DynoMCacheBackfill {

	private static final Logger Logger = LoggerFactory.getLogger(DynoMCacheBackfill.class);
	
	public DynoMCacheBackfill() {
		
	}
	
	
	public void backfill() throws Exception {
		
		final long start = System.currentTimeMillis();
	
		final DynoMCacheClient client = DynoClientHolder.getInstance().get();
		//final MemcachedClient client = DynoClientHolder.getInstance().get();
		
		final int numThreads = 10; 
		final int numKeysPerThread = DemoConfig.NumKeys.get()/numThreads;
		
		System.out.println("NUM KEYS: " + DemoConfig.NumKeys.get());
		System.out.println("NUM KEYS PER TH: " + numKeysPerThread);
		
		ExecutorService threadPool = Executors.newFixedThreadPool(numThreads + 1);
		final CountDownLatch latch = new CountDownLatch(numThreads);
		final AtomicInteger count = new AtomicInteger(0);
		
		
		for (int i=0; i<numThreads; i++) {
			
			final int threadId = i; 
			final int startKey = threadId*numKeysPerThread; 
			final int endKey = startKey + numKeysPerThread; 
			
			threadPool.submit(new Callable<Void>() {
				
				@Override
				public Void call() throws Exception {
					
					int k=startKey;
					
					while(k<endKey) {
						try {
							client.set(""+k, SampleData.getInstance().getRandomValue()).get();
							//client.set(""+k, 0, SampleData.getInstance().getRandomValue()).get();
							k++;
							count.incrementAndGet();
						} catch (Exception e) {
							Logger.error("Retrying after failure", e);
						}
					}
					
					latch.countDown();
					return null;
				}
			});
		}
			
		threadPool.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {

				while(!Thread.currentThread().isInterrupted()) {
					System.out.println("Count so far: " + count.get());
					Thread.sleep(5000);
				}
				return null;
			}
		});

		Logger.info("Backfiller waiting on latch");
		latch.await();
		Logger.info("Backfiller latch done! in " + (System.currentTimeMillis()-start) + " ms");

		threadPool.shutdownNow();
	}
}
