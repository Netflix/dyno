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
	
		final DynoMCacheClient client = DynoClientHolder.getInstance().get();
		
		final int numThreads = 10; 
		final int numKeysPerThread = DemoConfig.NumKeys.get()/numThreads;
		
		ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
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
							client.set(""+k, SampleData.getInstance().getRandomValue());
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
			
			Logger.info("Backfiller waiting on latch");
			latch.await();
			Logger.info("Backfiller latch done!");
			
			threadPool.shutdownNow();
		}
		
//		ExecutorService thPool = Executors.newFixedThreadPool(1);
//		
//		Future<Void> f = thPool.submit(new Callable<Void>() {
//
//			@Override
//			public Void call() throws Exception {
//				
//				boolean done = false;
//				while (!done) {
//					System.out.println("Count so far " + count.get());
//					Thread.sleep(1000);
//					if (count.get() >= 1000000) {
//						done = true;
//					}
//				}
//				return null;
//			}
//		});

//		try {
//			for (int i=0; i<1000000; i++) {
//				client.set("test" + i, "value_" + i).get();
//				count.incrementAndGet();
//				//System.out.println("Count : " + count.get());
//			}
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
		
//		try {
//			f.get();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}

//			thPool.shutdownNow();
//			pool.shutdown();

	}
}
