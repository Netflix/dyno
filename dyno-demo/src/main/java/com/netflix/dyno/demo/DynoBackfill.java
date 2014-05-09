package com.netflix.dyno.demo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import net.spy.memcached.MemcachedClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynoBackfill {

	private static final Logger Logger = LoggerFactory.getLogger(DynoBackfill.class);
	
	public DynoBackfill() {
		
	}
	
	public void backfill() {
		
		String className = "com.netflix.dyno.memcache.DynoMCacheBackfill";
		
		try {
			Class<?> clazz = Class.forName(className);
			Object obj = clazz.newInstance();
			obj.toString();

		} catch (Exception e) {
			//e.printStackTrace();
			Logger.error("FAILED", e);
		}
	}
	
	public void backfillDirectlyToMC() {
		try {
			final AtomicInteger count = new AtomicInteger(0);

			InetSocketAddress addr  = new InetSocketAddress("ec2-54-237-47-72.compute-1.amazonaws.com", 11211);
			MemcachedClient mc = new MemcachedClient(addr);

			for (int i=0; i<1000000; i++) {
				try {
					mc.set("test" + i, 360000, "Value_" + i).get();
					count.incrementAndGet();
					System.out.println("Count : " + count.get());

				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			mc.shutdown();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void backfill2() {
		
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
