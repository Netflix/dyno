package com.netflix.dyno.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.demo.DynoDriver.DynoClient;
import com.netflix.dyno.demo.redis.DynoRedisDriver;

public class DynoDataBackfill {

	private static final Logger Logger = LoggerFactory.getLogger(DynoDataBackfill.class);
	
	private final DynoClient client;
	
	public static final DynoDataBackfill Instance = new DynoDataBackfill(DynoRedisDriver.getInstance().getDynoClient());
	
	private final AtomicBoolean stop = new AtomicBoolean(false);
	
	private ExecutorService threadPool = null;
	
	private DynoDataBackfill(DynoClient client) {
		this.client = client;
	}
	
	
	public void randomWrites() throws Exception {
		
		stop.set(false);
		final Random random = new Random();
		final AtomicInteger totalOps = new AtomicInteger(0);
		final AtomicInteger success = new AtomicInteger(0);
		final AtomicInteger failure = new AtomicInteger(0);
		
		int numThreads = DemoConfig.NumReaders.get(); 
		threadPool = Executors.newFixedThreadPool(numThreads + 1);
		
		final String value1 = "dcfa7d0973834e5c9f480b65de19d684dcfa7d097383dcfa7d0973834e5c9f480b65de19d684dcfa7d097383dcfa7d0973834e5c9f480b65de19d684dcfa7d097383dcfa7d0973834e5c9f480b65de19d684dcfa7d097383";
		final String value = value1 + value1 + value1 + value1 + value1;
		
		final List<String> list = new ArrayList<String>();
		int totalCount = 100000;
		
		for (int i=0; i<totalCount; i++) {
			list.add("T"+i);
		}
		
		System.out.println("\n\nNum Threads: " + numThreads + ", KEYS: " + list.size() + ", Payload Size: " + value.length() + "\n\n");
		
		//String[] v = {"T1", "T2", "T3", "T4", "T5", "T10"};
		//list.addAll(Arrays.asList(v));
		
		for (int i=0; i<numThreads; i++) {
			
			threadPool.submit(new Callable<Void>() {
				
				@Override
				public Void call() throws Exception {
					
					final int n = list.size();
					while(!stop.get()) {
						try {
							
							int index = random.nextInt(n);
							String k = list.get(index);
							
							//client.set(k,value);
							client.get(k);
							success.incrementAndGet();	
							failure.incrementAndGet();
							
						} catch (Exception e) {
							Logger.error("Retrying after failure", e);
						} finally {
							totalOps.incrementAndGet();	
						}
					}
					
//					for (String k: list) {
//						System.out.println("Key: " + k);
//						client.set(k,value);
//						theCount.incrementAndGet();
//					}
					
					System.out.println("\n\nStopping RANDOM WRITES\n\n");
					return null;
				}
			});
		}
		
		
		threadPool.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {

				final AtomicInteger prevCount = new AtomicInteger(0);
				
				while(!Thread.currentThread().isInterrupted() && !stop.get()) {
					int count = totalOps.get();
					System.out.println("Success: " + success.get()  + ", failure: " + failure.get() + ",  RPS: " + ((count - prevCount.get())/5));
					prevCount.set(count);
					Thread.sleep(5000);
				}
				System.out.println("\n\nStopping datafill status poller\n\n");
				return null;
			}
		});

	}
	
	public void backfill() throws Exception {
		
		stop.set(false);
		
		final long start = System.currentTimeMillis();
	
		final int numThreads = DemoConfig.NumBackfill.get(); 
		final int numKeysPerThread = DemoConfig.NumKeys.get()/numThreads;
		
		System.out.println("NUM THREADS: " + numThreads);
		System.out.println("NUM KEYS: " + DemoConfig.NumKeys.get());
		System.out.println("NUM KEYS PER TH: " + numKeysPerThread);
		
		threadPool = Executors.newFixedThreadPool(numThreads + 1);
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
					
					while(k<endKey && !stop.get()) {
						try {
							String key = "T" + k;
							//System.out.println("key: " + key);
							client.set(key, SampleData.getInstance().getRandomValue());
							k++;
							count.incrementAndGet();
						} catch (Exception e) {
							Logger.error("Retrying after failure", e);
						}
					}
					
					latch.countDown();
					
					System.out.println("\n\nStopping datafill writer\n\n");
					return null;
				}
			});
		}
			
		threadPool.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {

				while(!Thread.currentThread().isInterrupted() && !stop.get()) {
					System.out.println("Count so far: " + count.get());
					Thread.sleep(5000);
				}
				System.out.println("\n\nStopping datafill status poller\n\n");
				return null;
			}
		});

		Logger.info("Backfiller waiting on latch");
		latch.await();
		Logger.info("Backfiller latch done! in " + (System.currentTimeMillis()-start) + " ms");
	}
	
	public void stopBackfill() {
		stop.set(true);;
	}
	
	public void shutdown() {
		if (threadPool != null) {
			threadPool.shutdownNow();
		}
	}
	
}
