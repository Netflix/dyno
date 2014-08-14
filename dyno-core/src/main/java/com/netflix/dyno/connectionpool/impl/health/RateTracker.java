/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.dyno.connectionpool.impl.health;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.impl.utils.RateLimitUtil;

/**
 * Class that tracks the rate at which events occur over a specified rolling time window (in seconds)
 * This is useful for tracking error rates from {@link ErrorRateMonitor}
 * 
 * @author poberai
 *
 */
public class RateTracker {
	
	private final AtomicReference<BucketCreator> bucketCreateLock = new AtomicReference<BucketCreator>(null);
	private final AtomicInteger wonLock = new AtomicInteger(0);

	private final RollingWindow rWindow;
	
	public RateTracker(int numSeconds) {
		
		int windowSize = numSeconds; 
		rWindow = new RollingWindow(windowSize);
	}

	public void trackRate() {
		trackRate(1);
	}
	
	public void trackRate(int count) {
	
		long currentTime = System.currentTimeMillis() / 1000;  // the current second
		
		// compare the current window
		int compare = rWindow.compareWindow(currentTime);
		
		if (compare == 0) {
			// it is the same window, increment the quota and check the rate for this second
			rWindow.trackRate(count);
			
		} else if (compare < 0) {
			
			// the current window that is tracked is in the past, so create the window for this second
			// it does not matter if some other thread beat us to setting the last bucket. 
			
			BucketCreator expected = bucketCreateLock.get();
			BucketCreator newCreator = new BucketCreator(currentTime);
			boolean success = bucketCreateLock.compareAndSet(expected, newCreator) ;
			
			if (success) {
				
				wonLock.incrementAndGet();
				newCreator.futureBucket.run();
				
			} else {
				
				try {
					bucketCreateLock.get().futureBucket.get(20, TimeUnit.MILLISECONDS);
				} catch (TimeoutException e) {
					//return true;
					e.printStackTrace();
				} catch (Exception e) {
					throw new DynoException(e);
				}
			}
			
			rWindow.trackRate(count);

			
		} else {
			// it is the prev window, let the request through
			return ;
		}
	}
	
	
	public List<Bucket> getBuckets(int lookback) {
		return rWindow.getBuckets(lookback);
	}
	

	public List<Bucket> getAllBuckets() {
		return rWindow.getAllBuckets();
	}
	
	private class RollingWindow { 
		
		private final int windowSize;
		
		private final LinkedBlockingDeque<Bucket> queue = new LinkedBlockingDeque<Bucket>();
		private final AtomicInteger bucketCreateCount = new AtomicInteger(0);

		private RollingWindow(int wSize) {
			
			windowSize = wSize;
			
			long currentTime = System.currentTimeMillis()/1000;
			long startTime = currentTime - windowSize + 1;
			
			for (long i = startTime; i<=currentTime; i++) {
				queue.addFirst(new Bucket(i));
			}
		}
		
		private void trackRate(int count) {
			queue.peekFirst().track(count);
		}
		
		private List<Bucket> getBuckets(int lookback) {
			
			List<Bucket> list = new ArrayList<Bucket>();
			int count = 0;
			Iterator<Bucket> iter = queue.iterator();
			
			while (iter.hasNext() && count < lookback) {
				list.add(iter.next());
				count++;
			}
			return list;
		}
		
		private List<Bucket> getAllBuckets() {
			
			List<Bucket> list = new ArrayList<Bucket>();
			Iterator<Bucket> iter = queue.iterator();
			
			while (iter.hasNext()) {
				list.add(iter.next());
			}
			return list;
		}
		
		private int compareWindow(long currentTimestamp) {
			
			Long lastBucketTimestamp = queue.peekFirst().lastTimestamp.get();
			return lastBucketTimestamp.compareTo(currentTimestamp);
		}
		
		private void addNewBucket(long timestamp) {
			
			bucketCreateCount.incrementAndGet();
			
			Bucket  newBucket = new Bucket(timestamp);
			queue.removeLast();
			queue.addFirst(newBucket);
		}
		
		private void syncToNewWindow(long timestamp) {
			
			long currentTimestamp = queue.peekFirst().lastTimestamp.get();
			
			if (currentTimestamp == timestamp) {
				return;
			}
			
			while (currentTimestamp < timestamp) {
				currentTimestamp++;
				addNewBucket(currentTimestamp);
			}
		}

		public Bucket firstBucket() {
			return queue.peekFirst();
		}
	}
	
	public static class Bucket { 
		
		private final AtomicLong lastTimestamp = new AtomicLong(0L);
		private final AtomicInteger count = new AtomicInteger(0);
		
		public Bucket() {
			this(System.currentTimeMillis()/1000); // the current second
		}
		
		private Bucket(long timestamp) {
			lastTimestamp.set(timestamp); // the current second
		}

		public int track(int delta) {
			return count.addAndGet(delta);
		}

		public int count() {
			return count.get();
		}
		
		public long timestamp() {
			return lastTimestamp.get();
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + new Long(lastTimestamp.get()).intValue();
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			
			Bucket other = (Bucket) obj;
			return this.lastTimestamp.get() == other.lastTimestamp.get();
		}
		
		public String toString() {
			return "" + this.count();
		}
	}
	
	private class BucketCreator {
		
		private final String id = UUID.randomUUID().toString();
		private final long timestamp;
		private final FutureTask<Bucket> futureBucket; 
		
		private BucketCreator(long time) {
			this.timestamp = time;
			
			this.futureBucket  = new FutureTask<Bucket>(new Callable<Bucket>() {

				@Override
				public Bucket call() throws Exception {
					rWindow.syncToNewWindow(timestamp);
					return rWindow.firstBucket();
				}
				
			}); 
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			BucketCreator other = (BucketCreator) obj;
			boolean equals = true;
			equals &=  (id != null) ? id.equals(other.id) : other.id == null;
			equals &=  (timestamp == other.timestamp);
			return equals;
		}
		
	}
	
	
	public static class UnitTest {
		
		@Test
		public void testProcess() throws Exception {
			
			final RateTracker tracker = new RateTracker(20);

			int numThreads = 5; 
			ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
			
			final AtomicReference<RateLimitUtil> limiter = new AtomicReference<RateLimitUtil>(RateLimitUtil.create(100));
			
			final AtomicBoolean stop = new AtomicBoolean(false);
			
			// stats
			final AtomicInteger totalOps = new AtomicInteger(0);
			
			final CyclicBarrier barrier = new CyclicBarrier(numThreads+1);
			final CountDownLatch latch = new CountDownLatch(numThreads);
			
			for (int i=0; i<numThreads; i++) {
				
				threadPool.submit(new Callable<Void>() {

					@Override
					public Void call() throws Exception {
						
						barrier.await();
						while (!stop.get() && !Thread.currentThread().isInterrupted()) {
							if (limiter.get().acquire()){
								tracker.trackRate(1);
								totalOps.incrementAndGet();
							}
						}
						latch.countDown();
						return null;
					}
				});
			}

			barrier.await();

			Thread.sleep(4000);
			System.out.println("Changing rate to 120");
			limiter.set(RateLimitUtil.create(120));
			
			Thread.sleep(4000);
			System.out.println("Changing rate to 80");
			limiter.set(RateLimitUtil.create(80));

			Thread.sleep(4000);
			System.out.println("Changing rate to 200");
			limiter.set(RateLimitUtil.create(200));

			Thread.sleep(4000);
			System.out.println("Changing rate to 100");
			limiter.set(RateLimitUtil.create(100));

			stop.set(true);
			
			Thread.sleep(1000);
			latch.await();
			System.out.println("=======================");
			System.out.println("Won lock: " + tracker.wonLock.get());
			System.out.println("Total ops: " + totalOps.get());

			Assert.assertEquals(20, tracker.rWindow.queue.size());
			Assert.assertTrue(16 >= tracker.rWindow.bucketCreateCount.get());
			
			List<Bucket> allBuckets = tracker.getAllBuckets();
			threadPool.shutdownNow();
			
			// Remove the first bucket since it's essentially unreliable since that is when the test had stopped.
			allBuckets.remove(0);
			
			for (Bucket b : allBuckets) {
				System.out.print(" " + b.count());
			}
			System.out.println("");
			Assert.assertTrue("P diff failed",  10 >= percentageDiff(200, allBuckets.get(0).count()));
			Assert.assertTrue("P diff failed",  10 >= percentageDiff(200, allBuckets.get(1).count()));
			Assert.assertTrue("P diff failed",  10 >= percentageDiff(200, allBuckets.get(2).count()));
			
			Assert.assertTrue("P diff failed",  10 >= percentageDiff(80, allBuckets.get(4).count()));
			Assert.assertTrue("P diff failed",  10 >= percentageDiff(80, allBuckets.get(5).count()));
			Assert.assertTrue("P diff failed",  10 >= percentageDiff(80, allBuckets.get(6).count()));
			
			Assert.assertTrue("P diff failed",  10 >= percentageDiff(120, allBuckets.get(8).count()));
			Assert.assertTrue("P diff failed",  10 >= percentageDiff(120, allBuckets.get(9).count()));
			Assert.assertTrue("P diff failed",  10 >= percentageDiff(120, allBuckets.get(10).count()));
			
			Assert.assertTrue("P diff failed",  10 >= percentageDiff(100, allBuckets.get(12).count()));
			Assert.assertTrue("P diff failed",  10 >= percentageDiff(100, allBuckets.get(13).count()));
			Assert.assertTrue("P diff failed",  10 >= percentageDiff(100, allBuckets.get(14).count()));
		}
		
		private int percentageDiff(int expected, int result) {
			int pDiff =   expected == 0 ? 0 : Math.abs(expected-result)*100/expected;
			System.out.println("Expected: " + expected  + " pDiff: " + pDiff);  
			return pDiff;
		}
	}
}
