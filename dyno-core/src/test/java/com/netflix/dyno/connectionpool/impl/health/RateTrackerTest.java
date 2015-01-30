package com.netflix.dyno.connectionpool.impl.health;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.impl.health.RateTracker.Bucket;
import com.netflix.dyno.connectionpool.impl.utils.RateLimitUtil;

public class RateTrackerTest {
		
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
		threadPool.shutdownNow();

		//Thread.sleep(100);
		latch.await();
		
		System.out.println("=======================");
		System.out.println("Won lock: " + tracker.getWonLockCount());
		System.out.println("Total ops: " + totalOps.get());

		Assert.assertEquals(20, tracker.rWindow.getQueueSize());
		Assert.assertTrue(16 >= tracker.rWindow.getBucketCreateCount());

		List<Bucket> allBuckets = tracker.getAllBuckets();

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
