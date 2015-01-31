package com.netflix.dyno.connectionpool.impl.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

public class RateLimitUtilTest {

	@Test
	public void testRate() throws Exception {

		int nThreads = 5;
		int expectedRps = 100;

		final RateLimitUtil rateLimiter = RateLimitUtil.create(expectedRps);
		final AtomicBoolean stop = new AtomicBoolean(false);
		final AtomicLong counter = new AtomicLong(0L);
		final CountDownLatch latch = new CountDownLatch(nThreads);

		ExecutorService thPool = Executors.newFixedThreadPool(nThreads);

		final CyclicBarrier barrier = new CyclicBarrier(nThreads+1);

		final AtomicLong end = new AtomicLong(0L);

		for (int i=0; i<nThreads; i++) {

			thPool.submit(new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					barrier.await();
					while (!stop.get()) {
						if(rateLimiter.acquire()) {
							counter.incrementAndGet();
						}
					}
					latch.countDown();
					return null;
				}
			});
		}

		long start = System.currentTimeMillis();
		barrier.await();
		Thread.sleep(10000);
		stop.set(true);
		latch.await();
		end.set(System.currentTimeMillis());
		thPool.shutdownNow();

		long duration = end.get() - start;
		long totalCount = counter.get();
		double resultRps = ((double)(totalCount)/((double)duration/1000.0));
		System.out.println("Total Count : " + totalCount + ", duration:  " + duration + ", result rps: " + resultRps); 

		double percentageDiff = Math.abs(expectedRps-resultRps)*100/resultRps;
		System.out.println("Percentage diff: " + percentageDiff);

		Assert.assertTrue(percentageDiff < 12.0);
	}
}
