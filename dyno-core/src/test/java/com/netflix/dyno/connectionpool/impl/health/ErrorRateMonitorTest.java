/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.connectionpool.impl.health;

import java.util.ArrayList;
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

import com.netflix.dyno.connectionpool.impl.health.ErrorRateMonitor.SimpleErrorCheckPolicy;
import com.netflix.dyno.connectionpool.impl.health.RateTracker.Bucket;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.RateLimitUtil;

public class ErrorRateMonitorTest {

    @Test
    public void testSimpleErrorCheckPolicy() throws Exception {

        List<Bucket> buckets = getBuckets(116, 120, 121, 120, 130, 125, 130, 120, 120, 120);

        SimpleErrorCheckPolicy policy = new SimpleErrorCheckPolicy(120, 10, 80);
        Assert.assertTrue(policy.checkErrorRate(buckets));

        policy = new SimpleErrorCheckPolicy(121, 10, 80);
        Assert.assertFalse(policy.checkErrorRate(buckets));

        policy = new SimpleErrorCheckPolicy(130, 10, 20);
        Assert.assertTrue(policy.checkErrorRate(buckets));

    }

    private List<Bucket> getBuckets(Integer... values) {
        List<Bucket> buckets = new ArrayList<Bucket>();

        for (Integer i : values) {
            Bucket b = new Bucket();
            b.track(i);
            buckets.add(b);
        }

        return buckets;
    }

    @Test
    public void testNoErrorCheckTriggers() throws Exception {

        final ErrorRateMonitor errorMonitor = new ErrorRateMonitor(20, 1, 10);
        errorMonitor.addPolicy(new SimpleErrorCheckPolicy(130, 8, 80));   // 80% of 10 seconds, if error rate > 120 then alert
        errorMonitor.addPolicy(new SimpleErrorCheckPolicy(200, 4, 80));  // 80% of 5 seconds, if error rate > 200 then alert

        List<Integer> rates = CollectionUtils.newArrayList(90, 120, 180);
        int errorCount = runTest(9, errorMonitor, rates);

        Assert.assertEquals(0, errorCount);
    }

    @Test
    public void testSustainedErrorTriggers() throws Exception {

        final ErrorRateMonitor errorMonitor = new ErrorRateMonitor(20, 1, 10);
        errorMonitor.addPolicy(new SimpleErrorCheckPolicy(130, 8, 80));   // 80% of 10 seconds, if error rate > 120 then alert
        errorMonitor.addPolicy(new SimpleErrorCheckPolicy(200, 4, 80));  // 80% of 5 seconds, if error rate > 200 then alert

        List<Integer> rates = CollectionUtils.newArrayList(130, 140, 180);
        int errorCount = runTest(9, errorMonitor, rates);

        Assert.assertEquals(1, errorCount);
    }

    @Test
    public void testOnlyLargeSpikeTriggers() throws Exception {

        final ErrorRateMonitor errorMonitor = new ErrorRateMonitor(20, 1, 10);
        errorMonitor.addPolicy(new SimpleErrorCheckPolicy(130, 10, 80));   // 80% of 10 seconds, if error rate > 120 then alert
        errorMonitor.addPolicy(new SimpleErrorCheckPolicy(200, 4, 80));  // 80% of 5 seconds, if error rate > 200 then alert

        List<Integer> rates = new ArrayList<Integer>();
        rates.add(110);
        rates.add(250);
        int errorCount = runTest(10, errorMonitor, rates);

        Assert.assertEquals(1, errorCount);
    }

    private int runTest(int totalTestRunTimeSeconds, final ErrorRateMonitor errorMonitor, final List<Integer> rates) throws Exception {

        int numThreads = 5;
        ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);

        final AtomicReference<RateLimitUtil> limiter = new AtomicReference<RateLimitUtil>(RateLimitUtil.create(rates.get(0)));
        final AtomicBoolean stop = new AtomicBoolean(false);

        final CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
        final CountDownLatch latch = new CountDownLatch(numThreads);

        final AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < numThreads; i++) {

            threadPool.submit(new Callable<Void>() {

                @Override
                public Void call() throws Exception {

                    barrier.await();
                    while (!stop.get() && !Thread.currentThread().isInterrupted()) {
                        if (limiter.get().acquire()) {
                            boolean success = errorMonitor.trackErrorRate(1);
                            if (!success) {
                                errorCount.incrementAndGet();
                            }
                        }
                    }
                    latch.countDown();
                    return null;
                }
            });
        }

        barrier.await();

        int numIterations = rates.size();
        int sleepPerIteration = totalTestRunTimeSeconds / numIterations;

        int round = 1;
        do {
            Thread.sleep(sleepPerIteration * 1000);
            if (round < rates.size()) {
                System.out.println("Changing rate to " + rates.get(round));
                limiter.set(RateLimitUtil.create(rates.get(round)));
            }
            round++;
        } while (round <= numIterations);

        stop.set(true);
        latch.await();
        threadPool.shutdownNow();

        List<Bucket> buckets = errorMonitor.getRateTracker().getAllBuckets();
        for (Bucket b : buckets) {
            System.out.print("  " + b.count());
        }
        System.out.println("\n=========TEST DONE==============");

        return errorCount.get();
    }
}