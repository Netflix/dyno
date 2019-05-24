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
package com.netflix.dyno.demo.redis;

import com.netflix.dyno.recipes.counter.DynoCounter;
import com.netflix.dyno.recipes.counter.DynoJedisBatchCounter;
import com.netflix.dyno.recipes.counter.DynoJedisCounter;
import com.netflix.dyno.recipes.counter.DynoJedisPipelineCounter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Demo of {@link DynoCounter} implementations.
 */
public class DynoDistributedCounterDemo extends DynoJedisDemo {

    private final List<DynoCounter> counters = new ArrayList<DynoCounter>();

    public DynoDistributedCounterDemo(String clusterName, String localDC) {
        super(clusterName, localDC);
    }

    private void runMultiThreadedCounter(final int numCounters) throws Exception {
        for (int i = 0; i < numCounters; i++) {
            counters.add(new DynoJedisCounter(client.getConnPool().getName() + "-counter", client));
        }

        this.runMultiThreaded(5000, false, 1, 2);
    }

    private void runMultiThreadedPipelineCounter(final int numCounters) throws Exception {
        for (int i = 0; i < numCounters; i++) {
            DynoJedisPipelineCounter counter =
                    new DynoJedisPipelineCounter(client.getConnPool().getName() +
                            "-async-counter-" + i, client);
            counter.initialize();
            counters.add(counter);
        }


        this.runMultiThreaded(50000, false, 1, 2);
    }

    private void runSingleThreadedPipelineCounter() throws Exception {

        DynoJedisPipelineCounter counter = new DynoJedisPipelineCounter("demo-single-async", client);

        counter.initialize();

        System.out.println("counter is currently set at " + counter.get());

        for (int i = 0; i < 10000; i++) {
            counter.incr();
        }

        counter.sync();

        System.out.println("Total count => " + counter.get());

        counter.close();

        System.out.println("Cleaning up keys");
        cleanup(counter);
    }

    private void runMultiThreadedBatchCounter(int numCounters) throws Exception {
        for (int i = 0; i < numCounters; i++) {

            DynoJedisBatchCounter counter = new DynoJedisBatchCounter(
                    client.getConnPool().getName() + "-batch-counter", // key
                    client,                                            // client
                    500L                                               // flush period
            );

            counter.initialize();
            counters.add(counter);
        }

        this.runMultiThreaded(-1, false, 1, 2);
    }

    @Override
    protected void startWrites(final int ops,
                               final int numWriters,
                               final ExecutorService threadPool,
                               final AtomicBoolean stop,
                               final CountDownLatch latch,
                               final AtomicInteger success,
                               final AtomicInteger failure) {

        final Random random = new Random(System.currentTimeMillis());
        final int numCounters = counters.size() > 1 ? counters.size() - 1 : 1;

        for (int i = 0; i < numWriters; i++) {

            threadPool.submit(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    int localCount = 0;
                    while (!stop.get() && (localCount < ops || ops == -1)) {
                        try {
                            int index = random.nextInt(numCounters);

                            DynoCounter counter = counters.get(index);

                            counter.incr();
                            success.incrementAndGet();

                            // If we are pipelining, sync every 10000 increments
                            if (++localCount % 10000 == 0 &&
                                    counter instanceof DynoJedisPipelineCounter) {
                                System.out.println("WRITE - sync() " + Thread.currentThread().getName());
                                ((DynoJedisPipelineCounter) counter).sync();
                            }
                        } catch (Exception e) {
                            System.out.println("WRITE FAILURE: " + Thread.currentThread().getName() + ": " + e.getMessage());
                            e.printStackTrace();
                            failure.incrementAndGet();
                        }
                    }

                    for (DynoCounter counter : counters) {
                        if (counter instanceof DynoJedisPipelineCounter) {
                            System.out.println(Thread.currentThread().getName() + " => localCount = " + localCount);
                            ((DynoJedisPipelineCounter) counter).sync();
                        }
                    }

                    latch.countDown();
                    System.out.println(Thread.currentThread().getName() + " => Done writes");
                    return null;
                }
            });

        }
    }

    @Override
    protected void startReads(final int nKeys,
                              final int numReaders,
                              final ExecutorService threadPool,
                              final AtomicBoolean stop,
                              final CountDownLatch latch,
                              final AtomicInteger success,
                              final AtomicInteger failure,
                              final AtomicInteger emptyReads) {

        threadPool.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {

                if (counters != null && counters.get(0) instanceof DynoJedisPipelineCounter) {
                    latch.countDown();
                    return null;
                }

                while (!stop.get()) {
                    long result = 0L;

                    for (DynoCounter counter : counters) {
                        result += counter.get();
                    }

                    System.out.println("counter value ==> " + result);
                    Thread.sleep(1000);
                }
                latch.countDown();
                return null;
            }
        });
    }

    @Override
    protected void executePostRunActions() {
        long result = 0L;
        for (DynoCounter counter : counters) {
            if (counter instanceof DynoJedisPipelineCounter) {
                ((DynoJedisPipelineCounter) counter).sync();
            }
            result += counter.get();
        }

        System.out.println("COUNTER value ==> " + result);
    }

    @Override
    public void cleanup(int nKeys) {
        try {
            for (DynoCounter counter : counters) {
                cleanup(counter);
            }

            for (int i = 0; i < counters.size(); i++) {
                counters.remove(i);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void cleanup(DynoCounter counter) throws Exception {
        for (String key : counter.getGeneratedKeys()) {
            System.out.println("deleting key: " + key);
            client.del(key);
            counter.close();
        }
    }

    /**
     * Entry point to run {@link DynoCounter} demos
     *
     * @param args Should contain:
     *             <ol>dynomite_cluster_name</ol>
     *             <ol># of distributed counters (optional)</ol>
     */
    public static void main(String[] args) throws IOException {

        final String clusterName = args[0];

        final DynoDistributedCounterDemo demo = new DynoDistributedCounterDemo(clusterName, "us-east-1e");
        int numCounters = (args.length == 2) ? Integer.valueOf(args[1]) : 1;
        Properties props = new Properties();
        props.load(DynoDistributedCounterDemo.class.getResourceAsStream("/demo.properties"));
        for (String name : props.stringPropertyNames()) {
            System.setProperty(name, props.getProperty(name));
        }

        try {
            demo.initWithRemoteClusterFromEurekaUrl(args[0], 8102);

            //demo.runMultiThreadedCounter(numCounters);
            //demo.runMultiThreadedPipelineCounter(numCounters);
            //demo.runSingleThreadedPipelineCounter();
            demo.runMultiThreadedBatchCounter(numCounters);

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            demo.stop();
            System.out.println("Done");
        }
    }


}
