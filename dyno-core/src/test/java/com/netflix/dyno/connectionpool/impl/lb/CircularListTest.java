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
package com.netflix.dyno.connectionpool.impl.lb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.dyno.connectionpool.impl.lb.CircularList;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Predicate;

public class CircularListTest {

    private static final List<Integer> iList = new ArrayList<Integer>();
    private static final CircularList<Integer> cList = new CircularList<Integer>(iList);
    private static final Integer size = 10;

    private static ExecutorService threadPool;

    @BeforeClass
    public static void beforeClass() {
        threadPool = Executors.newFixedThreadPool(5);
    }

    @Before
    public void beforeTest() {

        iList.clear();
        for (int i = 0; i < size; i++) {
            iList.add(i);
        }
        cList.swapWithList(iList);
    }

    @AfterClass
    public static void afterClass() {
        threadPool.shutdownNow();
    }

    @Test
    public void testSingleThread() throws Exception {

        TestWorker worker = new TestWorker();

        for (int i = 0; i < 100; i++) {
            worker.process();
        }

        System.out.println(worker.map);

        for (Integer key : worker.map.keySet()) {
            Assert.assertTrue(worker.map.toString(), 10 == worker.map.get(key));
        }
    }

    @Test
    public void testSingleThreadWithElementAdd() throws Exception {

        final AtomicBoolean stop = new AtomicBoolean(false);

        Future<Map<Integer, Integer>> future = threadPool.submit(new Callable<Map<Integer, Integer>>() {

            @Override
            public Map<Integer, Integer> call() throws Exception {

                TestWorker worker = new TestWorker();

                while (!stop.get()) {
                    worker.process();
                }

                return worker.map;
            }
        });

        Thread.sleep(500);

        List<Integer> newList = new ArrayList<Integer>();
        newList.addAll(iList);
        for (int i = 10; i < 15; i++) {
            newList.add(i);
        }

        cList.swapWithList(newList);

        Thread.sleep(100);


        stop.set(true);

        Map<Integer, Integer> result = future.get();

        Map<Integer, Integer> subMap = CollectionUtils.filterKeys(result, new Predicate<Integer>() {
            @Override
            public boolean apply(Integer input) {
                return input != null && input < 10;
            }
        });

        List<Integer> list = new ArrayList<Integer>(subMap.values());
        checkValues(list);

        subMap = CollectionUtils.difference(result, subMap).entriesOnlyOnLeft();
        list = new ArrayList<Integer>(subMap.values());
        checkValues(list);
    }


    @Test
    public void testSingleThreadWithElementRemove() throws Exception {

        final AtomicBoolean stop = new AtomicBoolean(false);

        Future<Map<Integer, Integer>> future = threadPool.submit(new Callable<Map<Integer, Integer>>() {

            @Override
            public Map<Integer, Integer> call() throws Exception {

                TestWorker worker = new TestWorker();

                while (!stop.get()) {
                    worker.process();
                }

                return worker.map;
            }
        });

        Thread.sleep(200);

        List<Integer> newList = new ArrayList<Integer>();
        newList.addAll(iList);

        final List<Integer> removedElements = new ArrayList<Integer>();
        removedElements.add(newList.remove(2));
        removedElements.add(newList.remove(5));
        removedElements.add(newList.remove(6));

        cList.swapWithList(newList);

        Thread.sleep(200);
        stop.set(true);

        Map<Integer, Integer> result = future.get();

        Map<Integer, Integer> subMap = CollectionUtils.filterKeys(result, new Predicate<Integer>() {
            @Override
            public boolean apply(Integer input) {
                return !removedElements.contains(input);
            }
        });

        checkValues(new ArrayList<Integer>(subMap.values()));
    }

    @Test
    public void testMultipleThreads() throws Exception {

        final AtomicBoolean stop = new AtomicBoolean(false);
        final CyclicBarrier barrier = new CyclicBarrier(5);
        final List<Future<Map<Integer, Integer>>> futures = new ArrayList<Future<Map<Integer, Integer>>>();

        for (int i = 0; i < 5; i++) {
            futures.add(threadPool.submit(new Callable<Map<Integer, Integer>>() {

                @Override
                public Map<Integer, Integer> call() throws Exception {

                    barrier.await();

                    TestWorker worker = new TestWorker();

                    while (!stop.get()) {
                        worker.process();
                    }

                    return worker.map;
                }
            }));
        }

        Thread.sleep(200);
        stop.set(true);

        Map<Integer, Integer> totalMap = getTotalMap(futures);
        checkValues(new ArrayList<Integer>(totalMap.values()));
    }


    @Test
    public void testMultipleThreadsWithElementAdd() throws Exception {

        final AtomicBoolean stop = new AtomicBoolean(false);
        final CyclicBarrier barrier = new CyclicBarrier(5);
        final List<Future<Map<Integer, Integer>>> futures = new ArrayList<Future<Map<Integer, Integer>>>();

        for (int i = 0; i < 5; i++) {
            futures.add(threadPool.submit(new Callable<Map<Integer, Integer>>() {

                @Override
                public Map<Integer, Integer> call() throws Exception {

                    barrier.await();

                    TestWorker worker = new TestWorker();

                    while (!stop.get()) {
                        worker.process();
                    }

                    return worker.map;
                }
            }));
        }

        Thread.sleep(200);

        List<Integer> newList = new ArrayList<Integer>(iList);
        for (int i = 10; i < 15; i++) {
            newList.add(i);
        }

        cList.swapWithList(newList);

        Thread.sleep(200);
        stop.set(true);

        Map<Integer, Integer> result = getTotalMap(futures);

        Map<Integer, Integer> subMap = CollectionUtils.filterKeys(result, new Predicate<Integer>() {
            @Override
            public boolean apply(Integer input) {
                return input < 10;
            }
        });

        checkValues(new ArrayList<Integer>(subMap.values()));

        subMap = CollectionUtils.difference(result, subMap).entriesOnlyOnLeft();
        checkValues(new ArrayList<Integer>(subMap.values()));
    }

    @Test
    public void testMultipleThreadsWithElementsRemoved() throws Exception {

        final AtomicBoolean stop = new AtomicBoolean(false);
        final CyclicBarrier barrier = new CyclicBarrier(5);
        final List<Future<Map<Integer, Integer>>> futures = new ArrayList<Future<Map<Integer, Integer>>>();

        for (int i = 0; i < 5; i++) {
            futures.add(threadPool.submit(new Callable<Map<Integer, Integer>>() {

                @Override
                public Map<Integer, Integer> call() throws Exception {

                    barrier.await();

                    TestWorker worker = new TestWorker();

                    while (!stop.get()) {
                        worker.process();
                    }

                    return worker.map;
                }
            }));
        }

        Thread.sleep(200);

        List<Integer> newList = new ArrayList<Integer>(iList);

        final List<Integer> removedElements = new ArrayList<Integer>();
        removedElements.add(newList.remove(2));
        removedElements.add(newList.remove(5));
        removedElements.add(newList.remove(6));

        cList.swapWithList(newList);

        Thread.sleep(200);
        stop.set(true);

        Map<Integer, Integer> result = getTotalMap(futures);

        Map<Integer, Integer> subMap = CollectionUtils.filterKeys(result, new Predicate<Integer>() {

            @Override
            public boolean apply(Integer x) {
                return !removedElements.contains(x);
            }
        });

        checkValues(new ArrayList<Integer>(subMap.values()));
    }

    @Test
    public void testCircularListGet() {
        // integer overflow on read index should not result in exception
        for (long i = 0; i < (long) Integer.MAX_VALUE + 10; i++) {
            cList.getNextElement();
        }
    }

    private class TestWorker {

        private final ConcurrentHashMap<Integer, Integer> map = new ConcurrentHashMap<Integer, Integer>();

        private void process() {

            Integer element = cList.getNextElement();
            Integer count = map.get(element);
            if (count == null) {
                map.put(element, 1);
            } else {
                map.put(element, count + 1);
            }
        }
    }

    private static Map<Integer, Integer> getTotalMap(List<Future<Map<Integer, Integer>>> futures) throws InterruptedException, ExecutionException {

        Map<Integer, Integer> totalMap = new HashMap<Integer, Integer>();

        for (Future<Map<Integer, Integer>> f : futures) {

            Map<Integer, Integer> map = f.get();

            for (Integer element : map.keySet()) {
                Integer count = totalMap.get(element);
                if (count == null) {
                    totalMap.put(element, map.get(element));
                } else {
                    totalMap.put(element, map.get(element) + count);
                }
            }
        }
        return totalMap;
    }


    private static double checkValues(List<Integer> values) {

        System.out.println("Values: " + values);
        SummaryStatistics ss = new SummaryStatistics();
        for (int i = 0; i < values.size(); i++) {
            ss.addValue(values.get(i));
        }

        double mean = ss.getMean();
        double stddev = ss.getStandardDeviation();

        double p = ((stddev * 100) / mean);
        System.out.println("Percentage diff: " + p);

        Assert.assertTrue("" + p + " " + values, p < 0.1);
        return p;
    }
}
