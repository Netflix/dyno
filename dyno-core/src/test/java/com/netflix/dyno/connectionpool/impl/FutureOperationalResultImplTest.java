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
package com.netflix.dyno.connectionpool.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.OperationResult;

public class FutureOperationalResultImplTest {

    @Test
    public void testFutureResult() throws Exception {

        final FutureTask<Integer> futureTask = new FutureTask<Integer>(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                return 11;
            }
        });

        LastOperationMonitor opMonitor = new LastOperationMonitor();
        FutureOperationalResultImpl<Integer> futureResult =
                new FutureOperationalResultImpl<Integer>("test", futureTask, System.currentTimeMillis(), opMonitor);

        ExecutorService threadPool = Executors.newSingleThreadExecutor();

        threadPool.submit(new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                Thread.sleep(400);
                futureTask.run();
                return null;
            }
        });

        OperationResult<Integer> opResult = futureResult.get();
        int integerResult = opResult.getResult();
        long latency = opResult.getLatency();

        Assert.assertEquals(11, integerResult);
        Assert.assertTrue(latency >= 400);

        threadPool.shutdownNow();
    }
}
