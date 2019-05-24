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
package com.netflix.dyno.connectionpool.impl;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.ListenableFuture;
import com.netflix.dyno.connectionpool.OperationMonitor;
import com.netflix.dyno.connectionpool.OperationResult;

/**
 * Impl for Future<OperationResult<R>> that encapsulates an inner future.
 * The class provides a functionality to record the time when the caller calls get() on the future.
 * This helps record end-end timing for async operations.
 * Not that there is a caveat here that if the future is called at a later point in time, then yes the timing stats
 * will appear to be bloated unnecessarily. What we really need here is a listenable future, where we should log the
 * timing stats on the callback.
 *
 * @param <R>
 * @author poberai
 */
public class FutureOperationalResultImpl<R> implements ListenableFuture<OperationResult<R>> {

    private final Future<R> future;
    private final OperationResultImpl<R> opResult;
    private final long startTime;
    private final AtomicBoolean timeRecorded = new AtomicBoolean(false);

    public FutureOperationalResultImpl(String opName, Future<R> rFuture, long start, OperationMonitor opMonitor) {
        this.future = rFuture;
        this.opResult = new OperationResultImpl<R>(opName, rFuture, opMonitor).attempts(1);
        this.startTime = start;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isDone() {
        return future.isDone();
    }

    @Override
    public OperationResult<R> get() throws InterruptedException, ExecutionException {
        try {
            future.get();
            return opResult;
        } finally {
            recordTimeIfNeeded();
        }
    }

    private void recordTimeIfNeeded() {
        if (timeRecorded.get()) {
            return;
        }
        if (timeRecorded.compareAndSet(false, true)) {
            opResult.setLatency(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public OperationResult<R> get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            future.get(timeout, unit);
            return opResult;
        } finally {
            recordTimeIfNeeded();
        }
    }

    public FutureOperationalResultImpl<R> node(Host node) {
        opResult.setNode(node);
        return this;
    }

    public OperationResultImpl<R> getOpResult() {
        return opResult;
    }


    @Override
    public void addListener(Runnable listener, Executor executor) {
        throw new RuntimeException("Not Implemented");
    }
}
