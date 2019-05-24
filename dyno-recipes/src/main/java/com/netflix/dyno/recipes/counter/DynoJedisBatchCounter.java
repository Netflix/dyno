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
package com.netflix.dyno.recipes.counter;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.dyno.jedis.DynoJedisClient;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Batch implementation of {@link DynoCounter} that uses an in-memory counter to
 * track {@link #incr()} calls and flushes the value at the given frequency.
 */
@ThreadSafe
public class DynoJedisBatchCounter implements DynoCounter {

    private final AtomicBoolean initialized = new AtomicBoolean(false);
    private final AtomicLong localCounter;
    private final AtomicReference<DynoJedisCounter> counter = new AtomicReference<DynoJedisCounter>(null);
    private final Long frequencyInMillis;

    private final ScheduledExecutorService counterThreadPool = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "DynoJedisBatchCounter-Poller");
        }
    });

    public DynoJedisBatchCounter(String key, DynoJedisClient client, Long frequencyInMillis) {
        this.counter.compareAndSet(null, new DynoJedisCounter(key, client));
        this.localCounter = new AtomicLong(0L);
        this.frequencyInMillis = frequencyInMillis;
    }

    @Override
    public void initialize() {
        if (initialized.compareAndSet(false, true)) {
            counter.get().initialize();

            counterThreadPool.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    if (localCounter.get() > 0) {
                        counter.get().incrBy(localCounter.getAndSet(0));
                    }
                }
            }, 1000, frequencyInMillis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void incr() {
        if (!initialized.get()) {
            throw new IllegalStateException("Counter has not been initialized");
        }

        this.localCounter.incrementAndGet();
    }

    @Override
    public void incrBy(long value) {
        if (!initialized.get()) {
            throw new IllegalStateException("Counter has not been initialized");
        }

        this.localCounter.addAndGet(value);
    }

    @Override
    public Long get() {
        return counter.get().get();
    }

    @Override
    public String getKey() {
        return counter.get().getKey();
    }

    @Override
    public List<String> getGeneratedKeys() {
        return counter.get().getGeneratedKeys();
    }

    @Override
    public void close() throws Exception {
        try {
            counterThreadPool.shutdownNow();
        } catch (Throwable th) {
            // ignore
        }
    }
}
