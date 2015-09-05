package com.netflix.dyno.recipes.counter;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.dyno.jedis.DynoJedisClient;

public class DynoJedisBatchDistributedCounter implements DynoCounter {

    private final AtomicLong localCounter;
    private final AtomicLong incrementCount;
    private final AtomicReference<DynoJedisDistributedCounter> counter = new AtomicReference<DynoJedisDistributedCounter>(null);

    private final ScheduledExecutorService counterThreadPool = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "DynoJedisBatchDistributedCounter-Poller");
        }
    });

    public DynoJedisBatchDistributedCounter(String key, DynoJedisClient client) {
        this.counter.compareAndSet(null, new DynoJedisDistributedCounter(key, client));
        this.localCounter = new AtomicLong(0L);
        this.incrementCount = new AtomicLong(0L);

        counterThreadPool.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (localCounter.get() > 0) {
                    incrementCount.addAndGet(localCounter.get());
                    counter.get().incrBy(localCounter.getAndSet(0));
                }
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void incr() {
        this.localCounter.incrementAndGet();
    }

    @Override
    public void incrBy(long value) {
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
    public Long getIncrCount() {
        return incrementCount.get();
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
