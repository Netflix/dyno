package com.netflix.dyno.recipes.lock;

import java.util.concurrent.atomic.AtomicInteger;

public class LockResource {
    private final AtomicInteger locked = new AtomicInteger(0);
    private final String resource;
    private final long ttlMs;

    public LockResource(String resource, long ttlMs) {
        this.resource = resource;
        this.ttlMs = ttlMs;
    }

    public String getResource() {
        return resource;
    }

    public long getTtlMs() {
        return ttlMs;
    }

    public int getLocked() {
        return locked.get();
    }

    public int incrementLocked() {
        return locked.incrementAndGet();
    }
}
