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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.dyno.connectionpool.RetryPolicy;

/**
 * Simple implementation of {@link RetryPolicy} that ensures an operation can be re tried at most N times.
 * <p>
 * Note that RetryNTimes (2) means that a total of 2 + 1 = 3 attempts will be allowed before giving up.
 *
 * @author poberai
 */
public class RetryNTimes implements RetryPolicy {

    private int n;
    private final AtomicReference<RetryState> state = new AtomicReference<>(new RetryState(0, false));
    private final boolean allowCrossZoneFallback;

    public RetryNTimes(int n, boolean allowFallback) {
        this.n = n;
        this.allowCrossZoneFallback = allowFallback;
    }

    @Override
    public void begin() {
    }

    @Override
    public void success() {
        boolean success = false;
        RetryState rs;
        while (!success) {
            rs = state.get();
            success = state.compareAndSet(rs, new RetryState(rs.count + 1, true));
        }
    }

    @Override
    public void failure(Exception e) {
        boolean success = false;
        RetryState rs;
        while (!success) {
            rs = state.get();
            success = state.compareAndSet(rs, new RetryState(rs.count + 1, false));
        }
    }

    @Override
    public boolean allowRetry() {
        final RetryState rs = state.get();
        return !rs.success && rs.count <= n;
    }

    @Override
    public int getAttemptCount() {
        return state.get().count;
    }

    @Override
    public boolean allowCrossZoneFallback() {
        return allowCrossZoneFallback;
    }

    @Override
    public String toString() {
        return "RetryNTimes{" +
                "n=" + n +
                ", state=" + state.get() +
                ", allowCrossZoneFallback=" + allowCrossZoneFallback +
                '}';
    }

    public static class RetryFactory implements RetryPolicyFactory {

        int n;
        boolean allowCrossZoneFallback;

        public RetryFactory(int n) {
            this(n, true);
        }

        public RetryFactory(int n, boolean allowFallback) {
            this.n = n;
            this.allowCrossZoneFallback = allowFallback;
        }

        @Override
        public RetryPolicy getRetryPolicy() {
            return new RetryNTimes(n, allowCrossZoneFallback);
        }

        @Override
        public String toString() {
            return "RetryFactory{" +
                    "n=" + n +
                    ", allowCrossZoneFallback=" + allowCrossZoneFallback +
                    '}';
        }
    }

    private class RetryState {
        private final int count;
        private final boolean success;

        public RetryState(final int count, final boolean success) {
            this.count = count;
            this.success = success;
        }

        @Override
        public String toString() {
            return "RetryState{" +
                    "count=" + count +
                    ", success=" + success +
                    '}';
        }
    }
}
