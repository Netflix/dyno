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
package com.netflix.dyno.connectionpool.impl.utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class RateLimitUtil {

    private final AtomicReference<InnerState> ref = new AtomicReference<InnerState>(null);

    private RateLimitUtil(int rps) {
        this.ref.set(new InnerState(rps));
    }

    public static RateLimitUtil create(int n) {
        return new RateLimitUtil(n);
    }

    public int getRps() {
        return ref.get().getRps();
    }

    public boolean acquire() {

        if (ref.get().checkSameSecond()) {
            long timeToSleepMs = ref.get().increment();
            if (timeToSleepMs != -1) {
                try {
                    Thread.sleep(timeToSleepMs);
                    return false;
                } catch (InterruptedException e) {
                    // do nothing here
                    return false;
                }
            } else {
                return true;
            }

        } else {

            InnerState oldState = ref.get();
            InnerState newState = new InnerState(oldState.limit);

            ref.compareAndSet(oldState, newState);
            return false;
        }
    }


    private class InnerState {

        private final AtomicInteger counter = new AtomicInteger();
        private final AtomicLong second = new AtomicLong(0L);
        private final AtomicLong origTime = new AtomicLong(0L);

        private final int limit;

        private InnerState(int limit) {
            this.limit = limit;
            counter.set(0);
            origTime.set(System.currentTimeMillis());
            second.set(origTime.get() / 1000);
        }

        private boolean checkSameSecond() {
            long time = System.currentTimeMillis();
            return second.get() == time / 1000;
        }

        private long increment() {

            if (counter.get() < limit) {
                counter.incrementAndGet();
                return -1;
            } else {
                return System.currentTimeMillis() - origTime.get();
            }
        }

        private int getRps() {
            return limit;
        }
    }
}
