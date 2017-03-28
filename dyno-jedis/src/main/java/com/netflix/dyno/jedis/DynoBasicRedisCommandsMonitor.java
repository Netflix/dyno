/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.jedis;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.netflix.dyno.connectionpool.OperationMonitor;
import com.netflix.dyno.connectionpool.impl.utils.EstimatedHistogram;
import com.netflix.dyno.contrib.EstimatedHistogramBasedCounter.EstimatedHistogramMean;
import com.netflix.dyno.contrib.EstimatedHistogramBasedCounter.EstimatedHistogramPercentile;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.BasicCounter;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.tag.BasicTag;

public class DynoBasicRedisCommandsMonitor implements OperationMonitor {
    private final String appName;
    private final ConcurrentHashMap<String, DynoOpCounter> counterMap = new ConcurrentHashMap<String, DynoOpCounter>();
    private final ConcurrentHashMap<String, DynoTimingCounters> timerMap = new ConcurrentHashMap<String, DynoTimingCounters>();

    public DynoBasicRedisCommandsMonitor(String applicationName) {
        appName = applicationName;
    }

    @Override
    public void recordLatency(String opName, long duration, TimeUnit unit) {
        getOrCreateTimers(opName).recordLatency(duration, unit);
    }

    @Override
    public void recordSuccess(String opName) {
        getOrCreateCounter(opName, false).incrementSuccess();
    }

    @Override
    public void recordFailure(String opName, String reason) {
        getOrCreateCounter(opName, false).incrementFailure();
    }

    @Override
    public void recordSuccess(String opName, boolean compressionEnabled) {
        getOrCreateCounter(opName, false).incrementSuccess();
    }

    @Override
    public void recordFailure(String opName, boolean compressionEnabled, String reason) {
        getOrCreateCounter(opName, true).incrementFailure();
    }

    private class DynoOpCounter {

        private final Counter success;
        private final Counter failure;

        private DynoOpCounter(String appName, String opName) {
            success = getNewCounter("Dyno__" + appName + "__" + opName + "__SUCCESS", opName);
            failure = getNewCounter("Dyno__" + appName + "__" + opName + "__ERROR", opName);
        }

        private void incrementSuccess() {
            success.increment();
        }

        private void incrementFailure() {
            failure.increment();
        }

        private BasicCounter getNewCounter(String metricName, String opName) {
            MonitorConfig config = MonitorConfig.builder(metricName).withTag(new BasicTag("dyno_admin", opName))
                    .build();
            return new BasicCounter(config);
        }
    }

    private DynoOpCounter getOrCreateCounter(String opName, boolean compressionEnabled) {

        String counterName = opName + "_" + compressionEnabled;
        DynoOpCounter counter = counterMap.get(counterName);

        if (counter != null) {
            return counter;
        }

        counter = new DynoOpCounter(appName, counterName);

        DynoOpCounter prevCounter = counterMap.putIfAbsent(counterName, counter);
        if (prevCounter != null) {
            return prevCounter;
        }

        DefaultMonitorRegistry.getInstance().register(counter.success);
        DefaultMonitorRegistry.getInstance().register(counter.failure);

        return counter;
    }

    private class DynoTimingCounters {

        private final EstimatedHistogramMean latMean;
        private final EstimatedHistogramPercentile lat99;
        private final EstimatedHistogramPercentile lat995;
        private final EstimatedHistogramPercentile lat999;

        private final EstimatedHistogram estHistogram;

        private DynoTimingCounters(String appName, String opName) {

            estHistogram = new EstimatedHistogram();
            latMean = new EstimatedHistogramMean("Dyno__" + appName + "__" + opName + "__latMean", opName,
                    estHistogram);
            lat99 = new EstimatedHistogramPercentile("Dyno__" + appName + "__" + opName + "__lat990", opName,
                    estHistogram, 0.99);
            lat995 = new EstimatedHistogramPercentile("Dyno__" + appName + "__" + opName + "__lat995", opName,
                    estHistogram, 0.995);
            lat999 = new EstimatedHistogramPercentile("Dyno__" + appName + "__" + opName + "__lat999", opName,
                    estHistogram, 0.999);
        }

        public void recordLatency(long duration, TimeUnit unit) {
            long durationMicros = TimeUnit.MICROSECONDS.convert(duration, unit);
            estHistogram.add(durationMicros);
        }
    }

    private DynoTimingCounters getOrCreateTimers(String opName) {

        DynoTimingCounters timer = timerMap.get(opName);
        if (timer != null) {
            return timer;
        }
        timer = new DynoTimingCounters(appName, opName);
        DynoTimingCounters prevTimer = timerMap.putIfAbsent(opName, timer);
        if (prevTimer != null) {
            return prevTimer;
        }
        DefaultMonitorRegistry.getInstance().register(timer.latMean);
        DefaultMonitorRegistry.getInstance().register(timer.lat99);
        DefaultMonitorRegistry.getInstance().register(timer.lat995);
        DefaultMonitorRegistry.getInstance().register(timer.lat999);
        return timer;
    }

}
