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
package com.netflix.dyno.jedis;

import java.util.Map;
import java.util.concurrent.*;

import com.netflix.dyno.connectionpool.impl.utils.EstimatedHistogram;
import com.netflix.dyno.contrib.EstimatedHistogramBasedCounter.EstimatedHistogramMean;
import com.netflix.dyno.contrib.EstimatedHistogramBasedCounter.EstimatedHistogramPercentile;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.monitor.BasicCounter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.tag.BasicTag;
import org.slf4j.LoggerFactory;

public class DynoJedisPipelineMonitor {

    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(DynoJedisPipelineMonitor.class);

    private final ConcurrentHashMap<String, BasicCounter> counterMap = new ConcurrentHashMap<String, BasicCounter>();
    private final String appName;
    private final BasicCounter pipelineSync;
    private final BasicCounter pipelineDiscard;
    private final PipelineTimer timer;
    private final PipelineSendTimer sendTimer;
    private final int resetTimingsFrequencyInSeconds;

    private final ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "DynoJedisPipelineMonitor");
        }
    });

    public DynoJedisPipelineMonitor(String applicationName, int resetTimingsFrequencyInSeconds) {
        appName = applicationName;
        pipelineSync = getNewPipelineCounter("SYNC");
        pipelineDiscard = getNewPipelineCounter("DISCARD");
        timer = new PipelineTimer(appName);
        sendTimer = new PipelineSendTimer(appName);
        this.resetTimingsFrequencyInSeconds = resetTimingsFrequencyInSeconds;
    }

    public DynoJedisPipelineMonitor(String applicationName) {
        this(applicationName, 0);
    }

    public void init() {
        // register the counters
        DefaultMonitorRegistry.getInstance().register(pipelineSync);
        DefaultMonitorRegistry.getInstance().register(pipelineDiscard);
        // register the pipeline timer
        DefaultMonitorRegistry.getInstance().register(timer.latMean);
        DefaultMonitorRegistry.getInstance().register(timer.lat99);
        DefaultMonitorRegistry.getInstance().register(timer.lat995);
        DefaultMonitorRegistry.getInstance().register(timer.lat999);

        // NOTE -- pipeline 'send' timers are created on demand and are registered
        // in PipelineSendTimer.getOrCreateHistogram()

        Logger.debug(String.format("Initializing DynoJedisPipelineMonitor with timing counter reset frequency %d",
                resetTimingsFrequencyInSeconds));
        if (resetTimingsFrequencyInSeconds > 0) {
            threadPool.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    timer.reset();
                    sendTimer.reset();
                }
            }, 1, resetTimingsFrequencyInSeconds, TimeUnit.SECONDS);
        }
    }

    public void recordOperation(String opName) {
        getOrCreateCounter(opName).increment();
    }

    public void recordPipelineSync() {
        pipelineSync.increment();
    }

    public void recordPipelineDiscard() {
        pipelineDiscard.increment();
    }

    public void recordLatency(long duration, TimeUnit unit) {
        timer.recordLatency(duration, unit);
    }

    public void recordSendLatency(String opName, long duration, TimeUnit unit) {
        sendTimer.recordLatency(opName, duration, unit);
    }

    public void stop() {
        threadPool.shutdownNow();
    }

    private BasicCounter getOrCreateCounter(String opName) {

        BasicCounter counter = counterMap.get(opName);
        if (counter != null) {
            return counter;
        }
        counter = getNewPipelineCounter(opName);
        BasicCounter prevCounter = counterMap.putIfAbsent(opName, counter);
        if (prevCounter != null) {
            return prevCounter;
        }
        DefaultMonitorRegistry.getInstance().register(counter);
        return counter;
    }

    private BasicCounter getNewPipelineCounter(String opName) {

        String metricName = "Dyno__" + appName + "__PL__" + opName;
        MonitorConfig config = MonitorConfig.builder(metricName)
                .withTag(new BasicTag("dyno_pl_op", opName))
                .build();
        return new BasicCounter(config);
    }

    /**
     * This class measures the latency of a sync() or syncAndReturnAll() operation, which is the time
     * it takes the client to receive the response of all operations in the pipeline.
     */
    private class PipelineTimer {

        private final EstimatedHistogramMean latMean;
        private final EstimatedHistogramPercentile lat99;
        private final EstimatedHistogramPercentile lat995;
        private final EstimatedHistogramPercentile lat999;

        private final EstimatedHistogram estHistogram;

        private PipelineTimer(String appName) {

            estHistogram = new EstimatedHistogram();
            latMean = new EstimatedHistogramMean("Dyno__" + appName + "__PL__latMean", "PL", "dyno_pl_op", estHistogram);
            lat99 = new EstimatedHistogramPercentile("Dyno__" + appName + "__PL__lat990", "PL", "dyno_pl_op", estHistogram, 0.99);
            lat995 = new EstimatedHistogramPercentile("Dyno__" + appName + "__PL__lat995", "PL", "dyno_pl_op", estHistogram, 0.995);
            lat999 = new EstimatedHistogramPercentile("Dyno__" + appName + "__PL__lat999", "PL", "dyno_pl_op", estHistogram, 0.999);
        }

        public void recordLatency(long duration, TimeUnit unit) {
            long durationMicros = TimeUnit.MICROSECONDS.convert(duration, unit);
            estHistogram.add(durationMicros);
        }

        public void reset() {
            Logger.info("resetting histogram");
            estHistogram.getBuckets(true);
        }
    }

    /**
     * This class measures the time it takes to send a request from the client to the server via the pipeline. The
     * 'send' is not asynchronous within the Jedis client
     */
    private class PipelineSendTimer {

        private final Map<String, EstimatedHistogramMean> histograms = new ConcurrentHashMap<String, EstimatedHistogramMean>();
        private final String appName;

        private PipelineSendTimer(String appName) {
            this.appName = appName;
        }

        public void recordLatency(String opName, long duration, TimeUnit unit) {
            long durationMicros = TimeUnit.MICROSECONDS.convert(duration, unit);
            getOrCreateHistogram(opName).add(durationMicros);
        }

        private EstimatedHistogramMean getOrCreateHistogram(String opName) {
            if (histograms.containsKey(opName)) {
                return histograms.get(opName);
            } else {
                EstimatedHistogram histogram = new EstimatedHistogram();
                EstimatedHistogramMean histogramMean =
                        new EstimatedHistogramMean("Dyno__" + appName + "__PL__latMean", "PL_SEND", opName, histogram);
                histograms.put(opName, histogramMean);
                DefaultMonitorRegistry.getInstance().register(histogramMean);
                return histogramMean;
            }
        }

        public void reset() {
            Logger.info("resetting all SEND histograms");

            for (EstimatedHistogramMean hm : histograms.values()) {
                hm.reset();
            }
        }

    }

}
