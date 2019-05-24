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
package com.netflix.dyno.contrib;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.netflix.dyno.connectionpool.impl.utils.EstimatedHistogram;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.monitor.AbstractMonitor;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.tag.BasicTag;

public abstract class EstimatedHistogramBasedCounter extends AbstractMonitor<Number> {

    protected final EstimatedHistogram estHistogram;

    /**
     * Creates a new instance of the counter.
     */
    public EstimatedHistogramBasedCounter(final String name, final String opName, final EstimatedHistogram histogram) {
        super(MonitorConfig.builder(name).build()
                .withAdditionalTag(DataSourceType.GAUGE)
                .withAdditionalTag(new BasicTag("dyno_op", opName)));
        this.estHistogram = histogram;
    }

    public EstimatedHistogramBasedCounter(final String name, final String opName, final String tagName, final EstimatedHistogram histogram) {
        super(MonitorConfig.builder(name).build()
                .withAdditionalTag(DataSourceType.GAUGE)
                .withAdditionalTag(new BasicTag(tagName, opName)));
        this.estHistogram = histogram;
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof EstimatedHistogramBasedCounter)) {
            return false;
        }
        EstimatedHistogramBasedCounter m = (EstimatedHistogramBasedCounter) obj;
        return config.equals(m.getConfig()) && estHistogram.equals(m.estHistogram);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hashCode(config, estHistogram.hashCode());
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("config", config)
                .add("count", getValue())
                .toString();
    }

    public static class EstimatedHistogramMean extends EstimatedHistogramBasedCounter {

        public EstimatedHistogramMean(final String name, final String opName, final EstimatedHistogram histogram) {
            super(name, opName, histogram);
        }

        public EstimatedHistogramMean(final String name, final String opName, final String tagName, final EstimatedHistogram histogram) {
            super(name, opName, tagName, histogram);
        }

        @Override
        public Number getValue() {
            return estHistogram.mean();
        }

        @Override
        public Number getValue(int pollerIndex) {
            return estHistogram.mean();
        }

        public void add(long n) {
            this.estHistogram.add(n);
        }

        public void reset() {
            this.estHistogram.getBuckets(true);
        }

    }

    public static class EstimatedHistogramPercentile extends EstimatedHistogramBasedCounter {

        private final double percentile;

        public EstimatedHistogramPercentile(final String name, final String opName, final EstimatedHistogram histogram, double pVal) {
            super(name, opName, histogram);
            percentile = pVal;
        }

        public EstimatedHistogramPercentile(final String name, final String opName, final String tagName, final EstimatedHistogram histogram, double pVal) {
            super(name, opName, tagName, histogram);
            percentile = pVal;
        }

        @Override
        public Number getValue() {
            return estHistogram.percentile(percentile);
        }

        @Override
        public Number getValue(int pollerIndex) {
            return estHistogram.percentile(percentile);
        }
    }
}
