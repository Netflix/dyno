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
package com.netflix.dyno.connectionpool;

import java.util.List;

import com.netflix.dyno.connectionpool.impl.health.ErrorRateMonitor;

/**
 * Interface for config required by {@link ErrorRateMonitor}
 *
 * @author poberai
 */
public interface ErrorRateMonitorConfig {

    /**
     * Size of the window (in seconds) that should be monitored
     *
     * @return int
     */
    public int getWindowSizeSeconds();

    /**
     * Frequency at which the error rate check should run
     *
     * @return int
     */
    public int getCheckFrequencySeconds();

    /**
     * Window for suppressing alerts once the alert has been triggered.
     * This is useful for suppressing a deluge of alerts once a critical alert has been fired.
     *
     * @return int
     */
    public int getCheckSuppressWindowSeconds();

    /**
     * Multiple ErrorThresholds to honor
     *
     * @return List<ErrorThreshold>
     */
    public List<ErrorThreshold> getThresholds();

    /**
     * Interface the describes an isolated error threshold to monitor
     *
     * @author poberai
     */
    public interface ErrorThreshold {

        /**
         * Error threshold
         *
         * @return int
         */
        public int getThresholdPerSecond();

        /**
         * Size of window to consider when sampling the error rate
         *
         * @return int
         */
        public int getWindowSeconds();

        /**
         * How much of the window should be above error threshold in order to generate the alert.
         * This is used when the error rates are sparse but still frequent enough to trigger an alert
         *
         * @return int
         */
        public int getWindowCoveragePercentage();
    }

}
