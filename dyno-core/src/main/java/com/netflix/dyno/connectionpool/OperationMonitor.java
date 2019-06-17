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

import java.util.concurrent.TimeUnit;

/**
 * Interface for recording high level stats for operation executions.
 * Includes latency, success and failures.
 *
 * @author poberai
 */
public interface OperationMonitor {

    /**
     * Record latency for the operation
     *
     * @param opName
     * @param duration
     * @param unit
     */
    void recordLatency(String opName, long duration, TimeUnit unit);

    /**
     * Record success for the operation
     *
     * @param opName
     */
    void recordSuccess(String opName);

    /**
     * Record success while specifying if compression has been enabled for
     * the operation. Note that this means that either the value was
     * compressed or decompressed during the operation. If compression has been
     * enabled but the value does not reach the threshold then this will be false.
     *
     * @param opName
     * @param compressionEnabled
     */
    void recordSuccess(String opName, boolean compressionEnabled);

    /**
     * Record failure for the operation
     *
     * @param opName
     * @param reason
     */
    void recordFailure(String opName, String reason);

    void recordFailure(String opName, boolean compressionEnabled, String reason);
}
