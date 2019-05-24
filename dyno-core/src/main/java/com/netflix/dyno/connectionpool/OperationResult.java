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

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Interface representing the result of executing an {@link Operation}
 *
 * @param <R>
 * @author poberai
 */
public interface OperationResult<R> {
    /**
     * @return Get the host on which the operation was performed
     */
    public Host getNode();

    /**
     * @param node
     */
    public OperationResult<R> setNode(Host node);

    /**
     * @return Get the result data
     */
    public R getResult();

    /**
     * @return Return the length of time to perform the operation. Does not include
     * connection pool overhead. This time is in nanoseconds
     */
    public long getLatency();

    /**
     * @param units
     * @return Return the length of time to perform the operation to the remote service. Does not include
     * connection pool overhead.
     */
    public long getLatency(TimeUnit units);

    /**
     * @return Return the number of times the operation had to be retried. This includes
     * retries for aborted connections.
     */
    public int getAttemptsCount();

    /**
     * Set the number of attempts executing this connection
     *
     * @param count
     */
    public OperationResult<R> setAttemptsCount(int count);

    /**
     * Set latency after executing the operation. This method is useful to apps that do async operations
     * and can proxy back latency stats to Dyno once they receive their result via the future.
     *
     * @param duration
     * @param unit
     */
    public OperationResult<R> setLatency(long duration, TimeUnit unit);

    /**
     * Method that returns any other metadata that is associated with this OperationResult.
     *
     * @return Map<String   ,       String>
     */
    public Map<String, String> getMetadata();

    /**
     * Add metadata to the OperationResult. Can be used within different layers of Dyno to add metadata about each layer.
     * Very useful for providing insight into the operation when debugging.
     *
     * @param key
     * @param value
     */
    public OperationResult<R> addMetadata(String key, String value);

    /**
     * Add metadata to the OperationResult. Can be used within different layers of Dyno to add metadata about each layer.
     * Very useful for providing insight into the operation when debugging.
     *
     * @param map
     */
    public OperationResult<R> addMetadata(Map<String, Object> map);
}
