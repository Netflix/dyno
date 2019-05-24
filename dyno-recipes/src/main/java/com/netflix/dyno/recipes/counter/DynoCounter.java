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
package com.netflix.dyno.recipes.counter;

import java.util.List;

/**
 * A counter whose semantics mimic that of an in-memory counter. The counter supports incrementing by one or
 * by an arbitrary natural number.
 * <p>
 * This counter has a lifecycle; callers must invoke {@link #initialize()} prior to all other methods except
 * {@link #getKey()} otherwise an {@link IllegalStateException} may be thrown. To properly shutdown the counter
 * Callers must invoke {@link #close}.
 * </p>
 *
 * @author jcacciatore
 * @see {@link AutoCloseable}
 */
public interface DynoCounter extends AutoCloseable {

    /**
     * Initializes the counter
     */
    void initialize();

    /**
     * Increments the counter instance by one.
     */
    void incr();

    /**
     * Increments the counter instance by the given value
     *
     * @param value
     */
    void incrBy(long value);

    /**
     * Retrieves the value of the counter instance. The value is the sum of the values of each individual key.
     *
     * @return {@link Long}
     */
    Long get();

    /**
     * The key of the counter instance
     *
     * @return String representation of the key
     */
    String getKey();

    /**
     * Returns the keys of all shards of the counter instance.
     *
     * @return The keys of all shards of the counter instance.
     */
    List<String> getGeneratedKeys();

}
