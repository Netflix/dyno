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

import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.jedis.DynoJedisPipeline;
import com.netflix.dyno.recipes.util.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Pipeline implementation of {@link DynoCounter}. Note that this implementation
 * does not encapsulate or shield callers from pipeline semantics. Callers must
 * call {@link #incr()} and {@link #sync()}. This implementation is thread-safe
 * although {@link DynoJedisPipeline} is not.
 *
 * @author jcacciatore
 */
public class DynoJedisPipelineDistributedCounter extends DynoJedisDistributedCounter {

    private final Object obj = new Object();
    private final AtomicReference<List<Tuple<String, DynoJedisPipeline>>> keysAndPipelinesRef =
            new AtomicReference<List<Tuple<String, DynoJedisPipeline>>>(null);

    public DynoJedisPipelineDistributedCounter(String key, DynoJedisClient client) {
        super(key, client);

        ArrayList<Tuple<String, DynoJedisPipeline>> keysAndPipelines =
                new ArrayList<Tuple<String, DynoJedisPipeline>>(generatedKeys.size());

        for (String gKey: generatedKeys) {
            keysAndPipelines.add(new Tuple<String, DynoJedisPipeline>(gKey, client.pipelined()));
        }

        keysAndPipelinesRef.set(keysAndPipelines);
    }

    @Override
    public void incr() {
        synchronized (obj) {
            Tuple<String, DynoJedisPipeline> tuple = keysAndPipelinesRef.get().get(randomIntFrom0toN());
            tuple._2().incr(tuple._1());
        }
        incrementCount.incrementAndGet();
    }

    public void sync() {
        final ArrayList<Tuple<String, DynoJedisPipeline>> refreshed =
                new ArrayList<Tuple<String, DynoJedisPipeline>>(generatedKeys.size());

         // todo - this is not thread safe
        for (Tuple<String, DynoJedisPipeline> tuple: keysAndPipelinesRef.get()) {
            refreshed.add(new Tuple<String, DynoJedisPipeline>(tuple._1(), client.pipelined()));
            tuple._2().sync();
        }

        keysAndPipelinesRef.set(refreshed);

    }

}
