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

import com.netflix.dyno.connectionpool.TokenPoolTopology;
import com.netflix.dyno.connectionpool.TopologyView;
import com.netflix.dyno.jedis.DynoJedisClient;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Synchronous implementation of a {@link DynoCounter}. This class is the base
 * class for other implementations as is contains the logic to shard the counter
 * key.
 * <p>
 * All DynoJedis* implementations, including this one, are predicated upon Redis's
 * atomic increment functionality therefore the dynomite cluster must be configured to
 * use Redis as its backing data store.
 * </p>
 *
 * @see {@Jedis.incr http://}
 * @see {@INCR http://redis.io/commands/INCR}
 *
 * @author jcacciatore
 */
public class DynoJedisDistributedCounter implements DynoCounter {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DynoJedisDistributedCounter.class);

    private static final int MAX_ITERATIONS = 1000;

    protected final String key;
    protected final DynoJedisClient client;
    protected final AtomicLong incrementCount = new AtomicLong(0L);
    protected final List<String> generatedKeys;

    public DynoJedisDistributedCounter(String key, DynoJedisClient client) {
        this.key = key;
        this.client = client;
        this.generatedKeys = generateKeys();
    }

    public void incr() {
        client.incr(generatedKeys.get(randomIntFrom0toN()));
        incrementCount.incrementAndGet();
    }

    public void incrBy(long value) {
        client.incrBy(generatedKeys.get(randomIntFrom0toN()), value);
    }

    public Long get() {
        Long result = 0L;
        ArrayList<String> values = new ArrayList<String>(generatedKeys.size());
        for (String key: generatedKeys) {
            String val = client.get(key);
            result += Long.valueOf(val);
            values.add(val);
        }

        logger.info("result=>" + result + ", key: " + key + ", values: " + values.toString());

        return result;
    }

    public String getKey() {
        return key;
    }

    public List<String> getGeneratedKeys() {
        return Collections.unmodifiableList(generatedKeys);
    }

    public Long getIncrCount() {
        return incrementCount.get();
    }

    List<String> generateKeys() {
        final TopologyView view = client.getTopologyView();
        final Map<String, List<TokenPoolTopology.TokenStatus>> topology = view.getTopologySnapshot();

        if (topology.keySet().isEmpty()) {
            throw new RuntimeException("Unable to determine dynomite topology");
        }

        // Retrieve the tokens for the cluster
        final List<String> racks = new ArrayList<String>(topology.keySet());
        final Set<Long> tokens = new HashSet<Long>();

        for (TokenPoolTopology.TokenStatus status : topology.get(racks.get(0))) {
            tokens.add(status.getToken());
        }

        final List<String> generatedKeys = new ArrayList<String>(tokens.size());

        // Find a key corresponding to each token
        int i = 0;
        while (tokens.size() > 0 && i++ < MAX_ITERATIONS) {
            Long token = view.getTokenForKey(key + "_" + i);
            if (tokens.contains(token)) {
                if (tokens.remove(token)) {
                    String generated = key + "_" + i;
                    logger.info(String.format("Found key=>%s for token=>%s", generated, token));
                    generatedKeys.add(generated);
                }
            }
        }

        return generatedKeys;
    }

    private void set(String value) {
        client.set(generatedKeys.get(0), value);
    }

    int randomIntFrom0toN() {
        // XORShift instead of Math.random http://javamex.com/tutorials/random_numbers/xorshift.shtml
        long x = System.nanoTime();
        x ^= (x << 21);
        x ^= (x >>> 35);
        x ^= (x << 4);
        return Math.abs((int) x % generatedKeys.size());
    }

    @Override
    public void close() throws Exception {
        // nothing to do
    }
}
