/*
 * Copyright 2017 Netflix, Inc.
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

import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.ScanResult;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Tests generic commands.
 *
 * Note - The underlying jedis client has been mocked to echo back the value
 * given for SET operations
 */
public class ExpireHashTest {
    private static final String REDIS_RACK = "rack-1c";
    private static final String REDIS_DATACENTER = "rack-1";
    private DynoJedisClient client;
    private UnitTestTokenMapAndHostSupplierImpl tokenMapAndHostSupplier;

    @Before
    public void before() throws IOException {
        MockitoAnnotations.initMocks(this);

        tokenMapAndHostSupplier = new UnitTestTokenMapAndHostSupplierImpl(3, REDIS_RACK);
        final ConnectionPoolConfigurationImpl connectionPoolConfiguration =
                new ConnectionPoolConfigurationImpl(REDIS_RACK);
        connectionPoolConfiguration.withTokenSupplier(tokenMapAndHostSupplier);
        connectionPoolConfiguration.withHashtag("{}");
        connectionPoolConfiguration.setLocalRack(REDIS_RACK);
        connectionPoolConfiguration.setLocalDataCenter(REDIS_DATACENTER);

        client = new DynoJedisClient.Builder()
                .withApplicationName("CommandTest")
                .withDynomiteClusterName(REDIS_RACK)
                .withHostSupplier(tokenMapAndHostSupplier)
                .withCPConfig(connectionPoolConfiguration)
                .build();
    }

    @After
    public void after() {
        client.stopClient();
        tokenMapAndHostSupplier.shutdown();
    }

    @Test
    public void testBasicCommands() {
        final String expireHashKey = "expireHashKey";

        Assert.assertEquals(new Long(1L), client.ehset(expireHashKey, "hello", "world", 900));
        Assert.assertEquals(new Long(0L), client.ehsetnx(expireHashKey, "hello", "world",900));
        Assert.assertEquals("world", client.ehget(expireHashKey, "hello"));
        Assert.assertTrue(client.ehexists(expireHashKey, "hello"));
        Assert.assertEquals(new Long(1L), client.ehdel(expireHashKey, "hello"));
        Assert.assertNull(client.ehget(expireHashKey, "hello"));

        // verify metadata explicitly
        Set<String> mFields = client.zrangeByScore(client.ehashMetadataKey(expireHashKey), 0, Integer.MAX_VALUE);
        Assert.assertEquals(Collections.EMPTY_SET, mFields);
    }

    @Test
    public void testSecondaryKeyTimeout() throws Exception {
        final String expireHashKey = "expireHashKey";
        long veryShortTimeout = 1;
        long shortTimeout = 3; //seconds
        long longTimeout = 100;

        long startTime = System.currentTimeMillis();

        Assert.assertEquals(new Long(1L), client.ehset(expireHashKey, "hello", "world", veryShortTimeout));
        Assert.assertEquals(new Long(1L), client.ehset(expireHashKey, "alice", "bob", shortTimeout));
        Assert.assertEquals(new Long(1L), client.ehset(expireHashKey, "foo", "bar", longTimeout));

        Assert.assertEquals("world", client.ehget(expireHashKey, "hello"));
        Assert.assertEquals("bob", client.ehget(expireHashKey, "alice"));
        Assert.assertEquals("bar", client.ehget(expireHashKey, "foo"));

        Thread.sleep(veryShortTimeout * 1000L);
        Assert.assertNull(client.ehget(expireHashKey, "hello"));
        Assert.assertEquals("bob", client.ehget(expireHashKey, "alice"));
        Assert.assertEquals("bar", client.ehget(expireHashKey, "foo"));

        // check timeout on the secondary key
        long timeElapsed = System.currentTimeMillis() - startTime;
        long timeRemainingInSecs = longTimeout - (timeElapsed / 1000L);
        Assert.assertTrue(client.ehttl(expireHashKey, "foo") == timeRemainingInSecs ||
                client.ehttl(expireHashKey, "foo") == (timeRemainingInSecs + 1));

        // check timeout on expirehash
        Assert.assertEquals(new Long(-1), client.ehttl(expireHashKey));

        Assert.assertEquals(new Long(1), client.ehexpire(expireHashKey, 20));

        // check the ttl values
        Assert.assertTrue(client.ehttl(expireHashKey) == 20 ||
                client.ehttl(expireHashKey) == (20 - 1));

        Assert.assertEquals(new Long(1), client.ehpersist(expireHashKey));
        Assert.assertEquals(new Long(-1), client.ehttl(expireHashKey));

        // verify metadata explicitly
        Set<String> mFields = client.zrangeByScore(client.ehashMetadataKey(expireHashKey), 0, Integer.MAX_VALUE);
        Assert.assertEquals(2, mFields.size());

        Thread.sleep(shortTimeout * 1000L);
        Assert.assertNull(client.ehget(expireHashKey, "alice"));
        Assert.assertEquals("bar", client.ehget(expireHashKey, "foo"));

        // verify metadata explicitly
        mFields = client.zrangeByScore(client.ehashMetadataKey(expireHashKey), 0, Integer.MAX_VALUE);
        Assert.assertEquals(1, mFields.size());
    }

    @Test
    public void testMultipleFields() throws InterruptedException {
        String expireHashKey = "expireHashKey";
        final String secondaryKeyPrefix = "secondaryKey-";
        final String valuePrefix = "value-";
        final int fieldCount = 10;
        final long minTimeout = 2; //seconds

        Map<String, Pair<String, Long>> fields = new HashMap<>();
        for (int i = 0; i < fieldCount; i++) {
            fields.put(secondaryKeyPrefix + i, new ImmutablePair<>(valuePrefix + i, i + minTimeout));
        }

        Assert.assertEquals("OK", client.ehmset(expireHashKey,fields));
        long startTime = System.currentTimeMillis();
        Map<String, String> allFields = client.ehgetall(expireHashKey);
        List<String> mgetFields = client.ehmget(expireHashKey, fields.keySet().toArray(new String[0]));
        Assert.assertEquals(fieldCount, allFields.size());
        Assert.assertTrue(allFields.values().containsAll(mgetFields));
        Assert.assertEquals(fields.size(), client.ehlen(expireHashKey).longValue());

        Set<String> allKeys = client.ehkeys(expireHashKey);
        List<String> allVals = client.ehvals(expireHashKey);

        Assert.assertEquals(fieldCount, allKeys.size());
        Assert.assertEquals(fieldCount, allVals.size());

        Assert.assertTrue(allKeys.containsAll(fields.keySet()));
        Assert.assertTrue(allVals.containsAll(fields.values().stream().map(Pair::getLeft).collect(Collectors.toSet())));

        Assert.assertEquals(new Long(0), client.ehrenamenx(expireHashKey, expireHashKey));
        Assert.assertEquals("OK", client.ehrename(expireHashKey, expireHashKey + "_new"));
        expireHashKey = expireHashKey + "_new";

        Thread.sleep((minTimeout + 2) * 1000L);

        long timeElapsed = System.currentTimeMillis() - startTime;
        long remainingCount = fieldCount - (long) Math.floor((double) timeElapsed / 1000L);
        Map<String, String> remainingFields = client.ehgetall(expireHashKey);
        Set<String> mFields = client.zrangeByScore(client.ehashMetadataKey(expireHashKey), 0, Integer.MAX_VALUE);
        Assert.assertTrue(remainingFields.size() == remainingCount ||
                remainingFields.size() == remainingCount + 1);

        // verify metadata explicitly
        Assert.assertTrue(mFields.size() == remainingCount ||
                mFields.size() == remainingCount + 1);
    }

    @Test
    public void testScan() {
        String expireHashKey = "expireHashKey";
        final String secondaryKeyPrefix = "secondaryKey-";
        final String valuePrefix = "value-";
        final int fieldCount = 1000;
        final long minTimeout = 15; //seconds

        Map<String, Pair<String, Long>> fields = new HashMap<>();
        for (int i = 1; i <= fieldCount; i++) {
            fields.put(secondaryKeyPrefix + i, new ImmutablePair<>(valuePrefix + i, i + minTimeout));
            if (i % 100 == 0) {
                Assert.assertEquals("OK", client.ehmset(expireHashKey, fields));
                fields = new HashMap<>();
            }
        }

        int count = 0;
        String cursor = "0";
        do {
            ScanResult<Map.Entry<String, String>> values = client.ehscan(expireHashKey, cursor);
            count += values.getResult().size();
            cursor = values.getCursor();
        } while(cursor.compareTo("0") != 0);

        Assert.assertEquals(fieldCount, count);
    }
}
