/**
 * Copyright 2017 Netflix, Inc.
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

import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.OperationMonitor;
import com.netflix.dyno.connectionpool.impl.LastOperationMonitor;

import redis.clients.jedis.Jedis;

/**
 * Tests generic commands.
 *
 * Note - The underlying jedis client has been mocked to echo back the value
 * given for SET operations
 */
public class CommandTest {

    private DynoJedisClient client;
    private ConnectionPool<Jedis> connectionPool;
    private OperationMonitor opMonitor;

    @Mock
    DynoJedisPipelineMonitor pipelineMonitor;

    @Mock
    ConnectionPoolMonitor cpMonitor;

    @Mock
    ConnectionPoolConfiguration config;

    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);

        opMonitor = new LastOperationMonitor();

        connectionPool = new UnitTestConnectionPool(config, opMonitor);

        client = new DynoJedisClient.TestBuilder().withAppname("CommandTest").withConnectionPool(connectionPool)
                .build();

    }

    @Test
    public void testDynoJedis_GetSet() {
        String resultSet = client.set("keyFor1KBValue", VALUE_1KB);
        Assert.assertEquals("OK", resultSet); // value should not be compressed

        String resultGet = client.get("keyFor1KBValue");
        Assert.assertEquals(VALUE_1KB, resultGet);

        Long resultDel = client.del("keyFor1KBValue");
        Assert.assertEquals((long) 1, (long) resultDel);
    }


    public static final String KEY_1KB = "keyFor1KBValue";
    public static final String VALUE_1KB = generateValue(1000);

    private static String generateValue(int msgSize) {
        StringBuilder sb = new StringBuilder(msgSize);
        for (int i = 0; i < msgSize; i++) {
            sb.append('a');
        }
        return sb.toString();
    }
}
