/*******************************************************************************
 * Copyright 2015 Netflix
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
package com.netflix.dyno.jedis;

import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.OperationMonitor;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl;
import com.netflix.dyno.connectionpool.impl.LastOperationMonitor;
import com.netflix.dyno.connectionpool.impl.utils.ZipUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests compression commands.
 *
 * Note - The underlying jedis client has been mocked to echo back the value given for SET operations and to
 *        ensure values over a 2KB threshold are compressed for HMSET operations.
 */
public class CompressionTest {

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

        when(config.getValueCompressionThreshold()).thenReturn(2 * 1024);

        opMonitor = new LastOperationMonitor();

        connectionPool = new  UnitTestConnectionPool(config, opMonitor);

        client = new DynoJedisClient.TestBuilder()
                .withAppname("CompressionTest")
                .withConnectionPool(connectionPool)
                .build();

    }

    @Test
    public void testDynoJedis_Set_UnderCompressionThreshold() {

        String result = client.set("keyFor1KBValue", VALUE_1KB);

        Assert.assertEquals(VALUE_1KB, result); // value should not be compressed

    }

    @Test
    public void testDynoJedis_Set_AboveCompressionThreshold() throws IOException {

        String result = client.set("keyFor3KBValue", VALUE_3KB);

        Assert.assertTrue(result.length() < 3072);
        Assert.assertTrue(ZipUtils.isCompressed(result));
    }

    @Test
    public void testDynoJedis_Get_UnderCompressionThreshold() {
        client.set(KEY_1KB, VALUE_1KB);

        String result = client.get(KEY_1KB);

        Assert.assertEquals(VALUE_1KB, result);
    }

    @Test
    public void testDynoJedis_Get_AboveCompressionThreshold() throws IOException {
        client.set(KEY_3KB, VALUE_3KB);

        String result = client.get(KEY_3KB);

        Assert.assertTrue(!ZipUtils.isCompressed(result));
        Assert.assertEquals(VALUE_3KB, result);
    }

    @Test
    public void testDynoJedis_Hmset_AboveCompressionThreshold() throws IOException {
        final Map<String, String> map = new HashMap<String, String>();
        map.put(KEY_1KB, VALUE_1KB);
        map.put(KEY_3KB, VALUE_3KB);

        client.d_hmset("compressionTestKey", map);

        LastOperationMonitor monitor = (LastOperationMonitor) opMonitor;
        Assert.assertTrue(1 == monitor.getSuccessCount(OpName.HMSET.name(), true));
    }

    @Test
    public void testZipUtilsDecompressBytesNonBase64() throws Exception {
        String s = "ABCDEFG__abcdefg__1234567890'\"\\+=-::ABCDEFG__abcdefg__1234567890'\"\\+=-::ABCDEFG__abcdefg__1234567890'\"\\+=-";
        byte[] val = s.getBytes();

        byte[] compressed =  ZipUtils.compressBytesNonBase64(val);

        Assert.assertTrue(compressed.length < val.length);

        byte[] decompressed = ZipUtils.decompressBytesNonBase64(compressed);

        Assert.assertEquals(s, new String(decompressed));
    }

//    @Test
//    public void testDynoJedisPipeline_Binary_HGETALL() throws Exception {
//        Map<byte[], byte[]>
//
//        ConnectionPoolImpl cp = mock(ConnectionPoolImpl.class);
//
//        DynoJedisPipeline pipeline = new
//                DynoJedisPipeline(cp, pipelineMonitor, cpMonitor);
//
//        //pipeline.hgetAll();
//
//    }

    public static final String KEY_1KB = "keyFor1KBValue";
    public static final String KEY_3KB = "keyFor3KBValue";
    public static final String VALUE_1KB = generateValue(1);
    public static final String VALUE_3KB = generateValue(3);

    private static String generateValue(int kilobytes) {
        StringBuilder sb = new StringBuilder(kilobytes * 512); // estimating 2 bytes per char
        for (int i = 0; i < kilobytes; i++) {
            for (int j = 0; j < 10; j++) {
                sb.append("abcdefghijklmnopqrstuvwxzy0123456789a1b2c3d4f5g6h7"); // 50 characters (~100 bytes)
                sb.append(":");
                sb.append("abcdefghijklmnopqrstuvwxzy0123456789a1b2c3d4f5g6h7");
                sb.append(":");
            }
        }

        return sb.toString();

    }

}
