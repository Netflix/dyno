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
package com.netflix.dyno.connectionpool.impl.lb;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Comparator;
import java.util.HashMap;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.impl.hash.Murmur1HashPartitioner;

/* author: ipapapa */

public class TokenAwareSelectionBinaryTest {

    /**
     * cqlsh:dyno_bootstrap> select "availabilityZone","hostname","token" from
     * tokens where "appId" = 'dynomite_redis_puneet';
     *
     * availabilityZone | hostname | token
     * ------------------+--------------------------------------------+------------
     * us-east-1c | ec2-54-83-179-213.compute-1.amazonaws.com | 1383429731
     * us-east-1c | ec2-54-224-184-99.compute-1.amazonaws.com | 309687905 us-east-1c
     * | ec2-54-91-190-159.compute-1.amazonaws.com | 3530913377 us-east-1c |
     * ec2-54-81-31-218.compute-1.amazonaws.com | 2457171554 us-east-1e |
     * ec2-54-198-222-153.compute-1.amazonaws.com | 309687905 us-east-1e |
     * ec2-54-198-239-231.compute-1.amazonaws.com | 2457171554 us-east-1e |
     * ec2-54-226-212-40.compute-1.amazonaws.com | 1383429731 us-east-1e |
     * ec2-54-197-178-229.compute-1.amazonaws.com | 3530913377
     *
     * cqlsh:dyno_bootstrap>
     */
    private static final String UTF_8 = "UTF-8";
    private static final Charset charset = Charset.forName(UTF_8);

    private final HostToken h1 = new HostToken(309687905L, new Host("h1", -1, "r1", Status.Up));
    private final HostToken h2 = new HostToken(1383429731L, new Host("h2", -1, "r1", Status.Up));
    private final HostToken h3 = new HostToken(2457171554L, new Host("h3", -1, "r1", Status.Up));
    private final HostToken h4 = new HostToken(3530913377L, new Host("h4", -1, "r1", Status.Up));

    private final HostToken h1p8100 = new HostToken(309687905L, new Host("h1", 8100, "r1", Status.Up));
    private final HostToken h1p8101 = new HostToken(1383429731L, new Host("h1", 8101, "r1", Status.Up));
    private final HostToken h1p8102 = new HostToken(2457171554L, new Host("h1", 8102, "r1", Status.Up));
    private final HostToken h1p8103 = new HostToken(3530913377L, new Host("h1", 8103, "r1", Status.Up));

    private final Murmur1HashPartitioner m1Hash = new Murmur1HashPartitioner();

    @Test
    public void testTokenAware() throws Exception {

        TreeMap<HostToken, HostConnectionPool<Integer>> pools = new TreeMap<HostToken, HostConnectionPool<Integer>>(
                new Comparator<HostToken>() {

                    @Override
                    public int compare(HostToken o1, HostToken o2) {
                        return o1.getHost().getHostAddress().compareTo(o2.getHost().getHostAddress());
                    }
                });

        pools.put(h1, getMockHostConnectionPool(h1));
        pools.put(h2, getMockHostConnectionPool(h2));
        pools.put(h3, getMockHostConnectionPool(h3));
        pools.put(h4, getMockHostConnectionPool(h4));

        TokenAwareSelection<Integer> tokenAwareSelector = new TokenAwareSelection<Integer>();
        tokenAwareSelector.initWithHosts(pools);

        Map<String, Integer> result = new HashMap<String, Integer>();
        runTest(0L, 100000L, result, tokenAwareSelector);

        System.out.println("Token distribution: " + result);

        verifyTokenDistribution(result.values());
    }

    @Test
    public void testTokenAwareMultiplePorts() throws Exception {

        TreeMap<HostToken, HostConnectionPool<Integer>> pools = new TreeMap<HostToken, HostConnectionPool<Integer>>(
                new Comparator<HostToken>() {

                    @Override
                    public int compare(HostToken o1, HostToken o2) {
                        return o1.compareTo(o2);
                    }
                });

        pools.put(h1p8100, getMockHostConnectionPool(h1p8100));
        pools.put(h1p8101, getMockHostConnectionPool(h1p8101));
        pools.put(h1p8102, getMockHostConnectionPool(h1p8102));
        pools.put(h1p8103, getMockHostConnectionPool(h1p8103));

        TokenAwareSelection<Integer> tokenAwareSelector = new TokenAwareSelection<Integer>();
        tokenAwareSelector.initWithHosts(pools);

        Map<Integer, Integer> result = new HashMap<Integer, Integer>();
        runTestWithPorts(0L, 100000L, result, tokenAwareSelector);

        System.out.println("Token distribution: " + result);

        verifyTokenDistribution(result.values());
    }

    private BaseOperation<Integer, Long> getTestOperation(final Long n) {

        return new BaseOperation<Integer, Long>() {

            @Override
            public String getName() {
                return "TestOperation" + n;
            }

            @Override
            public String getStringKey() {
                return "" + n;
            }

            @Override
            public byte[] getBinaryKey() {
                String key = "" + n;
                ByteBuffer bb = ByteBuffer.wrap(key.getBytes(charset));
                return bb.array();
            }
        };
    }

    private void runTest(long start, long end, Map<String, Integer> result,
                         TokenAwareSelection<Integer> tokenAwareSelector) {

        for (long i = start; i <= end; i++) {

            BaseOperation<Integer, Long> op = getTestOperation(i);
            HostConnectionPool<Integer> pool = tokenAwareSelector.getPoolForOperation(op, null);

            String hostName = pool.getHost().getHostAddress();

            verifyKeyHash(op.getBinaryKey(), hostName);

            Integer count = result.get(hostName);
            if (count == null) {
                count = 0;
            }
            result.put(hostName, ++count);
        }
    }

    private void runTestWithPorts(long start, long end, Map<Integer, Integer> result,
                                  TokenAwareSelection<Integer> tokenAwareSelector) {

        for (long i = start; i <= end; i++) {

            BaseOperation<Integer, Long> op = getTestOperation(i);
            HostConnectionPool<Integer> pool = tokenAwareSelector.getPoolForOperation(op, null);

            int port = pool.getHost().getPort();

            verifyKeyHashWithPort(op.getBinaryKey(), port);

            Integer count = result.get(port);
            if (count == null) {
                count = 0;
            }
            result.put(port, ++count);
        }
    }

    private void verifyKeyHash(byte[] key, String hostname) {

        Long keyHash = m1Hash.hash(key);

        String expectedHostname = null;

        if (keyHash <= 309687905L) {
            expectedHostname = "h1";
        } else if (keyHash <= 1383429731L) {
            expectedHostname = "h2";
        } else if (keyHash <= 2457171554L) {
            expectedHostname = "h3";
        } else if (keyHash <= 3530913377L) {
            expectedHostname = "h4";
        } else {
            expectedHostname = "h1";
        }

        if (!expectedHostname.equals(hostname)) {
            Assert.fail("FAILED! for key: " + key.toString() + ", got hostname: " + hostname + ", expected: "
                    + expectedHostname + " for hash: " + keyHash);
        }
    }

    private void verifyKeyHashWithPort(byte[] key, int port) {

        Long keyHash = m1Hash.hash(key);

        String expectedHostname = null;
        int expectedPort = 0;

        if (keyHash <= 309687905L) {
            expectedPort = 8100;
        } else if (keyHash <= 1383429731L) { // 1129055870
            expectedPort = 8101;
        } else if (keyHash <= 2457171554L) {
            expectedPort = 8102;
        } else if (keyHash <= 3530913377L) {
            expectedPort = 8103;
        } else {
            expectedPort = 8100;
        }

        if (expectedPort != port) {
            Assert.fail("FAILED! for key: " + key.toString() + ", got port: " + port + ", expected: " + expectedPort
                    + " for hash: " + keyHash);
        }
    }

    private void verifyTokenDistribution(Collection<Integer> values) {

        int sum = 0;
        int count = 0;
        for (int n : values) {
            sum += n;
            count++;
        }

        double mean = (sum / count);

        for (int n : values) {
            double percentageDiff = 100 * ((mean - n) / mean);
            Assert.assertTrue(percentageDiff < 1.0);
        }
    }

    @SuppressWarnings("unchecked")
    public HostConnectionPool<Integer> getMockHostConnectionPool(final HostToken hostToken) {

        HostConnectionPool<Integer> mockHostPool = mock(HostConnectionPool.class);
        when(mockHostPool.isActive()).thenReturn(true);
        when(mockHostPool.getHost()).thenReturn(hostToken.getHost());

        return mockHostPool;
    }
}
