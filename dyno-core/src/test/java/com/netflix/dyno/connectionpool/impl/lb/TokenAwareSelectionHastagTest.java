/**
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
package com.netflix.dyno.connectionpool.impl.lb;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.impl.hash.Murmur1HashPartitioner;

/**
 * Test cases to cover the hashtag APIs.
 * 
 * @author ipapapa
 *
 */
public class TokenAwareSelectionHastagTest {

    private final HostToken host1 = new HostToken(309687905L, new Host("host1", -1, "r1", Status.Up, "{}"));
    private final HostToken host2 = new HostToken(1383429731L, new Host("host2", -1, "r1", Status.Up, "{}"));
    private final HostToken host3 = new HostToken(2457171554L, new Host("host3", -1, "r1", Status.Up, "{}"));
    private final HostToken host4 = new HostToken(3530913377L, new Host("host4", -1, "r1", Status.Up, "{}"));

    private final HostToken host5 = new HostToken(309687905L, new Host("host5", -1, "r1", Status.Up, ""));
    private final HostToken host6 = new HostToken(1383429731L, new Host("host6", -1, "r1", Status.Up, ""));
    private final HostToken host7 = new HostToken(2457171554L, new Host("host7", -1, "r1", Status.Up, ""));
    private final HostToken host8 = new HostToken(3530913377L, new Host("host8", -1, "r1", Status.Up, ""));

    private final Murmur1HashPartitioner m1Hash = new Murmur1HashPartitioner();
    String hashValue = "bar";

    @Test
    public void testTokenAwareWithHashtag() throws Exception {

        TreeMap<HostToken, HostConnectionPool<Integer>> pools = new TreeMap<HostToken, HostConnectionPool<Integer>>(
                new Comparator<HostToken>() {

                    @Override
                    public int compare(HostToken o1, HostToken o2) {
                        return o1.getHost().getHostAddress().compareTo(o2.getHost().getHostAddress());
                    }
                });

        pools.put(host1, getMockHostConnectionPool(host1));
        pools.put(host2, getMockHostConnectionPool(host2));
        pools.put(host3, getMockHostConnectionPool(host3));
        pools.put(host4, getMockHostConnectionPool(host4));

        String hashtag = host1.getHost().getHashtag();

        TokenAwareSelection<Integer> tokenAwareSelector = new TokenAwareSelection<Integer>();
        tokenAwareSelector.initWithHosts(pools);

        Map<String, Integer> result = new HashMap<String, Integer>();
        runTest(0L, 100000L, result, tokenAwareSelector, hashtag);

        System.out.println("Token distribution: " + result);

        verifyTokenDistribution(result.values());
    }

    @Test
    public void testTokenAwareWithEmptyHashtag() throws Exception {

        TreeMap<HostToken, HostConnectionPool<Integer>> pools = new TreeMap<HostToken, HostConnectionPool<Integer>>(
                new Comparator<HostToken>() {

                    @Override
                    public int compare(HostToken o1, HostToken o2) {
                        return o1.getHost().getHostAddress().compareTo(o2.getHost().getHostAddress());
                    }
                });

        pools.put(host5, getMockHostConnectionPool(host5));
        pools.put(host6, getMockHostConnectionPool(host6));
        pools.put(host7, getMockHostConnectionPool(host7));
        pools.put(host8, getMockHostConnectionPool(host8));

        String hashtag = host5.getHost().getHashtag();

        TokenAwareSelection<Integer> tokenAwareSelector = new TokenAwareSelection<Integer>();
        tokenAwareSelector.initWithHosts(pools);

        Map<String, Integer> result = new HashMap<String, Integer>();
        runTestEmptyHashtag(0L, 100000L, result, tokenAwareSelector, hashtag);

        System.out.println("Token distribution: " + result);

        verifyTokenDistribution(result.values());
    }

    private BaseOperation<Integer, Long> getTestOperationWithHashtag(final Long n) {
        return new BaseOperation<Integer, Long>() {

            @Override
            public String getName() {
                return "TestOperation" + n;
            }

            @Override
            public String getKey() {
                return n + "-{" + hashValue + "}";
            }

        };
    }

    private void runTest(long start, long end, Map<String, Integer> result,
            TokenAwareSelection<Integer> tokenAwareSelector, String hashtag) {

        for (long i = start; i <= end; i++) {

            BaseOperation<Integer, Long> op = getTestOperationWithHashtag(i);
            HostConnectionPool<Integer> pool = tokenAwareSelector.getPoolForOperation(op, hashtag);

            String hostName = pool.getHost().getHostAddress();

            verifyHashtagHash(op.getKey(), hostName, hashtag);

            Integer count = result.get(hostName);
            if (count == null) {
                count = 0;
            }
            result.put(hostName, ++count);
        }
    }

    private void runTestEmptyHashtag(long start, long end, Map<String, Integer> result,
            TokenAwareSelection<Integer> tokenAwareSelector, String hashtag) {

        for (long i = start; i <= end; i++) {

            BaseOperation<Integer, Long> op = getTestOperationWithHashtag(i);
            HostConnectionPool<Integer> pool = tokenAwareSelector.getPoolForOperation(op, hashtag);

            String hostName = pool.getHost().getHostAddress();

            verifyEmptyHashtagHash(op.getKey(), hostName, hashtag);

            Integer count = result.get(hostName);
            if (count == null) {
                count = 0;
            }
            result.put(hostName, ++count);
        }
    }

    private void verifyHashtagHash(String key, String hostname, String hashtag) {

        Long hashtagHash = m1Hash.hash(hashValue);

        String expectedHostname = null;

        if (hashtagHash <= 309687905L) {
            expectedHostname = "host1";
        } else if (hashtagHash <= 1383429731L) {
            expectedHostname = "host2";
        } else if (hashtagHash <= 2457171554L) {
            expectedHostname = "host3";
        } else if (hashtagHash <= 3530913377L) {
            expectedHostname = "host4";
        } else {
            expectedHostname = "host1";
        }

        if (!expectedHostname.equals(hostname)) {
            Assert.fail("FAILED! for hashtag: " + hashtag + ", got hostname: " + hostname + ", expected: "
                    + expectedHostname + " for hash: " + hashtagHash);
        }
    }

    private void verifyEmptyHashtagHash(String key, String hostname, String hashtag) {

        // hashtag is empty so we can use the key as the basis for hashing
        Long hashtagHash = m1Hash.hash(key);

        String expectedHostname = null;

        if (hashtagHash <= 309687905L) {
            expectedHostname = "host5";
        } else if (hashtagHash <= 1383429731L) {
            expectedHostname = "host6";
        } else if (hashtagHash <= 2457171554L) {
            expectedHostname = "host7";
        } else if (hashtagHash <= 3530913377L) {
            expectedHostname = "host8";
        } else {
            expectedHostname = "host5";
        }

        if (!expectedHostname.equals(hostname)) {
            Assert.fail("FAILED! for hashtag: " + hashtag + ", got hostname: " + hostname + ", expected: "
                    + expectedHostname + " for hash: " + hashtagHash);
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
