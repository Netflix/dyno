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

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.netflix.dyno.connectionpool.TokenPoolTopology;
import com.netflix.dyno.connectionpool.TopologyView;
import com.netflix.dyno.jedis.DynoJedisClient;

public class DistributedCounterTest {

    @Mock
    private DynoJedisClient client;

    @Mock
    private TopologyView topologyView;

    @Mock
    private TokenPoolTopology.TokenStatus token1;

    @Mock
    private TokenPoolTopology.TokenStatus token2;

    @Mock
    private TokenPoolTopology.TokenStatus token3;

    @Mock
    private TokenPoolTopology.TokenStatus token4;

    private Map<String, List<TokenPoolTopology.TokenStatus>> topology;


    @Before
    public void before() {
        MockitoAnnotations.initMocks(this);

        when(token1.getToken()).thenReturn(1383429731L);
        when(token2.getToken()).thenReturn(2457171554L);
        when(token3.getToken()).thenReturn(3530913377L);
        when(token4.getToken()).thenReturn(309687905L);

        List<TokenPoolTopology.TokenStatus> tokenStatusList = Arrays.asList(token1, token2, token3, token4);

        topology = new HashMap<String, List<TokenPoolTopology.TokenStatus>>();
        topology.put("us-east-1c", tokenStatusList);

        when(topologyView.getTokenForKey(anyString())).thenAnswer(new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                Object[] args = invocation.getArguments();
                String arg = args[0].toString();
                if (arg.endsWith("_100")) {
                    return token1.getToken();
                } else if (arg.endsWith("_200")) {
                    return token2.getToken();
                } else if (arg.endsWith("_300")) {
                    return token3.getToken();
                } else if (arg.endsWith("_400")) {
                    return token4.getToken();
                }
                return 0L;
            }
        });

    }

    /**
     * Test the behavior that finds the key that matches the tokens in the ring
     *
     * Topology view from dynomite server node
     * <pre>
     *   {
     *       us-east-1c:
     *       {
     *         1383429731 : [ ec2-54-226-81-202.compute-1.amazonaws.com, UP ],
     *         2457171554 : [ ec2-54-242-76-134.compute-1.amazonaws.com, UP ],
     *         3530913377 : [ ec2-54-221-36-52.compute-1.amazonaws.com,  UP ]
     *         309687905 : [ ec2-54-167-87-164.compute-1.amazonaws.com,  UP ]
     *       }
     *       .
     *       .
     *       .
     *    }
     * </pre>
     */
    @Test
    public void testGenerateKeys() {
        when(client.getTopologyView()).thenReturn(topologyView);
        when(topologyView.getTopologySnapshot()).thenReturn(topology);

        DynoJedisCounter counter = new DynoJedisCounter("testCounter", client);

        List<String> keys = counter.generateKeys();

        Assert.assertTrue(4 == keys.size());

    }
}
