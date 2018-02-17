/**
 * Copyright 2016 Netflix, Inc.
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

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.jedis.utils.MockedRedisResponse;
import org.junit.Assert;
import org.junit.Test;

import javax.net.ssl.SSLContext;
import java.util.*;

import static com.netflix.dyno.jedis.utils.SSLContextUtil.createAndInitSSLContext;

/**
 * This tests checks if we can use Jedis with/without SSL/TLS against real TCP server, and because of Redis
 * itself doesn't support SSL, we are using "dummy" Redis that is able to answer our "GET key" command.
 */
public class JedisConnectionFactoryIntegrationTest {

    private final int port = 8998;

    private final String rack = "rack1";

    private final String datacenter = "rack";

    private final Host localHost = new Host("localhost", port, rack, Host.Status.Up);

    private final HostSupplier localHostSupplier = new HostSupplier() {

        @Override
        public List<Host> getHosts() {
            return Collections.singletonList(localHost);
        }
    };

    private final TokenMapSupplier supplier = new TokenMapSupplier() {

        @Override
        public List<HostToken> getTokens(Set<Host> activeHosts) {
            return Collections.singletonList(localHostToken);
        }

        @Override
        public HostToken getTokenForHost(Host host, Set<Host> activeHosts) {
            return localHostToken;
        }

        final HostToken localHostToken = new HostToken(100000L, localHost);

    };

    @Test
    public void testSSLJedisClient() throws Exception {
        //given
        final String expectedValue = String.valueOf(new Random().nextInt());

        //lets start server with SSL
        final MockedRedisResponse mockedRedisResponse = new MockedRedisResponse(expectedValue, true);

        //when
        mockedRedisResponse.start();

        final DynoJedisClient dynoClient = constructJedisClient(true);

        //key doesn't matter here, we just want to test tcp connection
        final String value = dynoClient.get("keyNameDoestMatter");

        dynoClient.stopClient();
        mockedRedisResponse.stop();

        //then
        Assert.assertEquals(value, expectedValue);
    }

    @Test
    public void testWithNoSSLJedisClient() throws Exception {
        //given
        final String expectedValue = String.valueOf(new Random().nextInt());

        //lets start server without SSL
        final MockedRedisResponse mockedRedisResponse = new MockedRedisResponse(expectedValue, false);

        //when
        mockedRedisResponse.start();

        final DynoJedisClient dynoClient = constructJedisClient(false);

        //key doesn't matter here, we just want to test tcp connection
        final String value = dynoClient.get("keyNameDoestMatter");

        dynoClient.stopClient();
        mockedRedisResponse.stop();

        //then
        Assert.assertEquals(value, expectedValue);
    }

    private DynoJedisClient constructJedisClient(final boolean withSsl) throws Exception {
        final ConnectionPoolConfigurationImpl connectionPoolConfiguration = new ConnectionPoolConfigurationImpl(rack);
        connectionPoolConfiguration.withTokenSupplier(supplier);
        connectionPoolConfiguration.setLocalRack(rack);
        connectionPoolConfiguration.setLocalDataCenter(datacenter);

        final SSLContext sslContext = createAndInitSSLContext("client.jks");

        final DynoJedisClient.Builder builder = new DynoJedisClient.Builder()
                .withApplicationName("appname")
                .withDynomiteClusterName(rack)
                .withHostSupplier(localHostSupplier)
                .withCPConfig(connectionPoolConfiguration);

        if (withSsl) {
            return builder
                    .withSSLSocketFactory(sslContext.getSocketFactory())
                    .build();
        } else {
            return builder.build();
        }
    }
}
