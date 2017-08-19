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

public class JedisConnectionFactoryIntegrationTest {

    final int port = 8998;

    final Host localHost = new Host("localhost", port, "rack1", Host.Status.Up);

    final HostSupplier localHostSupplier = new HostSupplier() {

        @Override
        public Collection<Host> getHosts() {
            return Collections.singletonList(localHost);
        }
    };

    final TokenMapSupplier supplier = new TokenMapSupplier() {

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

        final String value = dynoClient.get("keyNameDoestMatter");

        dynoClient.stopClient();

        mockedRedisResponse.stop();

        //then
        Assert.assertEquals(value, expectedValue);

    }

    private DynoJedisClient constructJedisClient(final boolean withSsl) throws Exception {
        final ConnectionPoolConfigurationImpl connectionPoolConfiguration = new ConnectionPoolConfigurationImpl("rack1");
        connectionPoolConfiguration.withTokenSupplier(supplier);
        connectionPoolConfiguration.setLocalRack("rack1");

        final SSLContext sslContext = createAndInitSSLContext("client.jks");

        if (withSsl) {
            return new DynoJedisClient.Builder()
                    .withApplicationName("appname")
                    .withDynomiteClusterName("rack1")
                    .withHostSupplier(localHostSupplier)
                    .withCPConfig(connectionPoolConfiguration)
                    .withSSLSocketFactory(sslContext.getSocketFactory())
                    .build();
        } else {
            return new DynoJedisClient.Builder()
                    .withApplicationName("appname")
                    .withDynomiteClusterName("rack1")
                    .withHostSupplier(localHostSupplier)
                    .withCPConfig(connectionPoolConfiguration)
                    .build();
        }


    }
}
