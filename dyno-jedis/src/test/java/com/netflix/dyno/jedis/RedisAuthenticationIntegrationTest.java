package com.netflix.dyno.jedis;

import com.google.common.base.Throwables;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostBuilder;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.impl.HostConnectionPoolImpl;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.contrib.DynoOPMonitor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.embedded.RedisServer;
import redis.embedded.RedisServerBuilder;

import java.net.ConnectException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class RedisAuthenticationIntegrationTest {

    private static final int REDIS_PORT = 8998;
    private static final String REDIS_RACK = "rack-1c";
    private static final String REDIS_DATACENTER = "rack-1";

    private RedisServer redisServer;

    @Before
    public void setUp() throws Exception {
        // skip tests on windows due to https://github.com/spinnaker/embedded-redis#redis-version
        Assume.assumeFalse(System.getProperty("os.name").toLowerCase().startsWith("win"));
    }

    @After
    public void tearDown() throws Exception {
        if (redisServer != null) {
            redisServer.stop();
        }
    }

    @Test
    public void testDynoClient_noAuthSuccess() throws Exception {
        redisServer = new RedisServer(REDIS_PORT);
        redisServer.start();

        Host noAuthHost = new HostBuilder().setHostname("localhost").setPort(REDIS_PORT).setRack(REDIS_RACK).setStatus(Status.Up).createHost();
        TokenMapSupplierImpl tokenMapSupplier = new TokenMapSupplierImpl(noAuthHost);
        DynoJedisClient dynoClient = constructJedisClient(tokenMapSupplier,
                () -> Collections.singletonList(noAuthHost));

        String statusCodeReply = dynoClient.set("some-key", "some-value");
        Assert.assertEquals("OK", statusCodeReply);

        String value = dynoClient.get("some-key");
        Assert.assertEquals("some-value", value);
    }

    @Test
    public void testDynoClient_authSuccess() throws Exception {
        redisServer = new RedisServerBuilder()
                .port(REDIS_PORT)
                .setting("requirepass password")
                .build();
        redisServer.start();

        Host authHost = new HostBuilder().setHostname("localhost").setPort(REDIS_PORT).setRack(REDIS_RACK).setStatus(Status.Up).setHashtag(null).setPassword("password").createHost();

        TokenMapSupplierImpl tokenMapSupplier = new TokenMapSupplierImpl(authHost);
        DynoJedisClient dynoClient = constructJedisClient(tokenMapSupplier,
                () -> Collections.singletonList(authHost));

        String statusCodeReply = dynoClient.set("some-key", "some-value");
        Assert.assertEquals("OK", statusCodeReply);

        String value = dynoClient.get("some-key");
        Assert.assertEquals("some-value", value);
    }

    @Test
    public void testJedisConnFactory_noAuthSuccess() throws Exception {
        redisServer = new RedisServer(REDIS_PORT);
        redisServer.start();

        Host noAuthHost = new HostBuilder().setHostname("localhost").setPort(REDIS_PORT).setRack(REDIS_RACK).setStatus(Status.Up).createHost();

        JedisConnectionFactory conFactory =
                new JedisConnectionFactory(new DynoOPMonitor("some-application-name"), null);
        ConnectionPoolConfiguration cpConfig = new ConnectionPoolConfigurationImpl("some-name");
        CountingConnectionPoolMonitor poolMonitor = new CountingConnectionPoolMonitor();
        HostConnectionPool<Jedis> hostConnectionPool =
                new HostConnectionPoolImpl<>(noAuthHost, conFactory, cpConfig, poolMonitor);
        Connection<Jedis> connection = conFactory
                .createConnection(hostConnectionPool);

        connection.execPing();
    }

    @Test
    public void testJedisConnFactory_authSuccess() throws Exception {
        redisServer = new RedisServerBuilder()
                .port(REDIS_PORT)
                .setting("requirepass password")
                .build();
        redisServer.start();

        Host authHost = new HostBuilder().setHostname("localhost").setPort(REDIS_PORT).setRack(REDIS_RACK).setStatus(Status.Up).setHashtag(null).setPassword("password").createHost();

        JedisConnectionFactory conFactory =
                new JedisConnectionFactory(new DynoOPMonitor("some-application-name"), null);
        ConnectionPoolConfiguration cpConfig = new ConnectionPoolConfigurationImpl("some-name");
        CountingConnectionPoolMonitor poolMonitor = new CountingConnectionPoolMonitor();
        HostConnectionPool<Jedis> hostConnectionPool =
                new HostConnectionPoolImpl<>(authHost, conFactory, cpConfig, poolMonitor);
        Connection<Jedis> connection = conFactory
                .createConnection(hostConnectionPool);

        connection.execPing();
    }

    @Test
    public void testJedisConnFactory_connectionFailed() throws Exception {
        Host noAuthHost = new HostBuilder().setHostname("localhost").setPort(REDIS_PORT).setRack(REDIS_RACK).setStatus(Status.Up).createHost();

        JedisConnectionFactory conFactory =
                new JedisConnectionFactory(new DynoOPMonitor("some-application-name"), null);
        ConnectionPoolConfiguration cpConfig = new ConnectionPoolConfigurationImpl("some-name");
        CountingConnectionPoolMonitor poolMonitor = new CountingConnectionPoolMonitor();
        HostConnectionPool<Jedis> hostConnectionPool =
                new HostConnectionPoolImpl<>(noAuthHost, conFactory, cpConfig, poolMonitor);
        Connection<Jedis> connection = conFactory
                .createConnection(hostConnectionPool);

        try {
            connection.execPing();
            Assert.fail("expected to throw");
        } catch (DynoConnectException e) {
            Assert.assertTrue("root cause should be connect exception",
                    Throwables.getRootCause(e) instanceof ConnectException);
        }
    }

    @Test
    public void testJedisConnFactory_authenticationRequired() throws Exception {
        redisServer = new RedisServerBuilder()
                .port(REDIS_PORT)
                .setting("requirepass password")
                .build();
        redisServer.start();

        Host noAuthHost = new HostBuilder().setHostname("localhost").setPort(REDIS_PORT).setRack(REDIS_RACK).setStatus(Status.Up).createHost();

        JedisConnectionFactory conFactory =
                new JedisConnectionFactory(new DynoOPMonitor("some-application-name"), null);
        ConnectionPoolConfiguration cpConfig = new ConnectionPoolConfigurationImpl("some-name");
        CountingConnectionPoolMonitor poolMonitor = new CountingConnectionPoolMonitor();
        HostConnectionPool<Jedis> hostConnectionPool =
                new HostConnectionPoolImpl<>(noAuthHost, conFactory, cpConfig, poolMonitor);
        Connection<Jedis> connection = conFactory
                .createConnection(hostConnectionPool);

        try {
            connection.execPing();
            Assert.fail("expected to throw");
        } catch (JedisDataException e) {
            Assert.assertEquals("NOAUTH Authentication required.", e.getMessage());
        }
    }

    @Test
    public void testJedisConnFactory_invalidPassword() throws Exception {
        redisServer = new RedisServerBuilder()
                .port(REDIS_PORT)
                .setting("requirepass password")
                .build();
        redisServer.start();

        Host authHost = new HostBuilder().setHostname("localhost").setPort(REDIS_PORT).setRack(REDIS_RACK).setStatus(Status.Up).setHashtag(null).setPassword("invalid-password").createHost();

        JedisConnectionFactory jedisConnectionFactory =
                new JedisConnectionFactory(new DynoOPMonitor("some-application-name"), null);

        ConnectionPoolConfiguration connectionPoolConfiguration = new ConnectionPoolConfigurationImpl(
                "some-name");
        HostConnectionPool<Jedis> hostConnectionPool = new HostConnectionPoolImpl<>(authHost,
                jedisConnectionFactory, connectionPoolConfiguration, new CountingConnectionPoolMonitor());
        Connection<Jedis> connection = jedisConnectionFactory
                .createConnection(hostConnectionPool);

        try {
            connection.execPing();
            Assert.fail("expected to throw");
        } catch (JedisDataException e) {
            Assert.assertEquals("ERR invalid password", e.getMessage());
        }
    }

    private DynoJedisClient constructJedisClient(TokenMapSupplier tokenMapSupplier,
                                                 HostSupplier hostSupplier) {

        final ConnectionPoolConfigurationImpl connectionPoolConfiguration =
                new ConnectionPoolConfigurationImpl(REDIS_RACK);
        connectionPoolConfiguration.withTokenSupplier(tokenMapSupplier);
        connectionPoolConfiguration.setLocalRack(REDIS_RACK);
        connectionPoolConfiguration.setLocalDataCenter(REDIS_DATACENTER);

        return new DynoJedisClient.Builder()
                .withApplicationName("some-application-name")
                .withDynomiteClusterName(REDIS_RACK)
                .withHostSupplier(hostSupplier)
                .withCPConfig(connectionPoolConfiguration)
                .build();
    }

    private static class TokenMapSupplierImpl implements TokenMapSupplier {

        private final HostToken localHostToken;

        private TokenMapSupplierImpl(Host host) {
            this.localHostToken = new HostToken(100000L, host);
        }

        @Override
        public List<HostToken> getTokens(Set<Host> activeHosts) {
            return Collections.singletonList(localHostToken);
        }

        @Override
        public HostToken getTokenForHost(Host host, Set<Host> activeHosts) {
            return localHostToken;
        }

    }
}
