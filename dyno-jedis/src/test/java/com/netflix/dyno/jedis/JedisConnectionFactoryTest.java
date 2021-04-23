package com.netflix.dyno.jedis;

import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostBuilder;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.impl.HostConnectionPoolImpl;
import com.netflix.dyno.contrib.DynoOPMonitor;
import com.netflix.dyno.jedis.JedisConnectionFactory.JedisConnection;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.embedded.RedisServer;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class JedisConnectionFactoryTest {

    private static final int REDIS_PORT = 8998;
    private static final String REDIS_RACK = "rack-1c";

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
    public void testJedisConnFactory_ensureSocketClose() throws Exception {
        redisServer = new RedisServer(REDIS_PORT);
        redisServer.start();
        
        Host noAuthHost = new HostBuilder()
                .setHostname("localhost")
                .setPort(REDIS_PORT)
                .setRack(REDIS_RACK)
                .setStatus(Status.Up)
                .createHost();

        JedisConnectionFactory conFactory = new JedisConnectionFactory(new DynoOPMonitor("some-application-name"), null);
        ConnectionPoolConfiguration cpConfig = new ConnectionPoolConfigurationImpl("some-name");
        CountingConnectionPoolMonitor poolMonitor = new CountingConnectionPoolMonitor();
        HostConnectionPool<Jedis> hostConnectionPool = new HostConnectionPoolImpl<>(noAuthHost, conFactory, cpConfig, poolMonitor);
        
        JedisConnection connection = (JedisConnection) conFactory.createConnection(hostConnectionPool);
        connection.execPing();        
        Assert.assertTrue(connection.getClient().isConnected());
        
        redisServer.stop();
        
        try {
            connection.close();     
            Assert.fail("expected to throw");
        } catch (JedisConnectionException e) {
            Assert.assertFalse(connection.getClient().isConnected());
        }
    }

}
