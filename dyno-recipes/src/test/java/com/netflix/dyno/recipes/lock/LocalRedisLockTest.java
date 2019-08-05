package com.netflix.dyno.recipes.lock;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostBuilder;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class LocalRedisLockTest extends DynoLockClientTest {
    private static final int REDIS_PORT = 8999;
    private static final String REDIS_RACK = "rack-1c";
    private static final String REDIS_DATACENTER = "rack-1";

    private RedisServer redisServer;

    @Before
    public void setUp() throws IOException {
        redisServer = new RedisServer(REDIS_PORT);
        redisServer.start();
        Assume.assumeFalse(System.getProperty("os.name").toLowerCase().startsWith("win"));
        host = new HostBuilder().setHostname("localhost").setDatastorePort(REDIS_PORT).setPort(REDIS_PORT).setRack(REDIS_RACK).setStatus(Host.Status.Up).createHost();
        tokenMapSupplier = new TokenMapSupplierImpl(host);
        dynoLockClient = constructDynoLockClient();
    }

    @After
    public void tearDown() {
        if (redisServer != null) {
            redisServer.stop();
        }
    }

    public DynoLockClient constructDynoLockClient() {
        HostSupplier hostSupplier = () -> Collections.singletonList(host);

        final ConnectionPoolConfigurationImpl connectionPoolConfiguration =
                new ConnectionPoolConfigurationImpl(REDIS_RACK);
        connectionPoolConfiguration.withTokenSupplier(tokenMapSupplier);
        connectionPoolConfiguration.setLocalRack(REDIS_RACK);
        connectionPoolConfiguration.setLocalDataCenter(REDIS_DATACENTER);
        connectionPoolConfiguration.setConnectToDatastore(true);

        return new DynoLockClient.Builder()
                .withApplicationName("test")
                .withDynomiteClusterName("testcluster")
                .withHostSupplier(hostSupplier)
                .withTokenMapSupplier(tokenMapSupplier)
                .withTimeout(50)
                .withConnectionPoolConfiguration(connectionPoolConfiguration)
                .withTimeoutUnit(TimeUnit.MILLISECONDS)
                .build();
    }

}
