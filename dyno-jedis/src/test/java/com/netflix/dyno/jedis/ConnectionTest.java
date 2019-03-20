package com.netflix.dyno.jedis;

import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ConnectionTest {
    private static final String REDIS_RACK = "rack-1c";
    private static final String REDIS_DATACENTER = "rack-1";
    private DynoJedisClient client;
    private UnitTestTokenMapAndHostSupplierImpl tokenMapAndHostSupplier;

    private static final int HEALTH_TRACKER_WAIT_MILLIS = 2 * 1000;
    private static final int POOL_RECONNECT_DELAY_MILLIS = 0;

    @Before
    public void before() throws IOException {
        MockitoAnnotations.initMocks(this);

        tokenMapAndHostSupplier = new UnitTestTokenMapAndHostSupplierImpl(1, REDIS_RACK);
        final ConnectionPoolConfigurationImpl connectionPoolConfiguration =
                new ConnectionPoolConfigurationImpl(REDIS_RACK)
                        .withTokenSupplier(tokenMapAndHostSupplier)
                        .withHashtag("{}")
                        .withPoolReconnectWaitMillis(POOL_RECONNECT_DELAY_MILLIS)
                        .withHealthTrackerDelayMills(HEALTH_TRACKER_WAIT_MILLIS)
                        .setLocalRack(REDIS_RACK)
                        .setLocalDataCenter(REDIS_DATACENTER);

        client = new DynoJedisClient.Builder()
                .withApplicationName("CommandTest")
                .withDynomiteClusterName(REDIS_RACK)
                .withHostSupplier(tokenMapAndHostSupplier)
                .withCPConfig(connectionPoolConfiguration)
                .build();
    }

    @After
    public void after() {
        client.stopClient();
        tokenMapAndHostSupplier.shutdown();
    }


    @Test
    public void testConnectionFailureWithDynoException() {

        int startConnectionCount = client.getConnPool().getHostPool(tokenMapAndHostSupplier.getHosts().get(0)).size();

        // shutdown server before operation
        tokenMapAndHostSupplier.pauseServer(0);
        for (int i = 0; i < startConnectionCount; i++) {
            try {
                client.set("testkey", "testval");
            } catch (DynoException de) {
                // ignore
            }
        }
        try {
            tokenMapAndHostSupplier.resumeServer(0);
            Thread.sleep(HEALTH_TRACKER_WAIT_MILLIS + POOL_RECONNECT_DELAY_MILLIS + 2 * 1000);
        } catch (InterruptedException ie) {
            // ignore
        }
        int endConnectionCount = client.getConnPool().getHostPool(tokenMapAndHostSupplier.getHosts().get(0)).size();

        assertEquals("ConnectionPool reconnect failed", startConnectionCount, endConnectionCount);
    }
}
