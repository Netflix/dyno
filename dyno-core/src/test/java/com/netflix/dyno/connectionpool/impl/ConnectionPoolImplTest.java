/**
 * Copyright 2016 Netflix, Inc.
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
package com.netflix.dyno.connectionpool.impl;

import com.netflix.dyno.connectionpool.AsyncOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.ConnectionFactory;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration.LoadBalancingStrategy;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostBuilder;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.HostConnectionStats;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.ListenableFuture;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.RetryPolicy;
import com.netflix.dyno.connectionpool.RetryPolicy.RetryPolicyFactory;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.FatalConnectionException;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.exception.PoolExhaustedException;
import com.netflix.dyno.connectionpool.exception.PoolOfflineException;
import com.netflix.dyno.connectionpool.exception.PoolTimeoutException;
import com.netflix.dyno.connectionpool.exception.ThrottledException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl.ErrorRateMonitorConfigImpl;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ConnectionPoolImplTest {

    private static class TestClient {

        private final AtomicInteger ops = new AtomicInteger(0);
    }

    private static TestClient client = new TestClient();

    private static ConnectionPoolConfigurationImpl cpConfig = new ConnectionPoolConfigurationImpl("TestClient");
    private static CountingConnectionPoolMonitor cpMonitor = new CountingConnectionPoolMonitor();

    private static class TestConnection implements Connection<TestClient> {

        private AtomicInteger ops = new AtomicInteger(0);
        private DynoConnectException ex;

        private HostConnectionPool<TestClient> hostPool;

        private TestConnection(HostConnectionPool<TestClient> pool) {
            this.hostPool = pool;
        }

        @Override
        public <R> OperationResult<R> execute(Operation<TestClient, R> op) throws DynoException {

            try {
                if (op != null) {
                    R r = op.execute(client, null);
                    return new OperationResultImpl<R>("Test", r, null);
                }
            } catch (DynoConnectException e) {
                ex = e;
                throw e;
            } finally {
                ops.incrementAndGet();
            }
            return null;
        }

        @Override
        public void close() {
        }

        @Override
        public Host getHost() {
            return hostPool.getHost();
        }

        @Override
        public void open() throws DynoException {
        }

        @Override
        public DynoConnectException getLastException() {
            return ex;
        }

        @Override
        public HostConnectionPool<TestClient> getParentConnectionPool() {
            return hostPool;
        }

        @Override
        public <R> ListenableFuture<OperationResult<R>> executeAsync(AsyncOperation<TestClient, R> op) throws DynoException {
            throw new RuntimeException("Not Implemented");
        }

        @Override
        public void execPing() {
        }

        @Override
        public ConnectionContext getContext() {
            return new ConnectionContextImpl();
        }
    }

    private static ConnectionFactory<TestClient> connFactory = new ConnectionFactory<TestClient>() {

        @Override
        public Connection<TestClient> createConnection(HostConnectionPool<TestClient> pool) throws DynoConnectException, ThrottledException {
            return new TestConnection(pool);
        }

        @Override
        public Connection<TestClient> createConnectionWithDataStore(HostConnectionPool<TestClient> pool) throws DynoConnectException {
            return null;
        }
    };

    private Host host1 = new HostBuilder().setHostname("host1").setPort(8080).setRack("localRack").setStatus(Status.Up).createHost();
    private Host host2 = new HostBuilder().setHostname("host2").setPort(8080).setRack("localRack").setStatus(Status.Up).createHost();
    private Host host3 = new HostBuilder().setHostname("host3").setPort(8080).setRack("localRack").setStatus(Status.Up).createHost();

    // Used for Cross Rack fallback testing
    private Host host4 = new HostBuilder().setHostname("host4").setPort(8080).setRack("remoteRack").setStatus(Status.Up).createHost();
    private Host host5 = new HostBuilder().setHostname("host5").setPort(8080).setRack("remoteRack").setStatus(Status.Up).createHost();
    private Host host6 = new HostBuilder().setHostname("host6").setPort(8080).setRack("remoteRack").setStatus(Status.Up).createHost();

    private final List<Host> hostSupplierHosts = new ArrayList<Host>();

    @Before
    public void beforeTest() {

        hostSupplierHosts.clear();

        client = new TestClient();
        cpConfig = new ConnectionPoolConfigurationImpl("TestClient").setLoadBalancingStrategy(LoadBalancingStrategy.RoundRobin);
        cpConfig.withHostSupplier(new HostSupplier() {

            @Override
            public List<Host> getHosts() {
                return hostSupplierHosts;
            }
        });

        cpConfig.setLocalRack("localRack");
        cpConfig.setLoadBalancingStrategy(LoadBalancingStrategy.RoundRobin);

        cpConfig.withTokenSupplier(getTokenMapSupplier());

        cpMonitor = new CountingConnectionPoolMonitor();
    }

    @Test
    public void testConnectionPoolNormal() throws Exception {

        final ConnectionPoolImpl<TestClient> pool = new ConnectionPoolImpl<TestClient>(connFactory, cpConfig, cpMonitor);
        hostSupplierHosts.add(host1);
        hostSupplierHosts.add(host2);

        pool.start();

        final Callable<Void> testLogic = new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                Thread.sleep(1000);
                return null;
            }
        };

        try {
            runTest(pool, testLogic);
            checkConnectionPoolMonitorStats(2);
        } finally {
            pool.shutdown();
        }
    }

    private void checkConnectionPoolMonitorStats(int numHosts) {
        checkConnectionPoolMonitorStats(numHosts, false);
    }

    private void checkConnectionPoolMonitorStats(int numHosts, boolean fallback) {
        System.out.println("Total ops: " + client.ops.get());
        Assert.assertTrue("Total ops: " + client.ops.get(), client.ops.get() > 0);

        Assert.assertEquals(client.ops.get(), cpMonitor.getOperationSuccessCount());

        if (!fallback) {
            Assert.assertEquals(0, cpMonitor.getOperationFailureCount());
        }

        Assert.assertEquals(0, cpMonitor.getOperationTimeoutCount());

        Assert.assertEquals(numHosts * cpConfig.getMaxConnsPerHost(), cpMonitor.getConnectionCreatedCount());
        Assert.assertEquals(0, cpMonitor.getConnectionCreateFailedCount());
        Assert.assertEquals(numHosts * cpConfig.getMaxConnsPerHost(), cpMonitor.getConnectionClosedCount());

        Assert.assertEquals(client.ops.get(), cpMonitor.getConnectionBorrowedCount());
        Assert.assertEquals(client.ops.get(), cpMonitor.getConnectionReturnedCount());
    }

    private TokenMapSupplier getTokenMapSupplier() {

        /**
         cqlsh:dyno_bootstrap> select "availabilityZone","hostname","token" from tokens where "appId" = 'dynomite_redis_puneet';

         availabilityZone | hostname                                   | token
         ------------------+--------------------------------------------+------------
         us-east-1c |  ec2-54-83-179-213.compute-1.amazonaws.com | 1383429731
         us-east-1c |  ec2-54-224-184-99.compute-1.amazonaws.com |  309687905
         us-east-1c |  ec2-54-91-190-159.compute-1.amazonaws.com | 3530913377
         us-east-1c |   ec2-54-81-31-218.compute-1.amazonaws.com | 2457171554
         us-east-1e | ec2-54-198-222-153.compute-1.amazonaws.com |  309687905
         us-east-1e | ec2-54-198-239-231.compute-1.amazonaws.com | 2457171554
         us-east-1e |  ec2-54-226-212-40.compute-1.amazonaws.com | 1383429731
         us-east-1e | ec2-54-197-178-229.compute-1.amazonaws.com | 3530913377

         cqlsh:dyno_bootstrap>
         */
        final Map<Host, HostToken> tokenMap = new HashMap<Host, HostToken>();

        tokenMap.put(host1, new HostToken(309687905L, host1));
        tokenMap.put(host2, new HostToken(1383429731L, host2));
        tokenMap.put(host3, new HostToken(2457171554L, host3));
        tokenMap.put(host4, new HostToken(309687905L, host4));
        tokenMap.put(host5, new HostToken(1383429731L, host5));
        tokenMap.put(host6, new HostToken(2457171554L, host6));

        return new TokenMapSupplier() {

            @Override
            public List<HostToken> getTokens(Set<Host> activeHosts) {
                if (activeHosts.size() < tokenMap.size()) {
                    List<HostToken> hostTokens = new ArrayList<HostToken>(activeHosts.size());
                    Iterator<Host> iterator = activeHosts.iterator();
                    while (iterator.hasNext()) {
                        Host activeHost = (Host) iterator.next();
                        hostTokens.add(tokenMap.get(activeHost));
                    }
                    return hostTokens;
                } else {
                    return new ArrayList<HostToken>(tokenMap.values());
                }
            }

            @Override
            public HostToken getTokenForHost(Host host, Set<Host> activeHosts) {
                return tokenMap.get(host);
            }

        };
    }

    @Test
    public void testAddingNewHosts() throws Exception {

        final ConnectionPoolImpl<TestClient> pool = new ConnectionPoolImpl<TestClient>(connFactory, cpConfig, cpMonitor);
        hostSupplierHosts.add(host1);
        hostSupplierHosts.add(host2);

        pool.start();

        final Callable<Void> testLogic = new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                Thread.sleep(1000);
                pool.addHost(host3);
                Thread.sleep(1000);
                return null;
            }
        };

        runTest(pool, testLogic);

        checkConnectionPoolMonitorStats(3);

        checkHostStats(host1);
        checkHostStats(host2);
        checkHostStats(host3);

        HostConnectionStats h1Stats = cpMonitor.getHostStats().get(host1);
        HostConnectionStats h2Stats = cpMonitor.getHostStats().get(host2);
        HostConnectionStats h3Stats = cpMonitor.getHostStats().get(host3);

        Assert.assertTrue("h3Stats: " + h3Stats + " h1Stats: " + h1Stats, h1Stats.getOperationSuccessCount() > h3Stats.getOperationSuccessCount());
        Assert.assertTrue("h3Stats: " + h3Stats + " h2Stats: " + h2Stats, h2Stats.getOperationSuccessCount() > h3Stats.getOperationSuccessCount());
    }

    private void checkHostStats(Host host) {

        HostConnectionStats hStats = cpMonitor.getHostStats().get(host);
        Assert.assertTrue("host ops: " + hStats.getOperationSuccessCount(), hStats.getOperationSuccessCount() > 0);
        Assert.assertEquals(0, hStats.getOperationErrorCount());
        Assert.assertEquals(cpConfig.getMaxConnsPerHost(), hStats.getConnectionsCreated());
        Assert.assertEquals(0, hStats.getConnectionsCreateFailed());
        Assert.assertEquals(cpConfig.getMaxConnsPerHost(), hStats.getConnectionsClosed());
        Assert.assertEquals(hStats.getOperationSuccessCount(), hStats.getConnectionsBorrowed());
        Assert.assertEquals(hStats.getOperationSuccessCount(), hStats.getConnectionsReturned());
    }

    @Test
    public void testRemovingHosts() throws Exception {

        final ConnectionPoolImpl<TestClient> pool = new ConnectionPoolImpl<TestClient>(connFactory, cpConfig, cpMonitor);
        hostSupplierHosts.add(host1);
        hostSupplierHosts.add(host2);
        hostSupplierHosts.add(host3);

        pool.start();

        final Callable<Void> testLogic = new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                Thread.sleep(1000);
                pool.removeHost(host2);
                Thread.sleep(1000);
                return null;
            }
        };

        runTest(pool, testLogic);

        checkConnectionPoolMonitorStats(3);

        checkHostStats(host1);
        checkHostStats(host2);
        checkHostStats(host3);

        HostConnectionStats h1Stats = cpMonitor.getHostStats().get(host1);
        HostConnectionStats h2Stats = cpMonitor.getHostStats().get(host2);
        HostConnectionStats h3Stats = cpMonitor.getHostStats().get(host3);

        Assert.assertTrue("h1Stats: " + h1Stats + " h2Stats: " + h2Stats, h1Stats.getOperationSuccessCount() > h2Stats.getOperationSuccessCount());
        Assert.assertTrue("h2Stats: " + h2Stats + " h3Stats: " + h3Stats, h3Stats.getOperationSuccessCount() > h2Stats.getOperationSuccessCount());
    }

    @Test(expected = NoAvailableHostsException.class)
    public void testNoAvailableHosts() throws Exception {

        final ConnectionPoolImpl<TestClient> pool = new ConnectionPoolImpl<TestClient>(connFactory, cpConfig, cpMonitor);
        pool.start();

        try {
            executeTestClientOperation(pool);
        } finally {
            pool.shutdown();
        }
    }

    @Test
    public void testIdleWhenNoAvailableHosts() throws Exception {
        final ConnectionPoolImpl<TestClient> pool = new ConnectionPoolImpl<TestClient>(connFactory, cpConfig, cpMonitor);

        try {
            pool.start();
        } catch (NoAvailableHostsException nah) {
            pool.idle();
            Thread.sleep(1000);

            // calling idle() again should have no effect and will throw an exception if the pool has already been
            // started
            pool.idle();
        } finally {
            pool.shutdown();
        }
    }

    @Test
    public void testPoolTimeout() throws Exception {

        final ConnectionPoolImpl<TestClient> pool = new ConnectionPoolImpl<TestClient>(connFactory, cpConfig, cpMonitor);
        hostSupplierHosts.add(host1);
        hostSupplierHosts.add(host2);
        hostSupplierHosts.add(host3);

        pool.start();

        // Now exhaust all 9 connections, so that the 10th one can fail with PoolExhaustedException
        final ExecutorService threadPool = Executors.newFixedThreadPool(9);

        final Callable<Void> blockConnectionForSomeTime = new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                try {
                    Thread.sleep(10000);  // sleep for a VERY long time to ensure pool exhaustion
                } catch (InterruptedException e) {
                    // just return
                }
                return null;
            }
        };

        final CountDownLatch latch = new CountDownLatch(9);
        for (int i = 0; i < 9; i++) {
            threadPool.submit(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    latch.countDown();
                    executeTestClientOperation(pool, blockConnectionForSomeTime);
                    return null;
                }
            });
        }

        latch.await();
        Thread.sleep(100); // wait patiently for all threads to have blocked the connections

        try {
            executeTestClientOperation(pool);
            Assert.fail("TEST FAILED");
        } catch (PoolTimeoutException e) {
            threadPool.shutdownNow();
            pool.shutdown();
        }
    }

    @Test(expected = PoolOfflineException.class)
    public void testPoolOffline() {
        final ConnectionPoolImpl<TestClient> pool = new ConnectionPoolImpl<TestClient>(connFactory, cpConfig, cpMonitor);
        hostSupplierHosts.add(host1);

        pool.start();

        // The test logic is simply to throw an exception so that errors will recorded by the health tracker
        // which will mark the pool as Down
        final Callable<Void> testLogic = new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                throw new PoolExhaustedException(pool.getHostPool(host1), "pool exhausted");
            }
        };

        try {
            executeTestClientOperation(pool, testLogic);
        } finally {
            pool.shutdown();
        }
    }


    @Test
    public void testHostEvictionDueToErrorRates() throws Exception {

        // First configure the error rate monitor
        ErrorRateMonitorConfigImpl errConfig = new ErrorRateMonitorConfigImpl();
        errConfig.checkFrequency = 1;
        errConfig.window = 1;
        errConfig.suppressWindow = 60;

        errConfig.addThreshold(10, 1, 100);

        final AtomicReference<String> badHost = new AtomicReference<String>();

        final ConnectionFactory<TestClient> badConnectionFactory = new ConnectionFactory<TestClient>() {

            @Override
            public Connection<TestClient> createConnection(final HostConnectionPool<TestClient> pool) throws DynoConnectException {

                return new TestConnection(pool) {

                    @Override
                    public <R> OperationResult<R> execute(Operation<TestClient, R> op) throws DynoException {
                        if (pool.getHost().getHostAddress().equals(badHost.get())) {
                            throw new FatalConnectionException("Fail for bad host");
                        }
                        return super.execute(op);
                    }
                };
            }

            @Override
            public Connection<TestClient> createConnectionWithDataStore(HostConnectionPool<TestClient> pool) throws DynoConnectException {
                return null;
            }
        };

        final ConnectionPoolImpl<TestClient> pool = new ConnectionPoolImpl<TestClient>(badConnectionFactory, cpConfig, cpMonitor);
        hostSupplierHosts.add(host1);
        hostSupplierHosts.add(host2);
        hostSupplierHosts.add(host3);

        pool.start();

        final Callable<Void> testLogic = new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                Thread.sleep(2000);
                badHost.set("host2");
                Thread.sleep(2000);
                return null;
            }
        };

        runTest(pool, testLogic);

        Assert.assertTrue("Total ops: " + client.ops.get(), client.ops.get() > 0);
        Assert.assertTrue("Total errors: " + cpMonitor.getOperationFailureCount(), cpMonitor.getOperationFailureCount() > 0);

        Assert.assertEquals(3 * cpConfig.getMaxConnsPerHost(), cpMonitor.getConnectionCreatedCount());
        Assert.assertEquals(0, cpMonitor.getConnectionCreateFailedCount());
        Assert.assertEquals(3 * cpConfig.getMaxConnsPerHost(), cpMonitor.getConnectionClosedCount());

        Assert.assertEquals(client.ops.get() + cpMonitor.getOperationFailureCount(), cpMonitor.getConnectionBorrowedCount());
        Assert.assertEquals(client.ops.get() + cpMonitor.getOperationFailureCount(), cpMonitor.getConnectionReturnedCount());

        checkHostStats(host1);
        checkHostStats(host3);

        HostConnectionStats h2Stats = cpMonitor.getHostStats().get(host2);
        Assert.assertEquals(cpMonitor.getOperationFailureCount(), h2Stats.getOperationErrorCount());

    }

    @Test
    public void testCrossRackFailover() throws Exception {

        final RetryNTimes retry = new RetryNTimes(3, true);
        final RetryPolicyFactory rFactory = new RetryNTimes.RetryPolicyFactory() {
            @Override
            public RetryPolicy getRetryPolicy() {
                return retry;
            }
        };
        cpConfig.setRetryPolicyFactory(rFactory);
        int numHosts = 6;

        final ConnectionPoolImpl<TestClient> pool = new ConnectionPoolImpl<TestClient>(connFactory, cpConfig, cpMonitor);
        hostSupplierHosts.add(host1);
        hostSupplierHosts.add(host2);
        hostSupplierHosts.add(host3);
        hostSupplierHosts.add(host4);
        hostSupplierHosts.add(host5);
        hostSupplierHosts.add(host6);

        pool.start();

        final Callable<Void> testLogic = new Callable<Void>() {

            Integer count = 0;

            @Override
            public Void call() throws Exception {
                if (count == 0) {
                    ++count;
                    throw new DynoException("1st try - FAILURE");
                } else {
                    Thread.sleep(1000);
                }
                return null;
            }
        };

        try {
            executeTestClientOperation(pool, testLogic);

            System.out.println("Total ops: " + client.ops.get());
            Assert.assertTrue("Total ops: " + client.ops.get(), client.ops.get() > 0);

            Assert.assertEquals(client.ops.get(), cpMonitor.getOperationSuccessCount());
            Assert.assertEquals(1, cpMonitor.getOperationFailureCount());

            Assert.assertEquals(0, cpMonitor.getOperationTimeoutCount());

            Assert.assertEquals(numHosts * cpConfig.getMaxConnsPerHost(), cpMonitor.getConnectionCreatedCount());
            Assert.assertEquals(0, cpMonitor.getConnectionCreateFailedCount());

        } finally {
            pool.shutdown();
        }
    }

    @Test
    public void testWithRetries() {

        final ConnectionFactory<TestClient> badConnectionFactory = new ConnectionFactory<TestClient>() {
            @Override
            public Connection<TestClient> createConnection(final HostConnectionPool<TestClient> pool) throws DynoConnectException {
                return new TestConnection(pool) {
                    @Override
                    public <R> OperationResult<R> execute(Operation<TestClient, R> op) throws DynoException {
                        throw new DynoException("Fail for bad host");
                    }
                };
            }

            @Override
            public Connection<TestClient> createConnectionWithDataStore(HostConnectionPool<TestClient> pool) throws DynoConnectException {
                return null;
            }
        };

        final RetryNTimes retry = new RetryNTimes(3, false);
        final RetryPolicyFactory rFactory = new RetryNTimes.RetryPolicyFactory() {
            @Override
            public RetryPolicy getRetryPolicy() {
                return retry;
            }
        };

        final ConnectionPoolImpl<TestClient> pool =
                new ConnectionPoolImpl<TestClient>(badConnectionFactory, cpConfig.setRetryPolicyFactory(rFactory), cpMonitor);
        hostSupplierHosts.add(host1);

        pool.start();


        try {
            executeTestClientOperation(pool, null);
            Assert.fail("Test failed: expected PoolExhaustedException");
        } catch (DynoException e) {
            Assert.assertEquals("Retry: " + retry.getAttemptCount(), 4, retry.getAttemptCount());
        } finally {
            pool.shutdown();
        }
    }

    @Test(expected = NoAvailableHostsException.class)
    public void testHostsDownDuringStartup() {

        final ConnectionPoolImpl<TestClient> pool = new ConnectionPoolImpl<TestClient>(connFactory, cpConfig, cpMonitor);

        hostSupplierHosts.add(new HostBuilder().setHostname("host1_down").setPort(8080).setRack("localRack").setStatus(Status.Down).createHost());
        hostSupplierHosts.add(new HostBuilder().setHostname("host2_down").setPort(8080).setRack("localRack").setStatus(Status.Down).createHost());
        hostSupplierHosts.add(new HostBuilder().setHostname("host3_down").setPort(8080).setRack("localRack").setStatus(Status.Down).createHost());

        pool.start();

    }

    private void executeTestClientOperation(final ConnectionPoolImpl<TestClient> pool) {
        executeTestClientOperation(pool, null);
    }

    private void executeTestClientOperation(final ConnectionPoolImpl<TestClient> pool, final Callable<Void> customLogic) {
        pool.executeWithFailover(new Operation<TestClient, Integer>() {

            @Override
            public Integer execute(TestClient client, ConnectionContext state) throws DynoException {
                if (customLogic != null) {
                    try {
                        customLogic.call();
                    } catch (DynoException de) {
                        throw de;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                client.ops.incrementAndGet();
                return 1;
            }

            @Override
            public String getName() {
                return "TestOperation";
            }


            @Override
            public String getStringKey() {
                return "TestOperation";
            }

            @Override
            public byte[] getBinaryKey() {
                return null;
            }
        });
    }

    private void runTest(final ConnectionPoolImpl<TestClient> pool, final Callable<Void> customTestLogic) throws Exception {

        int nThreads = 1;
        final ExecutorService threadPool = Executors.newFixedThreadPool(nThreads);
        final AtomicBoolean stop = new AtomicBoolean(false);

        final CountDownLatch latch = new CountDownLatch(nThreads);

        for (int i = 0; i < nThreads; i++) {
            threadPool.submit(new Callable<Void>() {

                @Override
                public Void call() throws Exception {
                    try {
                        while (!stop.get() && !Thread.currentThread().isInterrupted()) {
                            try {
                                pool.executeWithFailover(new Operation<TestClient, Integer>() {

                                    @Override
                                    public Integer execute(TestClient client, ConnectionContext state) throws DynoException {
                                        client.ops.incrementAndGet();
                                        return 1;
                                    }

                                    @Override
                                    public String getName() {
                                        return "TestOperation";
                                    }

                                    @Override
                                    public String getStringKey() {
                                        return "TestOperation";
                                    }

                                    @Override
                                    public byte[] getBinaryKey() {
                                        return null;
                                    }

                                });
                            } catch (DynoException e) {
//								System.out.println("FAILED Test Worker operation: " + e.getMessage());
//								e.printStackTrace();
                            }
                        }

                    } finally {
                        latch.countDown();
                    }
                    return null;
                }
            });
        }

        customTestLogic.call();

        stop.set(true);
        latch.await();
        threadPool.shutdownNow();
        pool.shutdown();
    }
}
