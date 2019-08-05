/*******************************************************************************
 * Copyright 2018 Netflix
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
package com.netflix.dyno.recipes.lock;

import com.netflix.discovery.EurekaClient;
import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.contrib.ArchaiusConnectionPoolConfiguration;
import com.netflix.dyno.contrib.DynoCPMonitor;
import com.netflix.dyno.contrib.DynoOPMonitor;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.jedis.DynoJedisUtils;
import com.netflix.dyno.recipes.lock.command.CheckAndRunHost;
import com.netflix.dyno.recipes.lock.command.ExtendHost;
import com.netflix.dyno.recipes.lock.command.LockHost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Client for acquiring locks similar to the redlock implementation https://redis.io/topics/distlock
 *
 * This locking mechanism does not give the guarantees for safety but can be used for efficiency.
 *
 * We assume some amount of clock skew between the client and server instances. Any major deviations in this
 * will result in reduced accuracy of the lock.
 *
 * In the common locking case we rely on TTL's being set on a majority of redis servers in order to achieve the right
 * locking characteristic. If we are unable to reach a majority of hosts when we try to acquire a lock or extend a lock.
 *
 * We try to release locks on all the hosts when we either shutdown or are unable to lock on a majority of hosts successfully
 *
 * These are the main edge cases where locking might not be mutually exclusive
 *
 * 1. An instance we acquired the lock on goes down and gets replaced by a new instance before the lock TTL expires.
 *    As suggested in the blog above, we need to ensure that new servers take longer than TTL time to come up so that
 *    any existing locks would've expired by then.(the client side code cannot control how quickly your servers come up). This can
 *    become a real issue if you're bringing up new servers in containers which can come up in a few seconds and you are holding locks for longer
 *    than the amount of time it takes for a new server to come up.
 * 2. You have the JVM go into GC from when you acquired the lock to when you are going to modify the resource blocked by the lock.
 *    The client needs to ensure that GC is not happening for a long enough time that it can effect the assumption of lock being held (or have an alert on long GCs).
 *
 */
public class DynoLockClient {

    private static final Logger logger = LoggerFactory.getLogger(DynoJedisClient.class);

    private final ConnectionPool pool;
    private final VotingHostsSelector votingHostsSelector;
    private final ExecutorService service;
    private final int quorum;
    // We assume a small amount of clock drift.
    private final double CLOCK_DRIFT = 0.01;
    private TimeUnit timeoutUnit;
    private long timeout;
    private final ConcurrentHashMap<String, String> resourceKeyMap = new ConcurrentHashMap<>();

    public DynoLockClient(ConnectionPool pool, VotingHostsSelector votingHostsSelector, long timeout, TimeUnit unit) {
        this.pool = pool;
        this.votingHostsSelector = votingHostsSelector;
        // Threads for locking and unlocking
        this.service = Executors.newCachedThreadPool();
        this.quorum = votingHostsSelector.getVotingSize() / 2 + 1;
        this.timeout = timeout;
        this.timeoutUnit = unit;
        // We want to release all locks in case of a graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> cleanup()));
    }

    public void setTimeoutUnit(TimeUnit timeoutUnit) {
        this.timeoutUnit = timeoutUnit;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    private static String getRandomString() {
        return UUID.randomUUID().toString();
    }

    /**
     * Gets list of resources which are locked by the client
     * @return
     */
    public List<String> getLockedResources() {
        return new ArrayList<>(resourceKeyMap.keySet());
    }

    /**
     * Release the lock (if held) on the resource
     * @param resource
     */
    public void releaseLock(String resource) {
        if (!checkResourceExists(resource)) {
            logger.info("No lock held on {}", resource);
            return;
        }
        CountDownLatch latch = new CountDownLatch(votingHostsSelector.getVotingSize());
        votingHostsSelector.getVotingHosts().getEntireList().stream()
                .map(host -> new CheckAndRunHost(host, pool, "del", resource, resourceKeyMap.get(resource)))
                .forEach(ulH -> CompletableFuture.supplyAsync(ulH, service)
                        .thenAccept(result -> latch.countDown())
                );
        boolean latchValue = false;
        try {
            latchValue = latch.await(timeout, timeoutUnit);
        } catch (InterruptedException e) {
            logger.info("Interrupted while releasing the lock for resource {}", resource);
        }
        if (latchValue) {
            logger.info("Released lock on {}", resource);
        } else {
            logger.info("Timed out before we could release the lock");
        }
        resourceKeyMap.remove(resource);
    }

    /**
     * Return a timer task which will recursively schedule itself when extension is successful.
     * @param runJob
     * @param resource
     * @param ttlMS
     * @param extensionFailed - This function gets called with the resource name when extension was unsuccessful.
     * @return
     */
    private TimerTask getExtensionTask(Timer runJob, String resource, long ttlMS, Consumer<String> extensionFailed) {
        return new TimerTask() {
            @Override
            public void run() {
                long extendedValue = extendLock(resource, ttlMS);
                if (extendedValue > 0) {
                    logger.info("Extended lock on {} for {} MS", resource, ttlMS);
                    TimerTask task = getExtensionTask(runJob, resource, ttlMS, extensionFailed);
                    runJob.schedule(task, extendedValue/2);
                    return;
                }
                extensionFailed.accept(resource);
            }
        };
    }

    /**
     * Try to acquire lock on resource for ttlMS and keep extending it by ttlMS when its about to expire.
     * Calls the failure function with the resource when extension fails.
     * @param resource The resource on which you want to acquire a lock
     * @param ttlMS The amount of time for which we need to acquire the lock. We try to extend the lock every ttlMS / 2
     * @param failure This function will be called with the resource which could not be locked. This function is called
     *                even if the client released the lock.
     * @return returns true if we were able to successfully acqurie the lock.
     */
    public boolean acquireLock(String resource, long ttlMS, Consumer<String> failure) {
        return acquireLockWithExtension(resource, ttlMS, (r) -> {
            releaseLock(r);
            failure.accept(r);
        });
    }

    /**
     * Try to acquire the lock and schedule extension jobs recursively until extension fails.
     * @param resource
     * @param ttlMS
     * @param extensionFailedCallback - gets called with the resource name when extension fails.
     * @return
     */
    private boolean acquireLockWithExtension(String resource, long ttlMS, Consumer<String> extensionFailedCallback) {
        long acquireResult = acquireLock(resource, ttlMS);
        if (acquireResult > 0) {
            Timer runJob = new Timer(resource, true);
            runJob.schedule(getExtensionTask(runJob, resource, ttlMS, extensionFailedCallback), acquireResult/2);
            return true;
        }
        return false;
    }

    /**
     * This function is used to acquire / extend the lock on at least quorum number of hosts
     * @param resource
     * @param ttlMS
     * @param extend
     * @return
     */
    private long runLockHost(String resource, long ttlMS, boolean extend) {
        long startTime = Instant.now().toEpochMilli();
        long drift = Math.round(ttlMS * CLOCK_DRIFT) + 2;
        LockResource lockResource = new LockResource(resource, ttlMS);
        CountDownLatch latch = new CountDownLatch(quorum);
        if (extend) {
            votingHostsSelector.getVotingHosts().getEntireList().stream()
                    .map(host -> new ExtendHost(host, pool, lockResource, latch, resourceKeyMap.get(resource)))
                    .forEach(lH -> CompletableFuture.supplyAsync(lH, service));
        } else {
            votingHostsSelector.getVotingHosts().getEntireList().stream()
                    .map(host -> new LockHost(host, pool, lockResource, latch, resourceKeyMap.get(resource)))
                    .forEach(lH -> CompletableFuture.supplyAsync(lH, service));
        }
        awaitLatch(latch, resource);
        long validity = 0L;
        if (lockResource.getLocked() >= quorum) {
            long timeElapsed = Instant.now().toEpochMilli() - startTime;
            validity = ttlMS - timeElapsed - drift;
        } else {
            releaseLock(resource);
        }
        return validity;
    }

    /**
     * Tries to acquire lock on resource for ttlMS milliseconds. Returns the amount of time for which the lock was acquired
     * @param resource
     * @param ttlMS
     * @return
     */
    public long acquireLock(String resource, long ttlMS) {
        resourceKeyMap.putIfAbsent(resource, getRandomString());
        return runLockHost(resource, ttlMS, false);
    }

    /**
     * Check if we still have the lock on a resource
     * @param resource
     * @return
     */
    boolean checkResourceExists(String resource) {
        if (!resourceKeyMap.containsKey(resource)) {
            logger.info("No lock held on {}", resource);
            return false;
        } else {
            return true;
        }
    }

    private boolean awaitLatch(CountDownLatch latch, String resource) {
        try {
            return latch.await(timeout, timeoutUnit);
        } catch (InterruptedException e) {
            logger.info("Interrupted while checking the lock for resource {}", resource);
            return false;
        }
    }

    /**
     * Check and get the ttls for the lock if it exists. This returns the minimum of ttls returned across the hosts
     * @param resource
     * @return
     */
    public long checkLock(String resource) {
        if (!checkResourceExists(resource)) {
            return 0;
        } else {
            long startTime = Instant.now().toEpochMilli();
            CopyOnWriteArrayList<Long> resultTtls = new CopyOnWriteArrayList<>();
            CountDownLatch latch = new CountDownLatch(quorum);
            votingHostsSelector.getVotingHosts().getEntireList().stream()
                    .map(host -> new CheckAndRunHost(host, pool, "pttl", resource, resourceKeyMap.get(resource)))
                    .forEach(checkAndRunHost -> CompletableFuture.supplyAsync(checkAndRunHost, service)
                            .thenAccept(r -> {
                                String result = r.getResult().toString();
                                // The lua script returns 0 if we have lost the lock or we get -2 if the ttl expired on
                                // the key when we checked for the pttl.
                                if (result.equals("0") || result.equals("-2")) {
                                    logger.info("Lock not present on host");
                                } else {
                                    resultTtls.add(Long.valueOf(result));
                                    latch.countDown();
                                }
                            })
                    );
            boolean latchValue = awaitLatch(latch, resource);
            if (latchValue) {
                long timeElapsed = Instant.now().toEpochMilli() - startTime;
                logger.info("Checked lock on {}", resource);
                return Collections.min(resultTtls) - timeElapsed;
            } else {
                logger.info("Timed out before we could check the lock");
                return 0;
            }
        }
    }

    /**
     * Try to extend lock by ttlMS
     * @param resource
     * @param ttlMS
     * @return
     */
    public long extendLock(String resource, long ttlMS) {
        if (!checkResourceExists(resource)) {
            logger.info("Could not extend lock since its already released");
            return 0;
        } else {
            return runLockHost(resource, ttlMS, true);
        }
    }

    /**
     * Release all locks to clean.
     */
    public void cleanup() {
        resourceKeyMap.keySet().stream().forEach(this::releaseLock);
    }

    /**
     * Log all the lock resources held right now.
     */
    public void logLocks() {
        resourceKeyMap.entrySet().stream()
                .forEach(e -> logger.info("Resource: {}, Key: {}", e.getKey(), e.getValue()));
    }

    public static class Builder {
        private String appName;
        private String clusterName;
        private TokenMapSupplier tokenMapSupplier;
        private HostSupplier hostSupplier;
        private ConnectionPoolConfigurationImpl cpConfig;
        private EurekaClient eurekaClient;
        private long timeout;
        private TimeUnit timeoutUnit;

        public Builder() {
        }

        public Builder withTimeout(long timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder withTimeoutUnit(TimeUnit unit) {
            this.timeoutUnit = unit;
            return this;
        }

        public Builder withEurekaClient(EurekaClient eurekaClient) {
            this.eurekaClient = eurekaClient;
            return this;
        }

        public Builder withApplicationName(String applicationName) {
            appName = applicationName;
            return this;
        }

        public Builder withDynomiteClusterName(String cluster) {
            clusterName = cluster;
            return this;
        }

        public Builder withHostSupplier(HostSupplier hSupplier) {
            hostSupplier = hSupplier;
            return this;
        }

        public Builder withTokenMapSupplier(TokenMapSupplier tokenMapSupplier) {
            this.tokenMapSupplier = tokenMapSupplier;
            return this;
        }

        public Builder withConnectionPoolConfiguration(ConnectionPoolConfigurationImpl cpConfig) {
            this.cpConfig = cpConfig;
            return this;
        }

        public DynoLockClient build() {
            assert (appName != null);
            assert (clusterName != null);

            if (cpConfig == null) {
                cpConfig = new ArchaiusConnectionPoolConfiguration(appName);
                logger.info("Dyno Client runtime properties: " + cpConfig.toString());
            }

            // We do not want to fallback to other azs which is the normal opertion for the connection pool
            cpConfig.setFallbackEnabled(false);
            cpConfig.setConnectToDatastore(true);

            return buildDynoLockClient();
        }

        private DynoLockClient buildDynoLockClient() {
            DynoOPMonitor opMonitor = new DynoOPMonitor(appName);
            ConnectionPoolMonitor cpMonitor = new DynoCPMonitor(appName);

            DynoJedisUtils.updateConnectionPoolConfig(cpConfig, hostSupplier, tokenMapSupplier, eurekaClient,
                    clusterName);
            if (tokenMapSupplier == null)
                tokenMapSupplier = cpConfig.getTokenSupplier();
            final ConnectionPool<Jedis> pool = DynoJedisUtils.createConnectionPool(appName, opMonitor, cpMonitor,
                    cpConfig, null);
            VotingHostsFromTokenRange votingHostSelector = new VotingHostsFromTokenRange(hostSupplier, tokenMapSupplier,
                    cpConfig.getLockVotingSize());

            return new DynoLockClient(pool, votingHostSelector, timeout, timeoutUnit);
        }
    }
}
