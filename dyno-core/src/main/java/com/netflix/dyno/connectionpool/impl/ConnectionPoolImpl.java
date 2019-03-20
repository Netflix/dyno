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
package com.netflix.dyno.connectionpool.impl;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.dyno.connectionpool.*;
import com.netflix.dyno.connectionpool.exception.FatalConnectionException;
import com.netflix.dyno.connectionpool.exception.PoolExhaustedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.impl.HostConnectionPoolFactory.Type;
import com.netflix.dyno.connectionpool.impl.health.ConnectionPoolHealthTracker;
import com.netflix.dyno.connectionpool.impl.lb.HostSelectionWithFallback;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Predicate;

import javax.management.*;

/**
 * Main implementation class for {@link ConnectionPool} The pool deals with a
 * bunch of other components and brings together all the functionality for Dyno.
 * Hence this is where all the action happens.
 * 
 * Here are the top salient features of this class.
 * 
 * 1. Manages a collection of {@link HostConnectionPool}s for all the
 * {@link Host}s that it receives from the {@link HostSupplier}
 * 
 * 2. Manages adding and removing hosts as dictated by the HostSupplier.
 * 
 * 3. Enables execution of {@link Operation} using a {@link Connection} borrowed
 * from the {@link HostConnectionPool}s
 * 
 * 4. Employs a {@link HostSelectionStrategy} (basically Round Robin or Token
 * Aware) when executing operations
 * 
 * 5. Uses a health check monitor for tracking error rates from the execution of
 * operations. The health check monitor can then decide to recycle a given
 * HostConnectionPool, and execute requests using fallback HostConnectionPools
 * for remote DCs.
 * 
 * 6. Uses {@link RetryPolicy} when executing operations for better resilience
 * against transient failures.
 * 
 * @see {@link HostSupplier} {@link Host} {@link HostSelectionStrategy}
 * @see {@link Connection} {@link ConnectionFactory}
 *      {@link ConnectionPoolConfiguration} {@link ConnectionPoolMonitor}
 * @see {@link ConnectionPoolHealthTracker}
 * 
 * @author poberai
 *
 * @param <CL>
 */
public class ConnectionPoolImpl<CL> implements ConnectionPool<CL>, TopologyView {

    private static final Logger Logger = LoggerFactory.getLogger(ConnectionPoolImpl.class);

    private final ConcurrentHashMap<Host, HostConnectionPool<CL>> cpMap = new ConcurrentHashMap<Host, HostConnectionPool<CL>>();
    private final ConnectionPoolHealthTracker<CL> cpHealthTracker;

    private final HostConnectionPoolFactory<CL> hostConnPoolFactory;
    private final ConnectionFactory<CL> connFactory;
    private final ConnectionPoolConfiguration cpConfiguration;
    private final ConnectionPoolMonitor cpMonitor;

    private final ScheduledExecutorService idleThreadPool = Executors.newSingleThreadScheduledExecutor();

    private final HostsUpdater hostsUpdater;
    private final ScheduledExecutorService connPoolThreadPool = Executors.newScheduledThreadPool(1);

    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean idling = new AtomicBoolean(false);

    private HostSelectionWithFallback<CL> selectionStrategy;

    private Type poolType;

    public ConnectionPoolImpl(ConnectionFactory<CL> cFactory, ConnectionPoolConfiguration cpConfig,
        ConnectionPoolMonitor cpMon) {
        this(cFactory, cpConfig, cpMon, Type.Sync);
    }

    public ConnectionPoolImpl(ConnectionFactory<CL> cFactory, ConnectionPoolConfiguration cpConfig,
        ConnectionPoolMonitor cpMon, Type type) {
        this.connFactory = cFactory;
        this.cpConfiguration = cpConfig;
        this.cpMonitor = cpMon;
        this.poolType = type;

        this.cpHealthTracker = new ConnectionPoolHealthTracker<CL>(cpConfiguration, connPoolThreadPool);

        switch (type) {
        case Sync:
            hostConnPoolFactory = new SyncHostConnectionPoolFactory();
            break;
        case Async:
            hostConnPoolFactory = new AsyncHostConnectionPoolFactory();
            break;
        default:
            throw new RuntimeException("unknown type");
        }

        this.hostsUpdater = new HostsUpdater(cpConfiguration.getHostSupplier(), cpConfiguration.getTokenSupplier());
    }


    public String getName() {
        return cpConfiguration.getName();
    }

    public ConnectionPoolMonitor getMonitor() {
        return cpMonitor;
    }

    public ConnectionPoolHealthTracker<CL> getHealthTracker() {
        return cpHealthTracker;
    }

    @Override
    public boolean isIdle() {
        return idling.get();
    }

    @Override
    public boolean addHost(Host host) {
        return addHost(host, true);
    }

    public boolean addHost(Host host, boolean refreshLoadBalancer) {

		//host.setPort(cpConfiguration.getPort());

		HostConnectionPool<CL> connPool = cpMap.get(host);

		if (connPool != null) {
			if (Logger.isDebugEnabled()) {
				Logger.debug("HostConnectionPool already exists for host: " + host + ", ignoring addHost");
			}
			return false;
		}


		final HostConnectionPool<CL> hostPool = hostConnPoolFactory.createHostConnectionPool(host, this);

		HostConnectionPool<CL> prevPool = cpMap.putIfAbsent(host, hostPool);
		if (prevPool == null) {
			// This is the first time we are adding this pool.
			Logger.info("Adding host connection pool for host: " + host);

			try {
				int primed = hostPool.primeConnections();
				Logger.info("Successfully primed " + primed + " of " + cpConfiguration.getMaxConnsPerHost() + " to "
					+ host);

				if (hostPool.isActive()) {
					if (refreshLoadBalancer) {
						selectionStrategy.addHost(host, hostPool);
					}

					cpHealthTracker.initializePingHealthchecksForPool(hostPool);

					cpMonitor.hostAdded(host, hostPool);

				} else {
					Logger.info(
						"Failed to prime enough connections to host " + host + " for it take traffic; will retry");
					cpMap.remove(host);
				}

				return primed > 0;
			} catch (DynoException e) {
				Logger.error("Failed to init host pool for host: " + host, e);
				cpMap.remove(host);
				return false;
			}
		} else {
			return false;
		}
    }

    @Override
    public boolean removeHost(Host host) {
		Logger.info(String.format("Removing host %s from selectionStrategy, cpHealthTracker, cpMonitor",
				host.getHostAddress()));
		// Since there are multiple data structures of host, token, connection pool etc, call removehost even
		// if it is not found in the cpMap
		selectionStrategy.removeHost(host);
		cpHealthTracker.removeHost(host);
		cpMonitor.hostRemoved(host);
		HostConnectionPool<CL> hostPool = cpMap.remove(host);
		if (hostPool != null) {
			hostPool.shutdown();
			Logger.info(String.format("Remove host: Successfully removed hostPool for host %s from connection pool",
				host.getHostAddress()));
			return true;
		} else {
			Logger.info(String.format("Remove host: Host pool for host %s NOT FOUND in the connection pool", host.getHostAddress()));
			return false;
		}
    }

    @Override
    public boolean isHostUp(Host host) {
		HostConnectionPool<CL> hostPool = cpMap.get(host);
		return (hostPool != null) ? hostPool.isActive() : false;
    }

    @Override
    public boolean hasHost(Host host) {
	return cpMap.get(host) != null;
    }

    @Override
    public List<HostConnectionPool<CL>> getActivePools() {
		return new ArrayList<HostConnectionPool<CL>>(
			CollectionUtils.filter(getPools(), new Predicate<HostConnectionPool<CL>>() {

				@Override
				public boolean apply(HostConnectionPool<CL> hostPool) {
				if (hostPool == null) {
					return false;
				}
				return hostPool.isActive();
				}
			}));
    }

    @Override
    public List<HostConnectionPool<CL>> getPools() {
    	return new ArrayList<HostConnectionPool<CL>>(cpMap.values());
    }

    @Override
    public Future<Boolean> updateHosts(Collection<Host> hostsUp, Collection<Host> hostsDown) {
		Logger.debug(String.format("Updating hosts: UP=%s, DOWN=%s", hostsUp, hostsDown));
		boolean condition = false;
		if (hostsUp != null && !hostsUp.isEmpty()) {
			for (Host hostUp : hostsUp) {
				condition |= addHost(hostUp);
			}
		}
		if (hostsDown != null && !hostsDown.isEmpty()) {
			for (Host hostDown : hostsDown) {
				condition |= removeHost(hostDown);
			}
		}
		return getEmptyFutureTask(condition);
    }

    @Override
    public HostConnectionPool<CL> getHostPool(Host host) {
    	return cpMap.get(host);
    }

    @Override
    public <R> OperationResult<R> executeWithFailover(Operation<CL, R> op) throws DynoException {

		// Start recording the operation
		long startTime = System.currentTimeMillis();

		RetryPolicy retry = cpConfiguration.getRetryPolicyFactory().getRetryPolicy();
		retry.begin();

		DynoException lastException = null;

		do {
			Connection<CL> connection = null;

			try {
				connection = selectionStrategy.getConnectionUsingRetryPolicy(op,
					cpConfiguration.getMaxTimeoutWhenExhausted(), TimeUnit.MILLISECONDS, retry);

				connection.getContext().setMetadata("host", connection.getHost().getHostAddress());
						connection.getContext().setMetadata("port", connection.getHost().getPort());

				OperationResult<R> result = connection.execute(op);

				// Add context to the result from the successful execution
				result.setNode(connection.getHost()).addMetadata(connection.getContext().getAll());

				retry.success();
				cpMonitor.incOperationSuccess(connection.getHost(), System.currentTimeMillis() - startTime);

				return result;

			} catch (NoAvailableHostsException e) {
				cpMonitor.incOperationFailure(null, e);

				throw e;
			} catch (PoolExhaustedException e) {
				Logger.warn("Pool exhausted: " + e.getMessage());
				cpMonitor.incOperationFailure(null, e);
				cpHealthTracker.trackConnectionError(e.getHostConnectionPool(), e);
			} catch (DynoException e) {

				retry.failure(e);
				lastException = e;

				if (connection != null) {
					cpMonitor.incOperationFailure(connection.getHost(), e);

					if (retry.allowRetry()) {
						cpMonitor.incFailover(connection.getHost(), e);
					}

					// Track the connection health so that the pool can be
					// purged at a later point
					cpHealthTracker.trackConnectionError(connection.getParentConnectionPool(), lastException);
				} else {
					cpMonitor.incOperationFailure(null, e);
				}
			} catch (Throwable t) {
				throw new RuntimeException(t);
			} finally {
				if (connection != null) {
					if (connection.getLastException() != null
						&& connection.getLastException() instanceof FatalConnectionException) {
						Logger.warn("Received FatalConnectionException; closing connection "
							+ connection.getContext().getAll() + " to host "
							+ connection.getParentConnectionPool().getHost());
						connection.getParentConnectionPool().recycleConnection(connection);
						// note - don't increment connection closed metric here;
						// it's done in closeConnection
					} else {
						connection.getContext().reset();
						connection.getParentConnectionPool().returnConnection(connection);
					}
				}
			}

		} while (retry.allowRetry());

		throw lastException;
    }

    @Override
	public <R> Collection<OperationResult<R>> executeWithRing(TokenRackMapper tokenRackMapper, Operation<CL, R> op) throws DynoException {

		// Start recording the operation
		long startTime = System.currentTimeMillis();

		Collection<Connection<CL>> connections = selectionStrategy
				.getConnectionsToRing(tokenRackMapper, cpConfiguration.getMaxTimeoutWhenExhausted(), TimeUnit.MILLISECONDS);

		LinkedBlockingQueue<Connection<CL>> connQueue = new LinkedBlockingQueue<Connection<CL>>();
		connQueue.addAll(connections);

		List<OperationResult<R>> results = new ArrayList<OperationResult<R>>();

		DynoException lastException = null;

		try {
			while (!connQueue.isEmpty()) {

				Connection<CL> connection = connQueue.poll();

				RetryPolicy retry = cpConfiguration.getRetryPolicyFactory().getRetryPolicy();
				retry.begin();

				do {
					try {
						connection.getContext().setMetadata("host", connection.getHost().getHostAddress());
						OperationResult<R> result = connection.execute(op);

						// Add context to the result from the successful
						// execution
						result.setNode(connection.getHost()).addMetadata(connection.getContext().getAll());

						retry.success();
						cpMonitor.incOperationSuccess(connection.getHost(), System.currentTimeMillis() - startTime);

						results.add(result);

					} catch (NoAvailableHostsException e) {
						cpMonitor.incOperationFailure(null, e);

						throw e;
					} catch (DynoException e) {

						retry.failure(e);
						lastException = e;

						cpMonitor.incOperationFailure(connection != null ? connection.getHost() : null, e);

						// Track the connection health so that the pool can be
						// purged at a later point
						if (connection != null) {
							cpHealthTracker.trackConnectionError(connection.getParentConnectionPool(), lastException);
						}

					} catch (Throwable t) {
						throw new RuntimeException(t);
					} finally {
						connection.getContext().reset();
						connection.getParentConnectionPool().returnConnection(connection);
					}

				} while (retry.allowRetry());
			}

			// we fail the entire operation on a partial failure. hence need to
			// clean up the rest of the pending connections
		} finally {
			List<Connection<CL>> remainingConns = new ArrayList<Connection<CL>>();
			connQueue.drainTo(remainingConns);
			for (Connection<CL> connectionToClose : remainingConns) {
				try {
					connectionToClose.getContext().reset();
					connectionToClose.getParentConnectionPool().returnConnection(connectionToClose);
				} catch (Throwable t) {

				}
			}
		}

		if (lastException != null) {
			throw lastException;
		} else {
			return results;
		}
	}

    /**
     * Use with EXTREME CAUTION. Connection that is borrowed must be returned,
     * else we will have connection pool exhaustion
     * 
     * @param baseOperation
     * @return
     */
    public <R> Connection<CL> getConnectionForOperation(BaseOperation<CL, R> baseOperation) {
	return selectionStrategy.getConnection(baseOperation, cpConfiguration.getMaxTimeoutWhenExhausted(),
		TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {

	if (started.get()) {
	    for (Host host : cpMap.keySet()) {
		removeHost(host);
	    }
	    cpHealthTracker.stop();
	    hostsUpdater.stop();
	    connPoolThreadPool.shutdownNow();
	    deregisterMonitorConsoleMBean();
	}
    }

    @Override
    public Future<Boolean> start() throws DynoException {

		if (started.get()) {
			return getEmptyFutureTask(false);
		}

		HostSupplier hostSupplier = cpConfiguration.getHostSupplier();
		if (hostSupplier == null) {
			throw new DynoException("Host supplier not configured!");
		}

		HostStatusTracker hostStatus = hostsUpdater.refreshHosts();
		cpMonitor.setHostCount(hostStatus.getHostCount());

		Collection<Host> hostsUp = hostStatus.getActiveHosts();
		if (hostsUp == null || hostsUp.isEmpty()) {
			throw new NoAvailableHostsException("No available hosts when starting connection pool");
		}

		final ExecutorService threadPool = Executors.newFixedThreadPool(Math.max(10, hostsUp.size()));
		final List<Future<Void>> futures = new ArrayList<Future<Void>>();

		for (final Host host : hostsUp) {

			// Add host connection pool, but don't init the load balancer yet
			futures.add(threadPool.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				addHost(host, false);
				return null;
			}
			}));
		}

		try {
			for (Future<Void> future : futures) {
				try {
					future.get();
				} catch (InterruptedException e) {
					// do nothing
				} catch (ExecutionException e) {
					throw new RuntimeException(e);
				}
			}
		} finally {
			threadPool.shutdownNow();
		}

		boolean success = started.compareAndSet(false, true);
		if (success) {
			idling.set(false);
			idleThreadPool.shutdownNow();
			selectionStrategy = initSelectionStrategy();
			cpHealthTracker.start();

			connPoolThreadPool.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
				try {
					HostStatusTracker hostStatus = hostsUpdater.refreshHosts();
					cpMonitor.setHostCount(hostStatus.getHostCount());
					Logger.debug(hostStatus.toString());
					updateHosts(hostStatus.getActiveHosts(), hostStatus.getInactiveHosts());
				} catch (Throwable throwable) {
					Logger.error("Failed to update hosts cache", throwable);
				}
			}

				}, 15 * 1000, 30 * 1000, TimeUnit.MILLISECONDS);

			MonitorConsole.getInstance().registerConnectionPool(this);
			registerMonitorConsoleMBean(MonitorConsole.getInstance());
		}
		return getEmptyFutureTask(true);
    }

    @Override
    public void idle() {
		if (this.started.get()) {
			throw new IllegalStateException("Cannot move from started to idle once the pool has been started");
		}

		if (idling.compareAndSet(false, true)) {
			idleThreadPool.scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					if (!started.get()) {
						try {
							HostStatusTracker hostStatus = hostsUpdater.refreshHosts();
							cpMonitor.setHostCount(hostStatus.getHostCount());
							Collection<Host> hostsUp = hostStatus.getActiveHosts();
							if (hostsUp.size() > 0) {
								Logger.debug("Found hosts while IDLING; starting the connection pool");
								start().get();
							}
						} catch (NoAvailableHostsException nah) {
							Logger.debug("No hosts found, will continue IDLING");
						} catch (DynoException de) {
							Logger.warn("Attempt to start connection pool FAILED", de);
						} catch (Exception e) {
							Logger.warn("Attempt to start connection pool FAILED", e);
						}
					}
				}
			}, 30, 60, TimeUnit.SECONDS);
		}
    }

    @Override
    public ConnectionPoolConfiguration getConfiguration() {
	return cpConfiguration;
    }

    private void registerMonitorConsoleMBean(MonitorConsoleMBean bean) {
	final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
	try {
	    ObjectName objectName = new ObjectName(MonitorConsole.OBJECT_NAME);
	    if (!server.isRegistered(objectName)) {
		server.registerMBean(bean, objectName);
		Logger.info("registered mbean " + objectName);
	    } else {
		Logger.info("mbean " + objectName + " has already been registered !");
	    }
	} catch (MalformedObjectNameException | InstanceAlreadyExistsException | MBeanRegistrationException
		| NotCompliantMBeanException ex) {
	    Logger.error("Unable to register MonitorConsole mbean ", ex);
	}
    }

    private void deregisterMonitorConsoleMBean() {
		final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
		try {
			ObjectName objectName = new ObjectName(MonitorConsole.OBJECT_NAME);
			if (server.isRegistered(objectName)) {
				server.unregisterMBean(objectName);
				Logger.info("deregistered mbean " + objectName);
			}
		} catch (MalformedObjectNameException | MBeanRegistrationException | InstanceNotFoundException ex) {
			Logger.error("Unable to deregister MonitorConsole mbean ", ex);
		}

    }

    private HostSelectionWithFallback<CL> initSelectionStrategy() {

		if (cpConfiguration.getTokenSupplier() == null) {
			throw new RuntimeException("TokenMapSupplier not configured");
		}
		HostSelectionWithFallback<CL> selection = new HostSelectionWithFallback<CL>(cpConfiguration, cpMonitor);
		selection.initWithHosts(cpMap);
		return selection;
    }

    private Future<Boolean> getEmptyFutureTask(final Boolean condition) {

	FutureTask<Boolean> future = new FutureTask<Boolean>(new Callable<Boolean>() {
	    @Override
	    public Boolean call() throws Exception {
		return condition;
	    }
	});

	future.run();
	return future;
    }

    private class SyncHostConnectionPoolFactory implements HostConnectionPoolFactory<CL> {

	@Override
	public HostConnectionPool<CL> createHostConnectionPool(Host host, ConnectionPoolImpl<CL> parentPoolImpl) {
	    return new HostConnectionPoolImpl<CL>(host, connFactory, cpConfiguration, cpMonitor);
	}
    }

    private class AsyncHostConnectionPoolFactory implements HostConnectionPoolFactory<CL> {

	@Override
	public HostConnectionPool<CL> createHostConnectionPool(Host host, ConnectionPoolImpl<CL> parentPoolImpl) {
	    return new SimpleAsyncConnectionPoolImpl<CL>(host, connFactory, cpConfiguration, cpMonitor);
	}
    }

    @Override
    public <R> ListenableFuture<OperationResult<R>> executeAsync(AsyncOperation<CL, R> op) throws DynoException {

	DynoException lastException = null;
	Connection<CL> connection = null;
	long startTime = System.currentTimeMillis();

	try {
	    connection = selectionStrategy.getConnection(op, cpConfiguration.getMaxTimeoutWhenExhausted(),
		    TimeUnit.MILLISECONDS);

	    ListenableFuture<OperationResult<R>> futureResult = connection.executeAsync(op);

	    cpMonitor.incOperationSuccess(connection.getHost(), System.currentTimeMillis() - startTime);

	    return futureResult;

	} catch (NoAvailableHostsException e) {
	    cpMonitor.incOperationFailure(null, e);
	    throw e;
	} catch (DynoException e) {

	    lastException = e;
	    cpMonitor.incOperationFailure(connection != null ? connection.getHost() : null, e);

	    // Track the connection health so that the pool can be purged at a
	    // later point
	    if (connection != null) {
		cpHealthTracker.trackConnectionError(connection.getParentConnectionPool(), lastException);
	    }

	} catch (Throwable t) {
	    t.printStackTrace();
	} finally {
	    if (connection != null) {
		connection.getParentConnectionPool().returnConnection(connection);
	    }
	}
	return null;
    }

    public TokenPoolTopology getTopology() {
		return selectionStrategy.getTokenPoolTopology();
    }

    @Override
    public Map<String, List<TokenPoolTopology.TokenStatus>> getTopologySnapshot() {
		return Collections.unmodifiableMap(selectionStrategy.getTokenPoolTopology().getAllTokens());
    }

    @Override
    public Long getTokenForKey(String key) {
	if (cpConfiguration
		.getLoadBalancingStrategy() == ConnectionPoolConfiguration.LoadBalancingStrategy.TokenAware) {
	    return selectionStrategy.getTokenForKey(key);
	}

	return null;
    }
}
