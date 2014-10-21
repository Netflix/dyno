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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.AsyncOperation;
import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.ConnectionFactory;
import com.netflix.dyno.connectionpool.ConnectionObservor;
import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration.LoadBalancingStrategy;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.HostConnectionStats;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.ListenableFuture;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.RetryPolicy;
import com.netflix.dyno.connectionpool.RetryPolicy.RetryPolicyFactory;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.TokenPoolTopology;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.FatalConnectionException;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.exception.PoolTimeoutException;
import com.netflix.dyno.connectionpool.exception.ThrottledException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl.ErrorRateMonitorConfigImpl;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl.HostConnectionPoolFactory.Type;
import com.netflix.dyno.connectionpool.impl.health.ConnectionPoolHealthTracker;
import com.netflix.dyno.connectionpool.impl.lb.HostSelectionWithFallback;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Predicate;

/**
 * Main implementation class for {@link ConnectionPool}
 * The pool deals with a bunch of other components and brings together all the functionality for Dyno. Hence this is where all the action happens. 
 * 
 * Here are the top salient features of this class. 
 * 
 *     1. Manages a collection of {@link HostConnectionPool}s for all the {@link Host}s that it receives from the {@link HostSupplier}
 * 
 *     2. Manages adding and removing hosts as dictated by the HostSupplier.
 *  
 *     3. Enables execution of {@link Operation} using a {@link Connection} borrowed from the {@link HostConnectionPool}s
 * 
 *     4. Employs a {@link HostSelectionStrategy} (basically Round Robin or Token Aware) when executing operations
 * 
 *     5. Uses a health check monitor for tracking error rates from the execution of operations. The health check monitor can then decide 
 *        to recycle a given HostConnectionPool, and execute requests using fallback HostConnectionPools for remote DCs.
 *        
 *     6. Uses {@link RetryPolicy} when executing operations for better resilience against transient failures.    
 * 
 * @see {@link HostSupplier} {@link Host} {@link HostSelectionStrategy}
 * @see {@link Connection} {@link ConnectionFactory} {@link ConnectionPoolConfiguration} {@link ConnectionPoolMonitor}
 * @see {@link ConnectionPoolHealthTracker} 
 * 
 * @author poberai
 *
 * @param <CL>
 */
public class ConnectionPoolImpl<CL> implements ConnectionPool<CL> {

	private static final Logger Logger = LoggerFactory.getLogger(ConnectionPoolImpl.class);
	
	private final ConcurrentHashMap<Host, HostConnectionPool<CL>> cpMap = new ConcurrentHashMap<Host, HostConnectionPool<CL>>();
	private final ConnectionPoolHealthTracker<CL> cpHealthTracker;
	
	private final HostConnectionPoolFactory<CL> hostConnPoolFactory;
	private final ConnectionFactory<CL> connFactory; 
	private final ConnectionPoolConfiguration cpConfiguration; 
	private final ConnectionPoolMonitor cpMonitor; 
	
	private final HostsUpdator hostsUpdator; 
	private final ScheduledExecutorService connPoolThreadPool = Executors.newScheduledThreadPool(1);
	
	private final AtomicBoolean started = new AtomicBoolean(false);
	
	private HostSelectionWithFallback<CL> selectionStrategy; 
	
	private Type poolType;

	public ConnectionPoolImpl(ConnectionFactory<CL> cFactory, ConnectionPoolConfiguration cpConfig, ConnectionPoolMonitor cpMon) {
		this(cFactory, cpConfig, cpMon, Type.Sync);
	}
	
	public ConnectionPoolImpl(ConnectionFactory<CL> cFactory, ConnectionPoolConfiguration cpConfig, ConnectionPoolMonitor cpMon, Type type) {
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
		};
	
		this.hostsUpdator = new HostsUpdator(cpConfiguration.getHostSupplier());
	}
	
	public HostSelectionWithFallback<CL> getTokenSelection() {
		return selectionStrategy;
	}

	public String getName() {
		return cpConfiguration.getName();
	}
	
	public ConnectionPoolMonitor getMonitor() {
		return cpMonitor;
	}
	
	@Override
	public boolean addHost(Host host) {
		return addHost(host, true);
	}

	public boolean addHost(Host host, boolean refreshLoadBalancer) {
		
		host.setPort(cpConfiguration.getPort());

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
				hostPool.primeConnections();
				if (refreshLoadBalancer) {
					selectionStrategy.addHost(host, hostPool);
				}
				
				// Initiate ping based monitoring only for async pools.
				// Note that sync pools get monitored based on feedback from operation executions on the pool itself
				if (poolType == Type.Async) {
					cpHealthTracker.initialPingHealthchecksForPool(hostPool);
				}
				
				cpMonitor.hostAdded(host, hostPool);
				
				return true;
			} catch (DynoException e) {
				Logger.info("Failed to init host pool for host: " + host, e);
				cpMap.remove(host);
				return false;
			}
		} else {
			return false;
		}
	}
	
	@Override
	public boolean removeHost(Host host) {
		 
		HostConnectionPool<CL> hostPool = cpMap.remove(host);
		if (hostPool != null) {
			selectionStrategy.removeHost(host, hostPool);
			cpHealthTracker.removeHost(host);
			cpMonitor.hostRemoved(host);
			hostPool.shutdown();
			return true;
		} else {
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
		
		return new ArrayList<HostConnectionPool<CL>>(CollectionUtils.filter(getPools(), new Predicate<HostConnectionPool<CL>>() {

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
		
		do  {
			Connection<CL> connection = null;
			
			try { 
					connection = 
							selectionStrategy.getConnection(op, cpConfiguration.getMaxTimeoutWhenExhausted(), TimeUnit.MILLISECONDS);

				OperationResult<R> result = connection.execute(op);
				
				// Add context to the result from the successful execution
				result.setNode(connection.getHost())
					  .addMetadata(connection.getContext().getAll());
				
				retry.success();
				cpMonitor.incOperationSuccess(connection.getHost(), System.currentTimeMillis()-startTime);
				
				return result; 
				
			} catch(NoAvailableHostsException e) {
				cpMonitor.incOperationFailure(null, e);

				throw e;
			} catch(DynoException e) {
				
				retry.failure(e);
				lastException = e;
				
				cpMonitor.incOperationFailure(connection != null ? connection.getHost() : null, e);
				if (retry.allowRetry()) {
					cpMonitor.incFailover(connection.getHost(), e);
				}
				
				// Track the connection health so that the pool can be purged at a later point
				if (connection != null) {
					cpHealthTracker.trackConnectionError(connection.getParentConnectionPool(), lastException);
				}
				
			} catch(Throwable t) {
				throw new RuntimeException(t);
			} finally {
				if (connection != null) {
					connection.getContext().reset();
					connection.getParentConnectionPool().returnConnection(connection);
				}
			}
			
		} while(retry.allowRetry());
		
		throw lastException;
	}

	@Override
	public <R> Collection<OperationResult<R>> executeWithRing(Operation<CL, R> op) throws DynoException {

		// Start recording the operation
		long startTime = System.currentTimeMillis();

		Collection<Connection<CL>> connections = selectionStrategy.getConnectionsToRing(cpConfiguration.getMaxTimeoutWhenExhausted(), TimeUnit.MILLISECONDS);

		LinkedBlockingQueue<Connection<CL>> connQueue = new LinkedBlockingQueue<Connection<CL>>();
		connQueue.addAll(connections);

		List<OperationResult<R>> results = new ArrayList<OperationResult<R>>();

		DynoException lastException = null;

		try { 
			while(!connQueue.isEmpty()) {

				Connection<CL> connection = connQueue.poll();

				RetryPolicy retry = cpConfiguration.getRetryPolicyFactory().getRetryPolicy();
				retry.begin();

				do {
					try { 
						OperationResult<R> result = connection.execute(op);

						// Add context to the result from the successful execution
						result.setNode(connection.getHost())
						.addMetadata(connection.getContext().getAll());

						retry.success();
						cpMonitor.incOperationSuccess(connection.getHost(), System.currentTimeMillis()-startTime);

						results.add(result); 

					} catch(NoAvailableHostsException e) {
						cpMonitor.incOperationFailure(null, e);

						throw e;
					} catch(DynoException e) {

						retry.failure(e);
						lastException = e;

						cpMonitor.incOperationFailure(connection != null ? connection.getHost() : null, e);

						// Track the connection health so that the pool can be purged at a later point
						if (connection != null) {
							cpHealthTracker.trackConnectionError(connection.getParentConnectionPool(), lastException);
						}

					} catch(Throwable t) {
						throw new RuntimeException(t);
					} finally {
						connection.getContext().reset();
						connection.getParentConnectionPool().returnConnection(connection);
					}

				} while(retry.allowRetry());
			}
			
			// we fail the entire operation on a partial failure. hence need to clean up the rest of the pending connections
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
	 * Use with EXTREME CAUTION. Connection that is borrowed must be returned, else we will have connection pool exhaustion
	 * @param baseOperation
	 * @return
	 */
	public <R> Connection<CL> getConnectionForOperation(BaseOperation<CL, R> baseOperation) {
		return selectionStrategy.getConnection(baseOperation, cpConfiguration.getConnectTimeout(), TimeUnit.MILLISECONDS);
	}
	
	@Override
	public void shutdown() {
		for (Host host : cpMap.keySet()) {
			removeHost(host);
		}
		cpHealthTracker.stop();
		hostsUpdator.stop();
		connPoolThreadPool.shutdownNow();
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

		HostStatusTracker hostStatus = hostsUpdator.refreshHosts();
		Collection<Host> hostsUp = hostStatus.getActiveHosts();
		
		if (hostsUp == null || hostsUp.isEmpty()) {
			throw new NoAvailableHostsException("No available hosts when starting connection pool");
		}

		for (Host host : hostsUp) {
			// Add host connection pool, but don't init the load balancer yet
			addHost(host, false);  
		}

		boolean success = started.compareAndSet(false, true);
		if (success) {
			selectionStrategy = initSelectionStrategy();
			cpHealthTracker.start();
			
			connPoolThreadPool.scheduleWithFixedDelay(new Runnable() {

				@Override
				public void run() {
					
					HostStatusTracker hostStatus = hostsUpdator.refreshHosts();
					updateHosts(hostStatus.getActiveHosts(), hostStatus.getInactiveHosts());
				}
				
			}, 15*1000, 30*1000, TimeUnit.MILLISECONDS);
			
			MonitorConsole.getInstance().registerConnectionPool(this);
		}
		
		return getEmptyFutureTask(true);
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

	public interface HostConnectionPoolFactory<CL> {
		
		HostConnectionPool<CL> createHostConnectionPool(Host host, ConnectionPoolImpl<CL> parentPoolImpl);
		
		public enum Type {
			Sync, Async;
		}
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
			connection = 
					selectionStrategy.getConnection(op, cpConfiguration.getMaxTimeoutWhenExhausted(), TimeUnit.MILLISECONDS);
			
			ListenableFuture<OperationResult<R>> futureResult = connection.executeAsync(op);
			
			cpMonitor.incOperationSuccess(connection.getHost(), System.currentTimeMillis()-startTime);
		
			return futureResult; 
			
		} catch(NoAvailableHostsException e) {
			cpMonitor.incOperationFailure(null, e);
			throw e;
		} catch(DynoException e) {
			
			lastException = e;
			cpMonitor.incOperationFailure(connection != null ? connection.getHost() : null, e);
			
			// Track the connection health so that the pool can be purged at a later point
			if (connection != null) {
				cpHealthTracker.trackConnectionError(connection.getParentConnectionPool(), lastException);
			}
			
		} catch(Throwable t) {
			t.printStackTrace();
		} finally {
			if (connection != null) {
				connection.getParentConnectionPool().returnConnection(connection);
			}
		}
		return null;
	}

	public TokenPoolTopology  getTopology() {
		return selectionStrategy.getTokenPoolTopology();
	}
	
	public static class UnitTest {
		
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
			public Connection<TestClient> createConnection(HostConnectionPool<TestClient> pool, ConnectionObservor observor) throws DynoConnectException, ThrottledException {
				return new TestConnection(pool);
			}
		};
		
		private Host host1 = new Host("host1", 8080, Status.Up).setRack("localDC");
		private Host host2 = new Host("host2", 8080, Status.Up).setRack("localDC");
		private Host host3 = new Host("host3", 8080, Status.Up).setRack("localDC");
		
		private final List<Host> hostSupplierHosts = new ArrayList<Host>();
		
		@Before
		public void beforeTest() {

			hostSupplierHosts.clear();
			
			host1 = new Host("host1", 8080, Status.Up).setRack("localDC");
			host2 = new Host("host2", 8080, Status.Up).setRack("localDC");
			host3 = new Host("host3", 8080, Status.Up).setRack("localDC");

			client = new TestClient();
			cpConfig = new ConnectionPoolConfigurationImpl("TestClient").setLoadBalancingStrategy(LoadBalancingStrategy.RoundRobin);
			cpConfig.withHostSupplier(new HostSupplier() {
				
				@Override
				public Collection<Host> getHosts() {
					return hostSupplierHosts;
				}
			});
			
			cpConfig.setLocalDC("localDC");
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
			
				checkHostStats(host1);
				checkHostStats(host2);
			} finally {
				pool.shutdown();
			}
		}
		
		private void checkConnectionPoolMonitorStats(int numHosts)  {
			
			System.out.println("Total ops: " + client.ops.get());
			Assert.assertTrue("Total ops: " + client.ops.get(), client.ops.get() > 0);

			Assert.assertEquals(client.ops.get(), cpMonitor.getOperationSuccessCount());
			Assert.assertEquals(0, cpMonitor.getOperationFailureCount());
			Assert.assertEquals(0, cpMonitor.getOperationTimeoutCount());
			
			Assert.assertEquals(numHosts*cpConfig.getMaxConnsPerHost(), cpMonitor.getConnectionCreatedCount());
			Assert.assertEquals(0, cpMonitor.getConnectionCreateFailedCount());
			Assert.assertEquals(numHosts*cpConfig.getMaxConnsPerHost(), cpMonitor.getConnectionClosedCount());
			
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

			return new TokenMapSupplier () {
				@Override
				public List<HostToken> getTokens() {
					return new ArrayList<HostToken>(tokenMap.values());
				}

				@Override
				public HostToken getTokenForHost(Host host) {
					Host h = host;
					if (h.getHostName().equals("host1")) {
						tokenMap.put(host1, new HostToken(309687905L, host1));
					} else if (h.getHostName().equals("host2")) {
						tokenMap.put(host2, new HostToken(1383429731L, host2));
					} else if (h.getHostName().equals("host3")) {
						tokenMap.put(host3, new HostToken(2457171554L, host3));
					}
					return tokenMap.get(host);
				}

				@Override
				public void initWithHosts(Collection<Host> hosts) {
					
					tokenMap.clear();
					for (Host h : hosts) {
						if (h.getHostName().equals("host1")) {
							tokenMap.put(host1, new HostToken(309687905L, host1));
						} else if (h.getHostName().equals("host2")) {
							tokenMap.put(host2, new HostToken(1383429731L, host2));
						} else if (h.getHostName().equals("host3")) {
							tokenMap.put(host3, new HostToken(2457171554L, host3));
						}
					}
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

		@Test (expected=NoAvailableHostsException.class)
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
		public void testPoolExhausted() throws Exception {

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
			for (int i=0; i<9; i++) {
				threadPool.submit(new Callable<Void>() {

					@Override
					public Void call() throws Exception {
						latch.countDown();
						executeTestClientOperation(pool, blockConnectionForSomeTime);
						return null;
					}
				});
			}
			
			latch.await(); Thread.sleep(100); // wait patiently for all threads to have blocked the connections
			
			try {
				executeTestClientOperation(pool);
				Assert.fail("TEST FAILED");
			} catch (PoolTimeoutException e) {
				threadPool.shutdownNow();
				pool.shutdown();
			}
		}
		
		
		@Test
		public void testHostEvictionDueToErrorRates() throws Exception {
			
			// First configure the error rate monitor
			ErrorRateMonitorConfigImpl errConfig =  new ErrorRateMonitorConfigImpl();  
			errConfig.checkFrequency = 1;
			errConfig.window = 1;
			errConfig.suppressWindow = 60;
			
			errConfig.addThreshold(10, 1, 100);
					
			final AtomicReference<String> badHost = new AtomicReference<String>();
			
			final ConnectionFactory<TestClient> badConnectionFactory = new ConnectionFactory<TestClient>() {

				@Override
				public Connection<TestClient> createConnection(final HostConnectionPool<TestClient> pool, ConnectionObservor cObservor) throws DynoConnectException, ThrottledException {
					
					return new TestConnection(pool) {

						@Override
						public <R> OperationResult<R> execute(Operation<com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl.UnitTest.TestClient, R> op) throws DynoException {
							if (pool.getHost().getHostName().equals(badHost.get())) {
								throw new FatalConnectionException("Fail for bad host");
							}
							return super.execute(op);
						}
					};
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
			
			Assert.assertEquals(3*cpConfig.getMaxConnsPerHost(), cpMonitor.getConnectionCreatedCount());
			Assert.assertEquals(0, cpMonitor.getConnectionCreateFailedCount());
			Assert.assertEquals(3*cpConfig.getMaxConnsPerHost(), cpMonitor.getConnectionClosedCount());
			
			Assert.assertEquals(client.ops.get() + cpMonitor.getOperationFailureCount(), cpMonitor.getConnectionBorrowedCount());
			Assert.assertEquals(client.ops.get() + cpMonitor.getOperationFailureCount(), cpMonitor.getConnectionReturnedCount());
			
			checkHostStats(host1); 
			checkHostStats(host3);
			
			HostConnectionStats h2Stats = cpMonitor.getHostStats().get(host2);
			Assert.assertEquals(cpMonitor.getOperationFailureCount(), h2Stats.getOperationErrorCount());

		}
		
		@Test
		public void testWithRetries() throws Exception {
			
			final ConnectionFactory<TestClient> badConnectionFactory = new ConnectionFactory<TestClient>() {
				@Override
				public Connection<TestClient> createConnection(final HostConnectionPool<TestClient> pool, ConnectionObservor cObservor) throws DynoConnectException, ThrottledException {
					return new TestConnection(pool) {
						@Override
						public <R> OperationResult<R> execute(Operation<com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl.UnitTest.TestClient, R> op) throws DynoException {
							throw new DynoException("Fail for bad host");
						}
					};
				}
			};
			
			final RetryNTimes retry = new RetryNTimes(3, false);
			final RetryPolicyFactory rFactory = new RetryNTimes.RetryPolicyFactory() {
				@Override
				public RetryPolicy getRetryPolicy() {
					return retry;
				}
			};
			
			final ConnectionPoolImpl<TestClient> pool = new ConnectionPoolImpl<TestClient>(badConnectionFactory, cpConfig.setRetryPolicyFactory(rFactory), cpMonitor);
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
				public String getKey() {
					return "TestOperation";
				}
			});
		}


		private void runTest(final ConnectionPoolImpl<TestClient> pool, final Callable<Void> customTestLogic) throws Exception {
			
			int nThreads = 1;
			final ExecutorService threadPool = Executors.newFixedThreadPool(nThreads);
			final AtomicBoolean stop = new AtomicBoolean(false);

			final CountDownLatch latch = new CountDownLatch(nThreads);
			
			for (int i=0; i<nThreads; i++) {
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
										public String getKey() {
											return "TestOperation";
										}
									});
								} catch (DynoException e) {
//									System.out.println("FAILED Test Worker operation: " + e.getMessage());
//									e.printStackTrace();
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

}
