package com.netflix.dyno.memcache;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.MemcachedClient;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.dyno.connectionpool.AsyncOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.ConnectionFactory;
import com.netflix.dyno.connectionpool.ConnectionObservor;
import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.HostConnectionStats;
import com.netflix.dyno.connectionpool.HostGroup;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.OperationMonitor;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.RetryPolicy;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.exception.ThrottledException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl;
import com.netflix.dyno.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.impl.HostStatusTracker;
import com.netflix.dyno.connectionpool.impl.LastOperationMonitor;
import com.netflix.dyno.connectionpool.impl.MonitorConsole;

/**
 * Impl of {@link ConnectionPool} 
 * 
 * Note that this class manages an active {@link MemcachedConnectionPool} underneath. The {@link MemcachedConnectionPool} maintains
 * a single async connection for all active hosts. Hence we only need one  {@link MemcachedConnectionPool} for all hosts.
 * 
 * When the active host set changes via {@link #addHost(Host)} {@link #removeHost(Host)} or {@link #updateHosts(Collection, Collection)}
 * we just swap the {@link MemcachedConnectionPool} with a new one and let the older one terminate. 
 * This is done via the {@link #reconnect(Collection, Collection)} method. 
 * 
 * @author poberai
 *
 * @param <CL>
 */
public class RollingMemcachedConnectionPoolImpl<CL> implements ConnectionPool<CL> {
	
	private static final Logger Logger = LoggerFactory.getLogger(ConnectionPoolImpl.class);
	
	// Critical components needed for the successful functioning of this pool
	
	// The connection factory that will create connections for this pool
	private final ConnectionFactory<CL> connFactory; 
	// The connection pool configuration for stuff like maxConnsPerHost etc
	private final ConnectionPoolConfiguration cpConfiguration; 
	// stats for tracking connection oriented operations
	private final ConnectionPoolMonitor connPoolMonitor; 
	private final OperationMonitor operationMonitor;
	
	// Executor pool for async operations like shutting down older connection pools while letting inflight requests finish
	private final ExecutorService threadPool = Executors.newFixedThreadPool(1); 
	
	// Status tracking the state of this connection pool
	private final AtomicBoolean active = new AtomicBoolean(false);
	private final AtomicBoolean reconnecting = new AtomicBoolean(false);
	
	// Inner state of the pool. Note that this state flips when there is a bad connection to a host of a change in the hosts supplied
	private final AtomicReference<InnerState> innerState = new AtomicReference<InnerState>(new InnerState());
	// Simple counter for tracking the no of state changes.
	private final AtomicInteger stateChangeCount = new AtomicInteger(0);
	
	private final MemcachedConnectionObserver connObservor = new MemcachedConnectionObserver(this);
	
	/**
	 * Constructor
	 * 
	 * @param cFactory
	 * @param cpConfig
	 * @param cpMon
	 */
	public RollingMemcachedConnectionPoolImpl(String namespace,
											  ConnectionFactory<CL> cFactory, ConnectionPoolConfiguration cpConfig, 
											  ConnectionPoolMonitor cpMonitor, OperationMonitor opMonitor) {
		this.connFactory = cFactory;
		this.cpConfiguration = cpConfig;
		this.connPoolMonitor = cpMonitor;
		this.operationMonitor = opMonitor;
		
		MonitorConsole.getInstance().addMonitorConsole(namespace, cpMonitor);
	}
	
	/**
	 * Simple method that determines if there is an actual change in the set of active hosts and calls 
	 * {@link RollingMemcachedConnectionPoolImpl#reconnect(Collection, Collection)} 
	 * 
	 * @return true/false indicating whether there was an actual change in connections for this pool. 
	 */
	@Override
	public boolean addHost(Host host) {
		
		List<Host> activeHosts = new ArrayList<Host>(innerState.get().hostTracker.getActiveHosts());
		boolean modified = activeHosts.add(host);
		if (modified) {
			
			List<Host> inactiveHosts = new ArrayList<Host>(innerState.get().hostTracker.getInactiveHosts());
			inactiveHosts.remove(host);
			
			try {
				reconnect(activeHosts, inactiveHosts);
				return true;
			} catch (Exception e) {
				throw new DynoConnectException(e);
			}
		} else {
			return false;
		}
	}

	/**
	 * Simple method that determines if there is an actual change in the set of inactive hosts and calls 
	 * {@link RollingMemcachedConnectionPoolImpl#reconnect(Collection, Collection)} 
	 * 
	 * @return true/false indicating whether there was an actual change in connections for this pool. 
	 */
	@Override
	public boolean removeHost(Host host) {

		List<Host> activeHosts = new ArrayList<Host>(innerState.get().hostTracker.getActiveHosts());
		
		boolean modified = activeHosts.remove(host);
		if (modified) {
			
			List<Host> inactiveHosts = new ArrayList<Host>(innerState.get().hostTracker.getInactiveHosts());
			inactiveHosts.add(host);
			
			try {
				reconnect(activeHosts, inactiveHosts);
				return true;
			} catch (Exception e) {
				throw new DynoConnectException(e);
			}
		} else {
			return false;
		}
	}

	/**
	 * @return true/false indicating whether the host is active/inactive
	 */
	@Override
	public boolean isHostUp(Host host) {
		return innerState.get().hostTracker.isHostUp(host);
	}

	/**
	 * @return true/false indicating whether this host is being tracked by this conn pool.
	 */
	@Override
	public boolean hasHost(Host host) {
		return isHostUp(host); // since we only track hosts that are up
	}

	/**
	 * @return all active {@link HostConnectionPool}s for this meta conn pool
	 */
	@Override
	public List<HostConnectionPool<CL>> getActivePools() {
		
		List<HostConnectionPool<CL>> list = new ArrayList<HostConnectionPool<CL>>();
		MemcachedConnectionPool<CL> pool = innerState.get().hostConnectionPool;
		if (pool != null) {
			list.add(pool);
		}
		return list;
	}

	/**
	 * Same as {@link #getActivePools()} due to the way the Memcached {@link HostConnectionPool} function.
	 */
	@Override
	public List<HostConnectionPool<CL>> getPools() {
		return getActivePools();  // remember that we are only tracking active pools here. Inactive pools are scheduled for shutdown
	}

	/**
	 * Similar to {@link #addHost(Host)} and {@link #removeHost(Host)}, ultimately calls {@link #reconnect(Collection, Collection)}
	 */
	@Override
    public Future<Boolean> updateHosts(Collection<Host> hostsUp, Collection<Host> hostsDown) {
		
		HostStatusTracker currentHostStatus = innerState.get().hostTracker;
		if (currentHostStatus.checkIfChanged(hostsUp, hostsDown)) {
			
			// Host status changed! Recycle existing pool.
			Logger.info("Host set has changed. Will recycle memcache connection pool");
			
			return reconnect(hostsUp, hostsDown);
		}
		return getEmptyFutureTask(false);
	}

	/**
	 * We don't track connection pool for individual hosts. The memcache connection pool is a multiplexed connection pool. 
	 * Use getActivePools() instead
	 */
	@Deprecated
	@Override
	public HostConnectionPool<CL> getHostPool(Host host) {
		return null; 
	}

	/**
	 * This method executes the supplied {@link Operation} using the {@link MemcachedConnectionPool} undernath. 
	 * It also maintains {@link ConnectionPoolMonitor} metrics and does retries. 
	 * 
	 * @return {@link OperationResult} 
	 */
	@Override
	public <R> OperationResult<R> executeWithFailover(Operation<CL, R> op) throws DynoException {
		
		// Start recording the operation
		long startTime = System.currentTimeMillis();
		
		RetryPolicy retry = cpConfiguration.getRetryPolicyFactory().getRetryPolicy();
		retry.begin();
		
		DynoException lastException = null;
		
		do  {
			Connection<CL> connection = null;
			OperationResult<R> result = null;
			
			try { 
				
				HostConnectionPool<CL> pool = innerState.get().hostConnectionPool; 
				if (pool == null) {
					throw new NoAvailableHostsException("No host connection pool setup");
				}
				
				connection = pool.borrowConnection(cpConfiguration.getMaxTimeoutWhenExhausted(), TimeUnit.MILLISECONDS); 
				if (connection == null) {
					throw new NoAvailableHostsException("No hosts to borrow from");
				}

				result = connection.execute(op);
				
				retry.success();
				connPoolMonitor.incOperationSuccess(connection.getHost(), System.currentTimeMillis()-startTime);
				
				return result; 
				
			} catch(NoAvailableHostsException e) {
				connPoolMonitor.incOperationFailure(null, e);
				throw e;
			} catch(DynoException e) {
				
				retry.failure(e);
				lastException = e;
				
				connPoolMonitor.incOperationFailure(connection != null ? connection.getHost() : null, e);
				if (retry.allowRetry()) {
					connPoolMonitor.incFailover(connection.getHost(), e);
				}
				
			} catch(Throwable t) {
				
				Logger.error("Caught throwable", t);
				throw new RuntimeException(t);
			} finally {
				if (result != null) {
					result.setLatency(System.currentTimeMillis()-startTime, TimeUnit.MILLISECONDS);
				}
				if (connection != null) {
					connection.getParentConnectionPool().returnConnection(connection);
				}
			}
			
		} while(retry.allowRetry());
		
		Logger.info("Throwing last ex " + lastException.getMessage());
		throw lastException;
	}


	/**
	 * Asynchronously executes  the {@link Operation} using the {@link MemcachedConnectionPool} underneath.
	 * It also maintains {@link ConnectionPoolMonitor} metrics. Since this is an async operation, we don't do any 
	 * retries. That is upto the caller of the api.
	 *
	 * @return Future<OperationResult<R>>
	 */
	@Override
	public <R> Future<OperationResult<R>> executeAsync(AsyncOperation<CL, R> op) throws DynoException {
		
		Connection<CL> connection = null;
		long startTime = System.currentTimeMillis();
		
		try { 
			
			HostConnectionPool<CL> pool = innerState.get().hostConnectionPool; 
			if (pool == null) {
				throw new NoAvailableHostsException("No host connection pool setup");
			}
			
			connection = pool.borrowConnection(cpConfiguration.getMaxTimeoutWhenExhausted(), TimeUnit.MILLISECONDS); 
			if (connection == null) {
				throw new NoAvailableHostsException("No hosts to borrow from");
			}

			Future<OperationResult<R>> futureResult = connection.executeAsync(op);
			
			connPoolMonitor.incOperationSuccess(connection.getHost(), System.currentTimeMillis()-startTime);
			
			return futureResult; 
			
		} catch(NoAvailableHostsException e) {
			connPoolMonitor.incOperationFailure(null, e);
			throw e;
		} catch(DynoException e) {
			
			connPoolMonitor.incOperationFailure(connection != null ? connection.getHost() : null, e);
			
		} catch(Throwable t) {
			t.printStackTrace();
			throw new RuntimeException(t);
		} finally {
			if (connection != null) {
				connection.getParentConnectionPool().returnConnection(connection);
			}
		}
		return null;
	}
	
	@Override
	public void shutdown() {

		MemcachedConnectionPool<CL> activePool = innerState.get().hostConnectionPool;
		if (activePool != null) {
			activePool.shutdown();
		}
		threadPool.shutdownNow();
	}

	@Override
	public  Future<Boolean> start() throws DynoException {
		
		if (active.get()) {
			Logger.info("Connection pool is already active, ignoring request");
			return getEmptyFutureTask(false);
		}
		
		active.set(true);
		return processUpdateFromHostSupplier();
	}
	
	private Future<Boolean> processUpdateFromHostSupplier() throws DynoException {

		Collection<Host> hosts = cpConfiguration.getHostSupplier().getHosts();
		
		List<Host> hostsUp = new ArrayList<Host>();
		List<Host> hostsDown = new ArrayList<Host>();
		
		for (Host host : hosts) {
			if (host.isUp()) {
				hostsUp.add(host);
			} else {
				hostsDown.add(host);
			}
		}
		
		return reconnect(hostsUp, hostsDown);
	}
	
	private Future<Boolean> getEmptyFutureTask(final Boolean condition) {
		
		final Callable<Boolean> task = new Callable<Boolean>() {
			@Override
			public Boolean call() throws Exception {
				return condition;
			}
		};
		
		try { 
			task.call();
		} catch (Exception e) {
			// do nothing here.
		}
		return new FutureTask<Boolean>(task);
	}
	
	/**
	 * Main method that can potentially cause a change in the connections for this connection pool. 
	 * Note that the inner {@link HostConnectionPool} is based off of {@link MemcachedConnectionPool}
	 * which is a wrapper over {@link MemcachedClient} from the spy memcached library. 
	 * MemcachedClient maintains an async connection to all the hosts in the pool. Hence when there are 
	 * new hosts added or existing hosts removed, we recycle the whole  MemcachedClient via the MemcachedConnectionPool
	 * 
	 * This is what the reconnect() method does. 
	 * 1. It creates a new MemcachedConnectionPool with a new set of hosts which include the new active hosts and exclude the newly inactive hosts.
	 * 2. The newly created MemcachedConnectionPool is added to the active pool set via the reference "innerState"
	 * 3  Now all new requests are being sent to the new pool and we wait for inflight requests to finish with the older pool for 
	 *    {@link ConnectionPoolConfiguration#getPoolShutdownDelay()} 
	 * 4. Then the older pool is finally terminated using a threadpool in another thread. This allows the caller of the method to return
	 *    without blocking for the delay {@link ConnectionPoolConfiguration#getPoolShutdownDelay()}. 
	 * 5. A Future<Boolean> is returned for callers interested in knowing when the older pool is actually terminated. 
	 *
	 * 
	 * @param activeHosts
	 * @param inactiveHosts
	 * @return
	 * @throws DynoException
	 */
	private Future<Boolean> reconnect(Collection<Host> activeHosts, Collection<Host> inactiveHosts) throws DynoException {
		
		if (reconnecting.get()) {
			Logger.info("Connection pool is already reconnecting, ignoring request");
			return getEmptyFutureTask(false);
		}

		if (!(reconnecting.compareAndSet(false, true))) {
			// someone already beat us to it. 
			return getEmptyFutureTask(false);
		}
		
		
		/** ====== Ok, we won the CAS. Go ahead and create the pool. === */
		Logger.info("Reconnecting connection pool with \nnew active hosts hosts: " + activeHosts
				+ " \nand inactive hosts: " + inactiveHosts);
		
		// Track the state change counter. Useful for stats
		int count = stateChangeCount.incrementAndGet();
		
		// Create the new host group from the new set of hosts (if any)
		HostGroup allHosts = new HostGroup("AllHosts" + count, cpConfiguration.getPort());
		allHosts.add(activeHosts);
		
		HostStatusTracker newTracker = new HostStatusTracker(activeHosts, inactiveHosts);
		
		// Create the new inner connection pool for these new hosts
		MemcachedConnectionPool<CL> connPool = 
				new MemcachedConnectionPool<CL>(allHosts, cpConfiguration, connPoolMonitor, connFactory, connObservor, operationMonitor);
		// Prime the connection pool, so that the connections are ready for traffic.
		connPool.primeConnections();
		
		// Now make the BIG SWAP for all traffic
		final InnerState newState = new InnerState(newTracker, connPool);
		final InnerState oldState = innerState.get(); 
		innerState.compareAndSet(oldState, newState);
		
		// Now shutdown the older pool, but let the inflight requests complete
		Future<Boolean> future = threadPool.submit(new Callable<Boolean>() {

			@Override
			public Boolean call() throws Exception {
				
				Thread.currentThread().setName("Pool-Shutdown");
				
				if (oldState.hostConnectionPool != null) {
					
					Logger.info("Sleeping for " + cpConfiguration.getPoolShutdownDelay() + 
							" before shutting down to allow in flight requests to complete");
					Thread.sleep(cpConfiguration.getPoolShutdownDelay());
					Logger.info("Shutting down older conn pool");
					oldState.hostConnectionPool.shutdown();
				}
				return true;
			}
		});
		
		reconnecting.set(false);
		
		return future;
	}
	
	/**
	 * Private inner class that tracks the core state of the {@link MemcachedConnectionPool} and the active set of hosts for that pool.
	 * When we need to create a new pool via {@link RollingMemcachedConnectionPoolImpl#reconnect(Collection, Collection)} a new object 
	 * of this class is created and swapped atomically with the older InnerState. 
	 * 
	 * @author poberai
	 *
	 */
	private class InnerState {
		
		private final HostStatusTracker hostTracker;
		private final MemcachedConnectionPool<CL> hostConnectionPool;
		
		private InnerState(HostStatusTracker tracker, MemcachedConnectionPool<CL> connPool) {
			this.hostTracker = tracker;
			this.hostConnectionPool = connPool;
		}
		private InnerState() {
			this.hostTracker = new HostStatusTracker();
			this.hostConnectionPool = null;
		}
	}

	private class MemcachedConnectionObserver implements ConnectionObservor {

		private final RollingMemcachedConnectionPoolImpl<CL> connPool; 
		
		private MemcachedConnectionObserver(RollingMemcachedConnectionPoolImpl<CL> pool) {
			this.connPool = pool;
		}
		
		@Override
		public void connectionEstablished(Host host) {
			Logger.info("Connection established for host: " + host);
		}

		@Override
		public void connectionLost(Host host) {
			Logger.info("Connection lost for host: " + host + ", recycling host connection pool");
			connPool.removeHost(host);
		}
	}
	
	public static class UnitTest {
		
		private static class TestClient {
			
			private final AtomicInteger ops = new AtomicInteger(0);
		}

		private static TestClient client = new TestClient();
		
		private static ConnectionPoolConfigurationImpl cpConfig = new ConnectionPoolConfigurationImpl("TestClient");
		private static CountingConnectionPoolMonitor cpMonitor = new CountingConnectionPoolMonitor();
		private static OperationMonitor opMonitor = new LastOperationMonitor();
		
		private static class TestConnection implements Connection<TestClient> {

			private AtomicBoolean open = new AtomicBoolean(true);
			private AtomicInteger ops = new AtomicInteger(0);
			private DynoConnectException ex; 
			
			private HostConnectionPool<TestClient> hostPool;
			
			private TestConnection(HostConnectionPool<TestClient> pool) {
				this.hostPool = pool;
			}
			
			@Override
			public <R> OperationResult<R> execute(Operation<TestClient, R> op) throws DynoException {

				if (!open.get()) {
					throw new RuntimeException("Connection not open!");
				}
				try {
					if (op != null) {
						op.execute(client, null);
					}
				} catch (DynoConnectException e) {
					ex = e;
					throw e;
				}
				ops.incrementAndGet();
				return null;
			}

			@Override
			public void close() {
				open.set(false);
			}

			@Override
			public Host getHost() {
				return hostPool.getHost();
			}

			@Override
			public void open() throws DynoException {
				open.set(true);
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
		}
		
		private static ConnectionFactory<TestClient> connFactory = new ConnectionFactory<TestClient>() {

			@Override
			public Connection<TestClient> createConnection(HostConnectionPool<TestClient> pool, ConnectionObservor connObservor) throws DynoConnectException, ThrottledException {
				return new TestConnection(pool);
			}
		};
		
		private static Host host1 = new Host("host1", 8080, Status.Up);
		private static Host host2 = new Host("host2", 8080, Status.Up);
		private static Host host3 = new Host("host3", 8080, Status.Up);
		
		@Before
		public void beforeTest() {
			
			client = new TestClient();
			cpConfig = new ConnectionPoolConfigurationImpl("TestClient");
			cpMonitor = new CountingConnectionPoolMonitor();
		}
		
		@Test
		public void testConnectionPoolNormal() throws Exception {

			final RollingMemcachedConnectionPoolImpl<TestClient> pool = 
					new RollingMemcachedConnectionPoolImpl<TestClient>("Test", connFactory, cpConfig, cpMonitor, opMonitor);
			
			pool.updateHosts(Arrays.asList(host1, host2), Collections.<Host> emptyList());
			
			final Callable<Void> testLogic = new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					Thread.sleep(1000);
					return null;
				}
			};
			
			runTest(pool, testLogic);
			
			checkConnectionPoolMonitorStats(1);
			
			checkHostStats(pool.getActivePools().get(0).getHost());
		}
		
		@Test
		public void testAddingNewHosts() throws Exception {
			
			final RollingMemcachedConnectionPoolImpl<TestClient> pool = 
					new RollingMemcachedConnectionPoolImpl<TestClient>("Test", connFactory, cpConfig, cpMonitor, opMonitor);
			
			cpConfig.setPoolShutdownDelay(500);
			
			pool.updateHosts(Arrays.asList(host1, host2), Collections.<Host> emptyList());
			
			
			final Callable<Void> testLogic = new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					Thread.sleep(1000);
					
					HostConnectionPool<TestClient> hostPool = pool.getActivePools().get(0);
					
					HostGroup hGroupBeforeAddingHosts = (HostGroup) hostPool.getHost();
					Assert.assertEquals("AllHosts1", hGroupBeforeAddingHosts.getHostName());
					Assert.assertTrue(hostPool.isActive());
					
					Assert.assertEquals(host1.getHostName(), hGroupBeforeAddingHosts.getHostList().get(0).getHostName());
					Assert.assertEquals(host2.getHostName(), hGroupBeforeAddingHosts.getHostList().get(1).getHostName());
					
					Future<Boolean> reconnect = pool.updateHosts(Arrays.asList(host1, host2, host3), Collections.<Host> emptyList());
					Boolean reconnected = reconnect.get();
					Assert.assertTrue("Reconnected: " + reconnected, reconnected);
					Assert.assertFalse(hostPool.isActive());
					
					HostConnectionPool<TestClient> newHostPool = pool.getActivePools().get(0);
					Assert.assertTrue(newHostPool.isActive());
					
					HostGroup hGroupAfterAddingHosts = (HostGroup) newHostPool.getHost();
					Assert.assertEquals(host1.getHostName(), hGroupAfterAddingHosts.getHostList().get(0).getHostName());
					Assert.assertEquals(host2.getHostName(), hGroupAfterAddingHosts.getHostList().get(1).getHostName());
					Assert.assertEquals(host3.getHostName(), hGroupAfterAddingHosts.getHostList().get(2).getHostName());
					
					return null;
				}
			};
			
			runTest(pool, testLogic);
			
			checkConnectionPoolMonitorStats(2);
		}

		@Test
		public void testRemovingHosts() throws Exception {
			
			cpConfig.setPoolShutdownDelay(500);
			
			final RollingMemcachedConnectionPoolImpl<TestClient> pool = 
					new RollingMemcachedConnectionPoolImpl<TestClient>("Test", connFactory, cpConfig, cpMonitor, opMonitor);
			pool.updateHosts(Arrays.asList(host1, host2, host3), Collections.<Host>emptyList());
			
			final Callable<Void> testLogic = new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					Thread.sleep(500);
					HostConnectionPool<TestClient> hostPool = pool.getActivePools().get(0);
					
					HostGroup hGroupBeforeAddingHosts = (HostGroup) hostPool.getHost();
					Assert.assertEquals("AllHosts1", hGroupBeforeAddingHosts.getHostName());
					Assert.assertTrue(hostPool.isActive());
					
					Assert.assertEquals(host1.getHostName(), hGroupBeforeAddingHosts.getHostList().get(0).getHostName());
					Assert.assertEquals(host2.getHostName(), hGroupBeforeAddingHosts.getHostList().get(1).getHostName());
					Assert.assertEquals(host3.getHostName(), hGroupBeforeAddingHosts.getHostList().get(2).getHostName());

					Future<Boolean> reconnect = pool.updateHosts(Arrays.asList(host1, host3), Arrays.asList(host2));
					Boolean reconnected = reconnect.get();

					Assert.assertTrue("Reconnected: " + reconnected, reconnected);
					Assert.assertFalse(hostPool.isActive());
					
					HostConnectionPool<TestClient> newHostPool = pool.getActivePools().get(0);
					Assert.assertTrue(newHostPool.isActive());
					
					HostGroup hGroupAfterAddingHosts = (HostGroup) newHostPool.getHost();
					Assert.assertEquals(host1.getHostName(), hGroupAfterAddingHosts.getHostList().get(0).getHostName());
					Assert.assertEquals(host3.getHostName(), hGroupAfterAddingHosts.getHostList().get(1).getHostName());

					return null;
				}
			};
			
			runTest(pool, testLogic);
		}
		
		@Test
		public void testBadConnection() throws Exception {
			

			cpConfig.setPoolShutdownDelay(500);
			
			final RollingMemcachedConnectionPoolImpl<TestClient> pool = 
					new RollingMemcachedConnectionPoolImpl<TestClient>("Test", connFactory, cpConfig, cpMonitor, opMonitor);
			pool.updateHosts(Arrays.asList(host1, host2, host3), Collections.<Host>emptyList());
			
			final Callable<Void> testLogic = new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					Thread.sleep(500);
					
					HostConnectionPool<TestClient> hostPool = pool.getActivePools().get(0);

					HostGroup hGroupBeforeAddingHosts = (HostGroup) hostPool.getHost();
					Assert.assertEquals("AllHosts1", hGroupBeforeAddingHosts.getHostName());
					Assert.assertTrue(hostPool.isActive());
					
					Assert.assertEquals(host1.getHostName(), hGroupBeforeAddingHosts.getHostList().get(0).getHostName());
					Assert.assertEquals(host2.getHostName(), hGroupBeforeAddingHosts.getHostList().get(1).getHostName());
					Assert.assertEquals(host3.getHostName(), hGroupBeforeAddingHosts.getHostList().get(2).getHostName());
					
					// NOW MAKE ONE CONN GOP BAD
					pool.connObservor.connectionLost(host1);
					Thread.sleep(1000);

					Assert.assertFalse(hostPool.isActive());
					
					HostConnectionPool<TestClient> newHostPool = pool.getActivePools().get(0);
					Assert.assertTrue(newHostPool.isActive());
					
					HostGroup hGroupAfterAddingHosts = (HostGroup) newHostPool.getHost();
					Assert.assertEquals(host2.getHostName(), hGroupAfterAddingHosts.getHostList().get(0).getHostName());
					Assert.assertEquals(host3.getHostName(), hGroupAfterAddingHosts.getHostList().get(1).getHostName());

					return null;
				}
			};

			runTest(pool, testLogic);
		}

		
		private void checkHostStats(Host host) {

			HostConnectionStats hStats = cpMonitor.getHostStats().get(host);
			
			System.out.println(cpMonitor.getHostStats().keySet());
			System.out.println("hStats: " + hStats.toString());

			Assert.assertTrue("host ops: " + hStats.getOperationSuccessCount(), hStats.getOperationSuccessCount() > 0);
			Assert.assertEquals(0, hStats.getOperationErrorCount());
			Assert.assertEquals(cpConfig.getMaxConnsPerHost(), hStats.getConnectionsCreated());
			Assert.assertEquals(0, hStats.getConnectionsCreateFailed());
			Assert.assertEquals(cpConfig.getMaxConnsPerHost(), hStats.getConnectionsClosed());
//			Assert.assertEquals(hStats.getOperationSuccessCount(), hStats.getConnectionsBorrowed());
//			Assert.assertEquals(hStats.getOperationSuccessCount(), hStats.getConnectionsReturned());
		}

		private void checkConnectionPoolMonitorStats(int expectedConnCreateCount)  {
			Assert.assertTrue("Total ops: " + client.ops.get(), client.ops.get() > 0);

			Assert.assertEquals(client.ops.get(), cpMonitor.getOperationSuccessCount());
			Assert.assertEquals(0, cpMonitor.getOperationFailureCount());
			Assert.assertEquals(0, cpMonitor.getOperationTimeoutCount());
			
			Assert.assertEquals(expectedConnCreateCount, cpMonitor.getConnectionCreatedCount());
			Assert.assertEquals(0, cpMonitor.getConnectionCreateFailedCount());
			Assert.assertEquals(expectedConnCreateCount, cpMonitor.getConnectionClosedCount());
			
			Assert.assertEquals(client.ops.get(), cpMonitor.getConnectionBorrowedCount());
			Assert.assertEquals(client.ops.get(), cpMonitor.getConnectionReturnedCount());
		}
		
		private void runTest(final RollingMemcachedConnectionPoolImpl<TestClient> pool, final Callable<Void> customTestLogic) throws Exception {

			int nThreads = 4;
			final ExecutorService threadPool = Executors.newFixedThreadPool(nThreads);
			final AtomicBoolean stop = new AtomicBoolean(false);

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
											return "testOperation";
										}
										

										@Override
										public String getKey() {
											return "TestOperation";
										}
									});
								} catch (DynoException e) {
									e.printStackTrace();
								}
							}

						} finally {
						}
						return null;
					}
				});
			}
			
			customTestLogic.call();
			
			stop.set(true);
			threadPool.shutdownNow();
			pool.shutdown();
		}
	}
}
