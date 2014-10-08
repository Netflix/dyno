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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.AsyncOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.ConnectionFactory;
import com.netflix.dyno.connectionpool.ConnectionObservor;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.RetryPolicy;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.FatalConnectionException;
import com.netflix.dyno.connectionpool.exception.PoolOfflineException;
import com.netflix.dyno.connectionpool.exception.PoolTimeoutException;
import com.netflix.dyno.connectionpool.exception.ThrottledException;

/**
 * Main impl for {@link HostConnectionPool}
 * 
 * This class does not allow shared access to the connections being managed for this pool. 
 * Hence it uses a {@link LinkedBlockingQueue} to manage the available connections. 
 * When a connection needs to be borrowed, we wait or poll the queue. As connections are returned, they are added back into the queue. 
 * This is the normal behavior during the "Active" state of this pool. 
 * 
 * The class also manages another state called "Inactive" where it can be put "Down" where it stops accepting requests for borrowing more connections, 
 * and simply terminates every connection that is returned to it. This is generally useful when the host is going away, or where the error rate 
 * from the connections of this pool are greater than a configured error threshold and then an external component decides to recycle the connection pool. 
 * 
 * @author poberai
 *
 * @param <CL>
 */
public class HostConnectionPoolImpl<CL> implements HostConnectionPool<CL> {

	private static final Logger Logger = LoggerFactory.getLogger(HostConnectionPoolImpl.class);
	
	// The connections available for this connection pool
	private final LinkedBlockingQueue<Connection<CL>> availableConnections = new LinkedBlockingQueue<Connection<CL>>();
	// Track the no of connections open (both available and in use)
	private final AtomicInteger numActiveConnections = new AtomicInteger(0);
	
	// Private members required by this class
	private final Host host; 
	private final ConnectionFactory<CL> connFactory; 
	private final ConnectionPoolConfiguration cpConfig; 
	private final ConnectionPoolMonitor monitor; 
	
	// states that dictate the behavior of the pool
	
	// cp not inited is the starting state of the pool. The pool will not allow connections to be borrowed in this state
	private final ConnectionPoolState<CL> cpNotInited = new ConnectionPoolNotInited();
	// cp active is where connections of the pool can be borrowed and returned
	private final ConnectionPoolState<CL> cpActive = new ConnectionPoolActive(this);
	// cp reconnecting is where connections cannot be borrowed and all returning connections will be shutdown
	private final ConnectionPoolState<CL> cpReconnecting = new ConnectionPoolReconnectingOrDown();
	// similar to reconnecting
	private final ConnectionPoolState<CL> cpDown = new ConnectionPoolReconnectingOrDown();
	
	// The thread safe reference to the pool state
	private final AtomicReference<ConnectionPoolState<CL>> cpState = new AtomicReference<ConnectionPoolState<CL>>(cpNotInited);
	
	public HostConnectionPoolImpl(Host host, ConnectionFactory<CL> conFactory, 
			                      ConnectionPoolConfiguration cpConfig, ConnectionPoolMonitor poolMonitor) {
			                      //ExecutorService thPool) {
		this.host = host;
		this.connFactory = conFactory;
		this.cpConfig = cpConfig;
		this.monitor = poolMonitor;
	}
	
	@Override
	public Connection<CL> borrowConnection(int duration, TimeUnit unit) throws DynoException {
		return cpState.get().borrowConnection(duration, unit);
	}

	@Override
	public boolean returnConnection(Connection<CL> connection) {
		return cpState.get().returnConnection(connection);
	}

	@Override
	public boolean closeConnection(Connection<CL> connection) {
		return cpState.get().closeConnection(connection);
	}

	@Override
	public void markAsDown(DynoException reason) {
		
		ConnectionPoolState<CL> currentState = cpState.get();
		
		if (currentState == cpDown) {
			if (Logger.isDebugEnabled()) {
				Logger.debug("CP is already down, hence ignoring mark as down request");
			}
			return;
		}
		
		if (!(cpState.compareAndSet(currentState, cpDown))) {
			// someone already beat us to it
			return;
		}
		
		monitor.hostDown(host, reason);
	}

	@Override
	public void reconnect() {
		
		markAsDown(null);
		reconnect(cpDown);
	
		if (cpState.get() == cpActive) {
			monitor.hostUp(host, this);
		}
	}

	@Override
	public void shutdown() {
		
		Logger.info("Shutting down connection pool for host:" + host);
		cpState.set(cpDown);
		
		List<Connection<CL>> connections = new ArrayList<Connection<CL>>();
		availableConnections.drainTo(connections);
		
		for (Connection<CL> connection : connections) {
			cpState.get().closeConnection(connection);
		}
	}

	@Override
	public int primeConnections() throws DynoException {

		Logger.info("Priming connection pool for host:" + host + ", with conns:" + cpConfig.getMaxConnsPerHost());

		if(cpState.get() != cpNotInited) {
			throw new DynoException("Connection pool has already been inited, cannot prime connections for host:" + host);
		}
		
		return reconnect(cpNotInited);
	}

	private int reconnect(ConnectionPoolState<CL> prevState) throws DynoException {

		if (!(cpState.compareAndSet(prevState, cpReconnecting))) {
			Logger.info("Reconnect connections already called by someone else, ignoring reconnect connections request");
			return 0;
		}
		
		int successfullyCreated = 0; 
		
		for (int i=0; i<cpConfig.getMaxConnsPerHost(); i++) {
			boolean success = createConnectionWithReries(); 
			if (success) {
				successfullyCreated++;
			}
		}
		
		if (successfullyCreated == cpConfig.getMaxConnsPerHost()) {
			if (!(cpState.compareAndSet(cpReconnecting, cpActive))) {
				throw new IllegalStateException("something went wrong with prime connections");
			}
		} else {
			if (!(cpState.compareAndSet(cpReconnecting, cpDown))) {
				throw new IllegalStateException("something went wrong with prime connections");
			}
		}
		return successfullyCreated;
	}
	
	private boolean createConnectionWithReries() {
		
		boolean success = false;
		RetryPolicy retry = new RetryNTimes.RetryFactory(3).getRetryPolicy();
		
		retry.begin();
		
		while (retry.allowRetry()) {
			
			try {
				cpActive.createConnection();
				retry.success();
				success = true;
				break;
			} catch (DynoException e) {
				retry.failure(e);
			}
		}
		
		return success;
	}

	@Override
	public Host getHost() {
		return host;
	}

	@Override
	public boolean isActive() {
		return cpState.get() == cpActive;
	}

	@Override
	public boolean isShutdown() {
		return cpState.get() == cpDown;
	}
	
	/**
	 * DO NOT call this method on this pool. This pool needs to manage shared thread safe access to connections
	 * and hence at any given time all connections are being used by some operation. 
	 * In any case getAllConnections() is meant for ping based active monitoring of connections which is not needed for this 
	 * pool since it is "sync" in nature. For sync pools we collect feedback from the operations directly and relay that to 
	 * ConnectionPoolHealthChecker.
	 * 
	 */
	@Override
	public Collection<Connection<CL>> getAllConnections() {
		throw new RuntimeException("Not Implemented");
	}

	private interface ConnectionPoolState<CL> { 
		
		
		Connection<CL> createConnection(); 
		
		Connection<CL> borrowConnection(int duration, TimeUnit unit);
		
		boolean returnConnection(Connection<CL> connection);
		
		boolean closeConnection(Connection<CL> connection);
		
	}
	
	
	private class ConnectionPoolActive implements ConnectionPoolState<CL> {

		private final HostConnectionPoolImpl<CL> pool; 
		
		private ConnectionPoolActive(HostConnectionPoolImpl<CL> cp) {
			pool = cp;
		}
		
		@Override
		public Connection<CL> createConnection() {
			
			try { 
				Connection<CL> connection = connFactory.createConnection((HostConnectionPool<CL>) pool, null);
				connection.open();
				availableConnections.add(connection);

				monitor.incConnectionCreated(host);
				numActiveConnections.incrementAndGet();
				
				return connection;
			} catch (DynoConnectException e) {
				if (Logger.isDebugEnabled()) {
					Logger.error("Failed to create connection", e);
				} else {
					//Logger.error("Failed to create connection" + e.getMessage());
				}
				monitor.incConnectionCreateFailed(host, e);
				throw e;
			} catch (RuntimeException e) {
				if (Logger.isDebugEnabled()) {
					Logger.error("Failed to create connection", e);
				} else {
					//Logger.error("Failed to create connection" + e.getMessage());
				}
				monitor.incConnectionCreateFailed(host, e);
				throw new DynoConnectException(e);
			}
		}


		@Override
		public boolean returnConnection(Connection<CL> connection) {
			try {
				if (numActiveConnections.get() > cpConfig.getMaxConnsPerHost()) {
					
					// Just close the connection
					return closeConnection(connection);
					
				} else {
					// add connection back to the pool
					availableConnections.add(connection);
					return false;
				}
			} finally { 
				monitor.incConnectionReturned(host);
			}
		}

		@Override
		public boolean closeConnection(Connection<CL> connection) {
			try  {
				connection.close();
				return true;
			} catch (Exception e) {
				Logger.error("Failed to close connection for host: " + host + " " + e.getMessage());
				return false;
			} finally {
				numActiveConnections.decrementAndGet();
				monitor.incConnectionClosed(host, connection.getLastException());
			}
		}
		
		@Override
		public Connection<CL> borrowConnection(int duration, TimeUnit unit) {

			// Start recording how long it takes to get the connection - for insight/metrics
			long startTime = System.currentTimeMillis();

			Connection<CL> conn = null;
			try {
				// wait on the connection pool with a timeout
				conn = availableConnections.poll(duration, unit);
			} catch (InterruptedException e) {
				Logger.info("Thread interrupted when waiting on connections");
				throw new DynoConnectException(e);
			}

			long delay = System.currentTimeMillis() - startTime;

			if (conn == null) {
				throw new PoolTimeoutException("Fast fail waiting for connection from pool")
				.setHost(getHost())
				.setLatency(delay);
			}

			monitor.incConnectionBorrowed(host, delay);
			return conn;
		}
	}


	
	private class ConnectionPoolReconnectingOrDown implements ConnectionPoolState<CL> {
		
		private ConnectionPoolReconnectingOrDown() {
		}

		@Override
		public Connection<CL> createConnection() {
			throw new PoolOfflineException(getHost(), "Cannot create new connection when pool is down");
		}

		@Override
		public Connection<CL> borrowConnection(int duration, TimeUnit unit) {
			throw new PoolOfflineException(getHost(), "Cannot borrow connection when pool is down");
		}

		@Override
		public boolean returnConnection(Connection<CL> connection) {
			
			monitor.incConnectionReturned(host);
			return closeConnection(connection);
		}

		@Override
		public boolean closeConnection(Connection<CL> connection) {
			try  {
				connection.close();
				return true;
			} catch (Exception e) {
				Logger.warn("Failed to close connection for host: " + host + " " + e.getMessage());
				return false;
			} finally {
				numActiveConnections.decrementAndGet();
				monitor.incConnectionClosed(host, connection.getLastException());
			}
		}
	}
	
	private class ConnectionPoolNotInited implements ConnectionPoolState<CL> {
		
		private ConnectionPoolNotInited() {
		}

		@Override
		public Connection<CL> createConnection() {
			throw new DynoConnectException("Pool must be inited first");
		}

		@Override
		public Connection<CL> borrowConnection(int duration, TimeUnit unit) {
			throw new DynoConnectException("Pool must be inited first");
		}

		@Override
		public boolean returnConnection(Connection<CL> connection) {
			throw new DynoConnectException("Pool must be inited first");
		}

		@Override
		public boolean closeConnection(Connection<CL> connection) {
			throw new DynoConnectException("Pool must be inited first");
		}
	}
	
	public String toString() {
		return "HostConnectionPool: [Host: " + host + ", Pool active: " + isActive() + "]";
	}
	
	public static class UnitTest { 
		
		private static final Host TestHost = new Host("TestHost", 1234);
		
		// TEST UTILS SETUP
		private class TestClient {
			
		}
		
		//private static AtomicBoolean stop = new AtomicBoolean(false);
		private static HostConnectionPoolImpl<TestClient> pool;
		private static ExecutorService threadPool;
		private static int numWorkers = 10;
		
		private static class TestConnection implements Connection<TestClient> {

			private DynoConnectException ex;
			private HostConnectionPool<TestClient> myPool;
			
			private TestConnection(HostConnectionPool<TestClient> pool) {
				myPool = pool;
			}
			@Override
			public <R> OperationResult<R> execute(Operation<TestClient, R> op) throws DynoException {
				return null;
			}

			@Override
			public void close() {
			}

			@Override
			public Host getHost() {
				return TestHost;
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
				return myPool;
			}
			
			public void setException(DynoConnectException e) {
				ex = e;
			}

			@Override
			public <R> Future<OperationResult<R>> executeAsync(AsyncOperation<TestClient, R> op) throws DynoException {
				throw new RuntimeException("Not Implemented");
			}

			@Override
			public void execPing() {
				// do nothing
			}

			@Override
			public ConnectionContext getContext() {
				return null;
			}
		}
		
		private static ConnectionFactory<TestClient> connFactory = new ConnectionFactory<TestClient>() {

			@Override
			public Connection<TestClient> createConnection(HostConnectionPool<TestClient> pool, ConnectionObservor cObservor) throws DynoConnectException, ThrottledException {
				return new TestConnection(pool);
			}
		};
		
		private static ConnectionPoolConfigurationImpl config = new ConnectionPoolConfigurationImpl("TestClient");
		private static CountingConnectionPoolMonitor cpMonitor = new CountingConnectionPoolMonitor();
		
		@BeforeClass
		public static void beforeClass() {
			threadPool = Executors.newFixedThreadPool(numWorkers);
		}
		
		@Before
		public void beforeTest() {
			//stop.set(false);
			cpMonitor = new CountingConnectionPoolMonitor(); // reset all monitor stats
		}
		
		@After
		public void afterTest() {
			if (pool != null) {
				pool.shutdown();
			}
		}

		@AfterClass
		public static void afterClass() {
			threadPool.shutdownNow();
		}
		
		@Test
		public void testRegularProcess() throws Exception {
			
			final BasicResult result = new BasicResult();
			final TestControl control = new TestControl(4);
			
			pool = new HostConnectionPoolImpl<TestClient>(TestHost, connFactory, config, cpMonitor);
			int numConns = pool.primeConnections();

			for (int i=0; i<4; i++) {
				threadPool.submit(new BasicWorker(result, control));
			}
			
			Thread.sleep(300);
			
			control.stop();
			control.waitOnFinish();

			pool.shutdown();
			
			Assert.assertEquals(config.getMaxConnsPerHost(), numConns);
			Assert.assertEquals(result.opCount.get(), cpMonitor.getConnectionBorrowedCount());
			Assert.assertEquals(result.opCount.get(), cpMonitor.getConnectionReturnedCount());
			Assert.assertEquals(config.getMaxConnsPerHost(), cpMonitor.getConnectionCreatedCount());
			Assert.assertEquals(config.getMaxConnsPerHost(), cpMonitor.getConnectionClosedCount());
			Assert.assertEquals(0, result.failureCount.get());
		}
		
		@Test
		public void testPoolTimeouts() throws Exception {
		
			pool = new HostConnectionPoolImpl<TestClient>(TestHost, connFactory, config, cpMonitor);
			int numConns = pool.primeConnections();

			final BasicResult result = new BasicResult();
			final TestControl control = new TestControl(4);

			for (int i=0; i<4; i++) {   // Note 4 threads .. which is more than the no of available conns .. hence we should see timeouts
				threadPool.submit(new BasicWorker(result, control, 55));
			}
			
			Thread.sleep(300);
			
			control.stop();
			control.waitOnFinish();
			
			pool.shutdown();
			
			Assert.assertEquals(config.getMaxConnsPerHost(), numConns);

			Assert.assertEquals(result.successCount.get(), cpMonitor.getConnectionBorrowedCount());
			Assert.assertEquals(result.successCount.get(), cpMonitor.getConnectionReturnedCount());
			Assert.assertEquals(config.getMaxConnsPerHost(), cpMonitor.getConnectionCreatedCount());
			Assert.assertEquals(config.getMaxConnsPerHost(), cpMonitor.getConnectionClosedCount());
			Assert.assertEquals(0, cpMonitor.getConnectionCreateFailedCount());
			
			Assert.assertTrue(result.failureCount.get() > 0);
		}
		
		@Test
		public void testMarkHostAsDown() throws Exception {
			
			pool = new HostConnectionPoolImpl<TestClient>(TestHost, connFactory, config, cpMonitor);
			int numConns = pool.primeConnections();

			final BasicResult result = new BasicResult();
			final TestControl control = new TestControl(4);

			for (int i=0; i<4; i++) {   // Note 4 threads .. which is more than the no of available conns .. hence we should see timeouts
				threadPool.submit(new BasicWorker(result, control));
			}
			
			Thread.sleep(500);
			
			Assert.assertTrue(result.opCount.get() > 0);
			Assert.assertEquals(0, result.failureCount.get());

			pool.markAsDown(new FatalConnectionException("mark pool as down"));
			
			Thread.sleep(200);
			
			control.stop();
			control.waitOnFinish();
			
			pool.shutdown();
			
			Assert.assertEquals(config.getMaxConnsPerHost(), numConns);

			Assert.assertEquals(result.successCount.get(), cpMonitor.getConnectionBorrowedCount());
			Assert.assertEquals(result.successCount.get(), cpMonitor.getConnectionReturnedCount());
			Assert.assertEquals(config.getMaxConnsPerHost(), cpMonitor.getConnectionCreatedCount());
			Assert.assertEquals(config.getMaxConnsPerHost(), cpMonitor.getConnectionClosedCount());
			Assert.assertEquals(0, cpMonitor.getConnectionCreateFailedCount());
			
			Assert.assertTrue(result.failureCount.get() > 0);
		}
		
		private class BasicWorker implements Callable<Void> {

			private final BasicResult result;
			private final TestControl control;
			
			private int sleepMs = -1;
			
			private BasicWorker(BasicResult result, TestControl testControl) {
				this.result = result;
				this.control = testControl;
			}
			
			private BasicWorker(BasicResult result, TestControl testControl, int sleep) {
				this.result = result;
				this.control = testControl;
				this.sleepMs = sleep;
			}
			
			@Override
			public Void call() throws Exception {
				
				while (!control.isStopped() && !Thread.currentThread().isInterrupted()) {

					Connection<TestClient> connection = null;
					try {
						connection = pool.borrowConnection(20, TimeUnit.MILLISECONDS);
						if (sleepMs > 0) {
							Thread.sleep(sleepMs);
						}
						pool.returnConnection(connection);
						result.successCount.incrementAndGet();
						result.lastSuccess.set(true);
					} catch (InterruptedException e) {
					} catch (DynoConnectException e) {
						result.failureCount.incrementAndGet();
						result.lastSuccess.set(false);
						if (connection != null) {
							((TestConnection)connection).setException(e);
						}
					} finally {
						result.opCount.incrementAndGet();
					}
				}
				
				control.reportFinish();
				return null;
			}
		}
		
		private class TestControl { 
			
			private final AtomicBoolean stop = new AtomicBoolean(false);
			private final CountDownLatch latch;
			
			private TestControl(int n) {
				latch = new CountDownLatch(n);
			}
			
			private void reportFinish() {
				latch.countDown();
			}
			
			private void waitOnFinish() throws InterruptedException {
				latch.await();
			}
			
			private boolean isStopped() {
				return stop.get();
			}
			
			private void stop() {
				stop.set(true);
			}
		}
		
		private class BasicResult { 
			
			private final AtomicInteger opCount = new AtomicInteger(0);
			private final AtomicInteger successCount = new AtomicInteger(0);
			private final AtomicInteger failureCount = new AtomicInteger(0);
			
			private AtomicBoolean lastSuccess = new AtomicBoolean(false);
			
			private BasicResult() {
			}

			@Override
			public String toString() {
				return "BasicResult [opCount=" + opCount + ", successCount=" + successCount + 
						", failureCount=" + failureCount + ", lastSuccess=" + lastSuccess.get() + "]";
			}
		}
	}
}
