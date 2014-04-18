package com.netflix.dyno.connectionpool.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionFactory;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.FatalConnectionException;
import com.netflix.dyno.connectionpool.exception.InterruptedOperationException;
import com.netflix.dyno.connectionpool.exception.PoolTimeoutException;
import com.netflix.dyno.connectionpool.exception.ThrottledException;


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
	private final ExecutorService recoveryThreadPool;
	
	private final ConnectionPoolState<CL> cpNotInited = new ConnectionPoolNotInited();
	private final ConnectionPoolState<CL> cpActive = new ConnectionPoolActive(this);
	private final ConnectionPoolState<CL> cpReconnecting = new ConnectionPoolReconnectingOrDown();
	private final ConnectionPoolState<CL> cpDown = new ConnectionPoolReconnectingOrDown();
	
	private final AtomicReference<ConnectionPoolState<CL>> cpState = new AtomicReference<ConnectionPoolState<CL>>(cpNotInited);
	
	public HostConnectionPoolImpl(Host host, ConnectionFactory<CL> conFactory, 
			                      ConnectionPoolConfiguration cpConfig, ConnectionPoolMonitor poolMonitor,
			                      ExecutorService thPool) {
		this.host = host;
		this.connFactory = conFactory;
		this.cpConfig = cpConfig;
		this.monitor = poolMonitor;
		this.recoveryThreadPool = thPool;
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
		
		if (currentState != cpActive) {
			if (Logger.isDebugEnabled()) {
				Logger.debug("CP is not active, hence ignoring mark as down request");
			}
			return;
		}
		
		if (!(cpState.compareAndSet(currentState, cpDown))) {
			// someone already beat us to it
			Logger.info("Someone already beat us to marking the host as down, ignoring request");
			return;
		}
		
		monitor.hostDown(host, reason);
		
		final HostConnectionPool<CL> pool = this;
		
		recoveryThreadPool.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				
				Thread.currentThread().setName("DynoConnectionRevivor");
			
				reconnect(cpDown);
				
				if (!cpState.compareAndSet(cpReconnecting, cpActive)) {
					throw new IllegalStateException();
				}
				
				System.out.println("Done Reconnecting. Host up");
				monitor.hostUp(host, pool);
				return null;
			}
		});
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

		Logger.info("Priming connection pool for host:" + host);

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
			try { 
				cpActive.createConnection();
				successfullyCreated++;
			} catch (DynoException e) {
				Logger.info("Will retry priming connection");
			}
		}
		
		if (!(cpState.compareAndSet(cpReconnecting, cpActive))) {
			throw new IllegalStateException("something went wrong with prime connections");
		} else {
			return successfullyCreated;
		}
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
				Connection<CL> connection = connFactory.createConnection((HostConnectionPool<CL>) pool);
				availableConnections.add(connection);

				monitor.incConnectionCreated(host);
				numActiveConnections.incrementAndGet();
				
				return connection;
			} catch (DynoConnectException e) {
				Logger.error("Failed to create connection", e);
				monitor.incConnectionCreateFailed(host, e);
				throw e;
			}
		}


		@Override
		public boolean returnConnection(Connection<CL> connection) {
			try {
				DynoConnectException e = connection.getLastException();
				
				if (numActiveConnections.get() > cpConfig.getMaxConnsPerHost()) {
					
					// Just close the connection
					return closeConnection(connection);
					
				} else if (e != null && e instanceof FatalConnectionException) {
					
					// create another connection and then close this one
					reviveSingleConnection();
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

		private void reviveSingleConnection() {
			
			recoveryThreadPool.submit(new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					
					if (cpState.get() != cpActive) {
						// Do not create the connection
						return null;
					}
					
					Connection<CL> connection = cpState.get().createConnection();

					// Check again if the pool has been shutdown
					if (cpState.get() != cpActive) {
						// Do not create the connection
						cpState.get().closeConnection(connection);
					}
					return null;
				}
			});
		}
		
		@Override
		public boolean closeConnection(Connection<CL> connection) {
			try  {
				connection.close();
				return true;
			} catch (Exception e) {
				Logger.error("Failed to close connection for host: " + host, e);
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
				throw new InterruptedOperationException(e);
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
			throw new DynoConnectException("Cannot create new connection when pool is down");
		}

		@Override
		public Connection<CL> borrowConnection(int duration, TimeUnit unit) {
			throw new DynoConnectException("Cannot borrow connection when pool is down");
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
				Logger.error("Failed to close connection for host: " + host, e);
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
	
	public static class UnitTest { 
		
		private static final Host TestHost = new Host("TestHost", 1234);
		
		// TEST UTILS SETUP
		private class TestClient {
			
		}
		
		private static AtomicBoolean stop = new AtomicBoolean(false);
		private static HostConnectionPoolImpl<TestClient> pool;
		private static ExecutorService threadPool;
		
		private static class TestConnection implements Connection<TestClient> {

			private DynoConnectException ex;
			@Override
			public <R> OperationResult<R> execute(Operation<TestClient, R> op) throws DynoException {
				return null;
			}

			@Override
			public void close() {
			}

			@Override
			public Host getHost() {
				return null;
			}

			@Override
			public void open() throws DynoException {
			}

			@Override
			public void openAsync(AsyncOpenCallback<TestClient> callback) {
			}

			@Override
			public DynoConnectException getLastException() {
				return ex;
			}

			@Override
			public HostConnectionPool<TestClient> getParentConnectionPool() {
				return null;
			}
			
			public void setException(DynoConnectException e) {
				ex = e;
			}
			
		}
		
		private static ConnectionFactory<TestClient> connFactory = new ConnectionFactory<TestClient>() {

			@Override
			public Connection<TestClient> createConnection(HostConnectionPool<TestClient> pool) throws DynoConnectException, ThrottledException {
				return new TestConnection();
			}
			
		};
		private static ConnectionPoolConfigurationImpl config = new ConnectionPoolConfigurationImpl("TestClient");
		private static CountingConnectionPoolMonitor cpMonitor = new CountingConnectionPoolMonitor();
		
		@BeforeClass
		public static void beforeClass() {
			threadPool = Executors.newFixedThreadPool(10);
		}
		
		@Before
		public void beforeTest() {
			stop.set(false);
			cpMonitor = new CountingConnectionPoolMonitor(); // reset all monitor stats
		}
		
		@After
		public void afterTest() {
			stop.set(true);
			if (pool != null) {
				pool.shutdown();
			}
		}

		@AfterClass
		public static void afterClass() {
			stop.set(true);
			threadPool.shutdownNow();
		}
		
		//@Test
		public void testRegularProcess() throws Exception {
			
			pool = new HostConnectionPoolImpl<TestClient>(TestHost, connFactory, config, cpMonitor, threadPool);
			int numConns = pool.primeConnections();
			System.out.println("numConns: " + numConns);

			List<Future<BasicResult>> futures = new ArrayList<Future<BasicResult>>(); 
			
			for (int i=0; i<3; i++) {
				futures.add(threadPool.submit(new BasicWorker()));
			}
			
			Thread.sleep(300);
			
			stop.set(true);
			
			int totalOps = 0;
			for (Future<BasicResult> f : futures) {
				totalOps += f.get().opCount.get();
			}
			
			System.out.println(totalOps);
			
			pool.shutdown();
			
			System.out.println("Conns borrowed: " + cpMonitor.getConnectionBorrowedCount());
			System.out.println("Conns returned: " + cpMonitor.getConnectionReturnedCount());
			System.out.println("Conns created: " + cpMonitor.getConnectionCreatedCount());
			System.out.println("Conns closed: " + cpMonitor.getConnectionClosedCount());
			System.out.println("Conns create failed: " + cpMonitor.getConnectionCreateFailedCount());
		}
		
		//@Test
		public void testPoolTimeouts() throws Exception {
		
			pool = new HostConnectionPoolImpl<TestClient>(TestHost, connFactory, config, cpMonitor, threadPool);
			int numConns = pool.primeConnections();
			System.out.println("numConns: " + numConns);

			List<Future<BasicResult>> futures = new ArrayList<Future<BasicResult>>(); 
			
			for (int i=0; i<4; i++) {   // Note 4 threads .. which is more than the no of available conns .. hence we should see timeouts
				futures.add(threadPool.submit(new BasicWorker(55)));
			}
			
			Thread.sleep(300);
			
			stop.set(true);
			
			BasicResult result = new BasicResult();
			for (Future<BasicResult> f : futures) {
				result.addResult(f.get());
			}
			
			System.out.println(result.toString());
			
			pool.shutdown();
			
			System.out.println("\n\nConns borrowed: " + cpMonitor.getConnectionBorrowedCount());
			System.out.println("Conns returned: " + cpMonitor.getConnectionReturnedCount());
			System.out.println("Conns created: " + cpMonitor.getConnectionCreatedCount());
			System.out.println("Conns closed: " + cpMonitor.getConnectionClosedCount());
			System.out.println("Conns create failed: " + cpMonitor.getConnectionCreateFailedCount());
		}
		
		//@Test
		public void testCloseBadConnections() throws Exception {
			
			pool = new HostConnectionPoolImpl<TestClient>(TestHost, connFactory, config, cpMonitor, threadPool);
			int numConns = pool.primeConnections();
			System.out.println("numConns: " + numConns);

			final List<Future<BasicResult>> futures = new ArrayList<Future<BasicResult>>(); 
			
			final List<WorkerThatCanFailOperations> workers = new ArrayList<WorkerThatCanFailOperations>();
			
			for (int i=0; i<2; i++) {   // Note 4 threads .. which is more than the no of available conns .. hence we should see timeouts
				WorkerThatCanFailOperations worker = new WorkerThatCanFailOperations();
				workers.add(worker);
				futures.add(threadPool.submit(worker));
			}
			
			Thread.sleep(300);
			
			final AtomicBoolean stopFailingConnections = new AtomicBoolean(false);
			
			Future<Integer> failCount = threadPool.submit(new Callable<Integer>() {

				@Override
				public Integer call() throws Exception {
					Random rand = new Random();
					Integer count = 0;
					while (!stopFailingConnections.get() && !Thread.currentThread().isInterrupted()) {
						workers.get(rand.nextInt(workers.size())).failOperation.set(true);
						count++;
						Thread.sleep(20);
					}
					return count;
				}
				
			});
			
			Thread.sleep(1000);
			
			stopFailingConnections.set(true);
			Thread.sleep(100);
			stop.set(true);
			
			BasicResult result = new BasicResult();
			for (Future<BasicResult> f : futures) {
				result.addResult(f.get());
			}
			
			System.out.println(result.toString());
			System.out.println("\nConnections failed: " + failCount.get());
			
			pool.shutdown();
			
			System.out.println("\n\nConns borrowed: " + cpMonitor.getConnectionBorrowedCount());
			System.out.println("Conns returned: " + cpMonitor.getConnectionReturnedCount());
			System.out.println("Conns created: " + cpMonitor.getConnectionCreatedCount());
			System.out.println("Conns closed: " + cpMonitor.getConnectionClosedCount());
			System.out.println("Conns create failed: " + cpMonitor.getConnectionCreateFailedCount());
			
		}
		
		@Test
		public void testMarkHostAsDown() throws Exception {
			
			pool = new HostConnectionPoolImpl<TestClient>(TestHost, connFactory, config, cpMonitor, threadPool);
			int numConns = pool.primeConnections();
			System.out.println("numConns: " + numConns);

			final List<Future<BasicResult>> futures = new ArrayList<Future<BasicResult>>(); 
			
			final List<BasicWorker> workers = new ArrayList<BasicWorker>();
			
			for (int i=0; i<2; i++) {   // Note 4 threads .. which is more than the no of available conns .. hence we should see timeouts
				BasicWorker worker = new BasicWorker();
				workers.add(worker);
				futures.add(threadPool.submit(worker));
			}
			
			Thread.sleep(500);
			
			pool.markAsDown(new FatalConnectionException("mark pool as down"));
			
			Thread.sleep(200);
			
			stop.set(true);
			
			BasicResult result = new BasicResult();
			for (Future<BasicResult> f : futures) {
				result.addResult(f.get());
			}
			
			System.out.println(result.toString());
			
			pool.shutdown();
			
			System.out.println("\n\nConns borrowed: " + cpMonitor.getConnectionBorrowedCount());
			System.out.println("Conns returned: " + cpMonitor.getConnectionReturnedCount());
			System.out.println("Conns created: " + cpMonitor.getConnectionCreatedCount());
			System.out.println("Conns closed: " + cpMonitor.getConnectionClosedCount());
			System.out.println("Conns create failed: " + cpMonitor.getConnectionCreateFailedCount());

		}
		
		private class BasicWorker implements Callable<BasicResult> {

			private final BasicResult result = new BasicResult();
			
			private int sleepMs = 10;
			
			private BasicWorker() {
				
			}
			
			private BasicWorker(int sleep) {
				this.sleepMs = sleep;
			}
			
			@Override
			public BasicResult call() throws Exception {
				
				while (!stop.get() && !Thread.currentThread().isInterrupted()) {

					Connection<TestClient> connection = null;
					try {
						Thread.sleep(sleepMs);
						connection = pool.borrowConnection(20, TimeUnit.MILLISECONDS);
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
				
				return result;
			}
		}
		
		private class WorkerThatCanFailOperations implements Callable<BasicResult> {

			private final BasicResult result = new BasicResult();
			private final AtomicBoolean failOperation = new AtomicBoolean(false);
			
			private int sleepMs = 10;
			
			private WorkerThatCanFailOperations() {
				
			}
			
			
			@Override
			public BasicResult call() throws Exception {
				
				while (!stop.get() && !Thread.currentThread().isInterrupted()) {

					try {
						Connection<TestClient> connection = pool.borrowConnection(20, TimeUnit.MILLISECONDS);
						Thread.sleep(sleepMs);
						
						if (failOperation.get()) {
							((TestConnection)connection).setException(new FatalConnectionException("fail connection"));
							failOperation.set(false); // reset the latch
						}
						pool.returnConnection(connection);
						result.successCount.incrementAndGet();
					} catch (InterruptedException e) {
					} catch (DynoException e) {
						result.failureCount.incrementAndGet();
					} finally {
						result.opCount.incrementAndGet();
					}
				}
				
				return result;
			}
		}
		
		private class BasicResult { 
			
			private AtomicInteger opCount = new AtomicInteger(0);
			private AtomicInteger successCount = new AtomicInteger(0);
			private AtomicInteger failureCount = new AtomicInteger(0);
			
			private AtomicBoolean lastSuccess = new AtomicBoolean(false);
			
			private void addResult(BasicResult other) {
				opCount.addAndGet(other.opCount.get());
				successCount.addAndGet(other.successCount.get());
				failureCount.addAndGet(other.failureCount.get());
				boolean success = lastSuccess.get() && other.lastSuccess.get();
				lastSuccess.set(success);
			}

			@Override
			public String toString() {
				return "BasicResult [opCount=" + opCount + ", successCount=" + successCount + 
						", failureCount=" + failureCount + ", lastSuccess=" + lastSuccess.get() + "]";
			}
			
			
		}
		
		
		
	}
	

	
}
