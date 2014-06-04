package com.netflix.dyno.connectionpool.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.AsyncOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionFactory;
import com.netflix.dyno.connectionpool.ConnectionObservor;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.OperationMonitor;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.FatalConnectionException;
import com.netflix.dyno.connectionpool.exception.ThrottledException;

public class SimpleAsyncConnectionPoolImpl<CL> implements HostConnectionPool<CL> {

	private static final Logger Logger = LoggerFactory.getLogger(SimpleAsyncConnectionPoolImpl.class);
	
	private final Host host;
	private final ConnectionFactory<CL> connFactory;
	private final ConnectionPoolConfiguration cpConfig;
	private final ConnectionPoolMonitor cpMonitor;
	
	// state to track the connections being used
	private final CircularList<Connection<CL>> rrSelector = new CircularList<Connection<CL>>(new ArrayList<Connection<CL>>());
	private final ConcurrentHashMap<Connection<CL>, Connection<CL>> connMap = new ConcurrentHashMap<Connection<CL>, Connection<CL>>();

	// Tracking state of host connection pool.
	private final AtomicBoolean shutdown = new AtomicBoolean(true);
	private final AtomicBoolean reconnecting = new AtomicBoolean(false);
	
	public SimpleAsyncConnectionPoolImpl(Host host, ConnectionFactory<CL> cFactory, 
										 ConnectionPoolConfiguration config, ConnectionPoolMonitor monitor) {
		
		this.host = host;
		this.connFactory = cFactory;
		this.cpConfig = config;
		this.cpMonitor = monitor;
	}
	
	@Override
	public Connection<CL> borrowConnection(int duration, TimeUnit unit) throws DynoException {
		
		if (shutdown.get()) {
			throw new DynoConnectException("Cannot connect to pool when pool is shutdown for host: " + host);
		}
		if (reconnecting.get()) {
			throw new DynoConnectException("Cannot connect to pool when pool is reconnecting for host: " + host);
		}

		long start = System.currentTimeMillis();
		Connection<CL> connection = rrSelector.getNextElement();
		if (connection == null) {
			throw new DynoConnectException("Cannot find connection for host: " + host);
		}
		cpMonitor.incConnectionBorrowed(host, System.currentTimeMillis() - start);
		return connection;
	}

	@Override
	public boolean returnConnection(Connection<CL> connection) {
		try {
			DynoConnectException e = connection.getLastException();
			
			if (shutdown.get() || reconnecting.get()) {
				// Just close the connection
				return closeConnection(connection);
				
			} else if (e != null && e instanceof FatalConnectionException) {
				
				// create another connection and then close this one
//				Connection<CL> connToRemove = connMap.remove(connection);
//				if (connToRemove == null) {
//					// This only happens when someone else already beat us to it
//					return false;
//				}
				recycleConnection(connection);
				return true;
				
			} else {
				// do nothing here
				return false;
			}
		} finally { 
			cpMonitor.incConnectionReturned(host);
		}
	}
	
	@Override
	public boolean closeConnection(Connection<CL> connection) {
		try  {
			connection.close();
			rrSelector.removeElement(connection);
			connMap.remove(connection);
			return true;
		} catch (Exception e) {
			Logger.error("Failed to close connection for host: " + host, e);
			return false;
		} finally {
			cpMonitor.incConnectionClosed(host, connection.getLastException());
		}
	}


	@Override
	public void markAsDown(DynoException reason) {
		
		if (shutdown.get()) {
			if (Logger.isDebugEnabled()) {
				Logger.debug("CP is not active, hence ignoring mark as down request");
			}
			return;
		}
		
		if (!(shutdown.compareAndSet(false, true))) {
			// someone already beat us to it
			Logger.info("Someone already beat us to marking the host as down, ignoring request");
			return;
		}
		
		// Call the shutdown code to start closing all the connections
		Logger.info("Marking host connection pool as DOWN for host: " + host + ", will recycle all connections");
		cpMonitor.hostDown(host, reason);
		
		List<Connection<CL>> connsToRecycle = new ArrayList<Connection<CL>>(connMap.keySet());
		
		for (Connection<CL> connection : connsToRecycle) {
			recycleConnection(connection);
		}

		if (!shutdown.compareAndSet(true, false)) {
			throw new IllegalStateException();
		}
	}

	@Override
	public void shutdown() {
		
		Logger.info("Shutting down connection pool for host:" + host);
		shutdown.set(true);
		
		for (Connection<CL> connection : connMap.keySet()) {
			closeConnection(connection);
		}
		
		connMap.clear();
	}

	@Override
	public int primeConnections() throws DynoException {

		Logger.info("Priming connection pool for host:" + host);

		if(!shutdown.get()) {
			throw new DynoException("Connection pool has already been inited, cannot prime connections for host:" + host);
		}
		
		int n = reconnect();
		shutdown.compareAndSet(true, false);
		
		return n;
	}

	@Override
	public OperationMonitor getOperationMonitor() {
		return null;
	}

	private int reconnect() throws DynoException {
		
		if (reconnecting.get()) {
			Logger.info("Reconnect in progress, ignoring reconnect connections request");
			return 0;
		}
		
		if (!(reconnecting.compareAndSet(false, true))) {
			Logger.info("Reconnect connections already called by someone else, ignoring reconnect connections request");
			return 0;
		}
		
		int successfullyCreated = 0; 
		
		for (int i=0; i<cpConfig.getMaxConnsPerHost(); i++) {
			try { 
				createConnection();
				successfullyCreated++;
				
			} catch (DynoConnectException e) {
				Logger.error("Failed to create connection", e);
				cpMonitor.incConnectionCreateFailed(host, e);
				throw e;
			}
		}
		
		if (!(reconnecting.compareAndSet(true, false))) {
			throw new IllegalStateException("something went wrong with prime connections");
		} else {
			return successfullyCreated;
		}
	}


	private Connection<CL> createConnection() throws DynoException {
		
		Connection<CL> connection = connFactory.createConnection((HostConnectionPool<CL>) this, null);
		connMap.put(connection, connection);
		rrSelector.addElement(connection);

		cpMonitor.incConnectionCreated(host);
		return connection;
	}
	
	private final AtomicInteger reviveCount = new AtomicInteger(0);
	private final AtomicInteger rrCount = new AtomicInteger(0);
	
	private void recycleConnection(final Connection<CL> connection) {
		
		reviveCount.incrementAndGet();
		
		Future<Connection<?>> conn = 
				ConnectionRecycler.getInstance().submitForRecycle((Connection<?>)connection, 

				new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					closeConnection(connection);
					return null;
				}
		}, 
			new Callable<Connection<?>>() {

			@Override
			public Connection<?> call() throws Exception {
				rrCount.incrementAndGet();
				return createConnection();
			}

		});

		try {
			conn.get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		
//		recoveryThreadPool.submit(new Callable<Void>() {
//
//			@Override
//			public Void call() throws Exception {
//				
//				if (shutdown.get()) {
//					// Do not create the connection
//					return null;
//				}
//				
//				rrCount.incrementAndGet();
//				Connection<CL> connection = createConnection();
//
//				// Check again if the pool has been shutdown
//				if (shutdown.get()) {
//					// Do not create the connection
//					System.out.println("CLOSING");
//					closeConnection(connection);
//				}
//				return null;
//			}
//		});
	}
	
	@Override
	public Host getHost() {
		return host;
	}

	@Override
	public boolean isActive() {
		return !shutdown.get();
	}

	@Override
	public boolean isShutdown() {
		return shutdown.get();
	}

	
	
	
	public static class UnitTest { 
		
		private static final Host TestHost = new Host("TestHost", 1234);
		
		// TEST UTILS SETUP
		private class TestClient {
			
		}
		
		private static AtomicBoolean stop = new AtomicBoolean(false);
		private static SimpleAsyncConnectionPoolImpl<TestClient> pool;
		private static ExecutorService threadPool;
		
		private static class TestConnection implements Connection<TestClient> {

			private final String id = UUID.randomUUID().toString();
			
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

			@Override
			public <R> Future<OperationResult<R>> executeAsync(AsyncOperation<TestClient, R> op) throws DynoException {
				throw new RuntimeException("Not Implemented");
			}

			@Override
			public int hashCode() {
				final int prime = 31;
				int result = 1;
				result = prime * result + ((id == null) ? 0 : id.hashCode());
				return result;
			}

			@Override
			public boolean equals(Object obj) {
				if (this == obj) return true;
				if (obj == null) return false;
				if (getClass() != obj.getClass()) return false;
				TestConnection other = (TestConnection) obj;
				return id != null ? id.equals(other.id) : other.id == null;
			}
		}
		
		private static ConnectionFactory<TestClient> connFactory = new ConnectionFactory<TestClient>() {

			@Override
			public Connection<TestClient> createConnection(HostConnectionPool<TestClient> pool, ConnectionObservor cObservor) throws DynoConnectException, ThrottledException {
				return new TestConnection();
			}
			
		};
		private static ConnectionPoolConfigurationImpl config = new ConnectionPoolConfigurationImpl("TestClient");
		private static CountingConnectionPoolMonitor cpMonitor = new CountingConnectionPoolMonitor();
		
		@BeforeClass
		public static void beforeClass() {
			threadPool = Executors.newFixedThreadPool(10);
			ConnectionRecycler.getInstance().start();
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
			ConnectionRecycler.getInstance().stop();
		}
		
		//@Test
		public void testRegularProcess() throws Exception {
			
			pool = new SimpleAsyncConnectionPoolImpl<TestClient>(TestHost, connFactory, config, cpMonitor);
			int numConns = pool.primeConnections();
			System.out.println("numConns: " + numConns);

			List<Future<BasicResult>> futures = new ArrayList<Future<BasicResult>>(); 
			
			for (int i=0; i<3; i++) {
				futures.add(threadPool.submit(new BasicWorker()));
			}
			
			Thread.sleep(300);
			
			stop.set(true);
			
			BasicResult total = new BasicResult();
			for (Future<BasicResult> f : futures) {
				total.addResult(f.get());
			}
			
			System.out.println(total);
			
			pool.shutdown();
			
			System.out.println("Conns borrowed: " + cpMonitor.getConnectionBorrowedCount());
			System.out.println("Conns returned: " + cpMonitor.getConnectionReturnedCount());
			System.out.println("Conns created: " + cpMonitor.getConnectionCreatedCount());
			System.out.println("Conns closed: " + cpMonitor.getConnectionClosedCount());
			System.out.println("Conns create failed: " + cpMonitor.getConnectionCreateFailedCount());
			
			System.out.println("Op success: " + cpMonitor.getOperationSuccessCount());
			System.out.println("Op failure: " + cpMonitor.getOperationFailureCount());
			System.out.println("Op timeout: " + cpMonitor.getOperationTimeoutCount());
		}
		
		
		//@Test
		public void testCloseBadConnections() throws Exception {
			
			pool = new SimpleAsyncConnectionPoolImpl<TestClient>(TestHost, connFactory, config, cpMonitor);
			int numConns = pool.primeConnections();
			System.out.println("numConns: " + numConns);

			final List<Future<BasicResult>> futures = new ArrayList<Future<BasicResult>>(); 
			
			for (int i=0; i<2; i++) {   
				BasicWorker worker = new BasicWorker();
				futures.add(threadPool.submit(worker));
			}

			Thread.sleep(300);
			
			final AtomicBoolean stopFailingConnections = new AtomicBoolean(false);
			
			final WorkerThatCanFailOperations badWorker = new WorkerThatCanFailOperations();
			futures.add(threadPool.submit(badWorker));
			
			Future<Integer> failCount = threadPool.submit(new Callable<Integer>() {

				@Override
				public Integer call() throws Exception {
					Integer count = 0;
					while (!stopFailingConnections.get() && !Thread.currentThread().isInterrupted()) {
						badWorker.failOperation.set(true);
						count++;
						Thread.sleep(50);
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
			
			System.out.println("Reviving single connection " + pool.reviveCount.get() + " " + pool.rrCount.get());

			System.out.println("\n\nConns borrowed: " + cpMonitor.getConnectionBorrowedCount());
			System.out.println("Conns returned: " + cpMonitor.getConnectionReturnedCount());
			System.out.println("Conns created: " + cpMonitor.getConnectionCreatedCount());
			System.out.println("Conns closed: " + cpMonitor.getConnectionClosedCount());
			System.out.println("Conns create failed: " + cpMonitor.getConnectionCreateFailedCount());
			
		}
		
		@Test
		public void testMarkHostAsDown() throws Exception {
			
			pool = new SimpleAsyncConnectionPoolImpl<TestClient>(TestHost, connFactory, config, cpMonitor);
			int numConns = pool.primeConnections();
			System.out.println("numConns: " + numConns);

			final List<Future<BasicResult>> futures = new ArrayList<Future<BasicResult>>(); 
			
			
			for (int i=0; i<2; i++) {   // Note 4 threads .. which is more than the no of available conns .. hence we should see timeouts
				BasicWorker worker = new BasicWorker();
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
						System.out.println(e.getMessage());
						result.failureCount.incrementAndGet();
						result.lastSuccess.set(false);
						if (connection != null) {
							((TestConnection)connection).setException(e);
						}
					} finally {
						result.opCount.incrementAndGet();
					}
				}
				
				System.out.println("Worker stopping: " + result);
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
							//System.out.println("Worker failing conn");
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
			
			private AtomicBoolean lastSuccess = new AtomicBoolean(true);
			
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
