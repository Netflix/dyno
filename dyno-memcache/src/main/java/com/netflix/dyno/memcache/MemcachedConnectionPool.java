package com.netflix.dyno.memcache;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
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

import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionFactory;
import com.netflix.dyno.connectionpool.ConnectionObservor;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.OperationMonitor;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.impl.CircularList;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.CountingConnectionPoolMonitor;

public class MemcachedConnectionPool<CL> implements HostConnectionPool<CL> {

	private static final Logger Logger = LoggerFactory.getLogger(MemcachedConnectionPool.class);
	
	private final CircularList<Connection<CL>> mClients = new CircularList<Connection<CL>>(null);
	
	private final ConnectionFactory<CL> connFactory;
	private final ConnectionPoolConfiguration cpConfig; 
	private final ConnectionPoolMonitor cpMonitor;
	private final Host host;
	private final ConnectionObservor connObservor;
	private final OperationMonitor operationMonitor;
	
	private final AtomicBoolean shutdown = new AtomicBoolean(true);
	
	public MemcachedConnectionPool(Host host, 
								   ConnectionPoolConfiguration config, ConnectionPoolMonitor monitor, 
								   ConnectionFactory<CL> connFactory, ConnectionObservor observor,
								   OperationMonitor opMonitor) {
		this.cpConfig = config; 
		this.cpMonitor = monitor;
		this.connFactory = connFactory;
		this.host = host;
		this.connObservor = observor;
		this.operationMonitor = opMonitor;
	}
	
	@Override
	public Connection<CL> borrowConnection(int duration, TimeUnit unit) throws DynoException {

		try { 
			if (shutdown.get()) {
				throw new DynoConnectException("Cannot borrow connection from pool when it is shutdown");
			}
			return mClients.getNextElement();
		} finally {
			
		}
	}

	@Override
	public boolean returnConnection(Connection<CL> connection) {
		cpMonitor.incConnectionReturned(host);
		return false;
	}

	@Override
	public boolean closeConnection(Connection<CL> connection) {
		try { 
			connection.close();
		} finally {
			cpMonitor.incConnectionClosed(host, null);
		}
		return true;
	}

	@Override
	public void markAsDown(DynoException reason) {
		shutdown();
	}

	private static AtomicInteger sCount = new AtomicInteger(0);
	@Override
	public void shutdown() {
		
		if (shutdown.get()) {
			Logger.info("Connection pool already shutdown, ignoring request");
			return;
		}
		
		if (!shutdown.compareAndSet(false, true)) {
			Logger.info("Someone already shut pool down, ignoring shutdown request");
			return; 
		}

		int sc = sCount.incrementAndGet();
		Logger.info("Shutting down connection pool");
		System.out.println("Shutting down connection pool " + this.getHost().getHostName() + " " + sc + " " + Thread.currentThread().getName());
		
		List<Connection<CL>> connections = mClients.getEntireList();
		
		if (connections != null) {
			for (Connection<CL> connection : connections) {
				closeConnection(connection);
			}
		}
	}

	@Override
	public int primeConnections() throws DynoException {
		
		if (!shutdown.get()) {
			throw new DynoConnectException("Cannot prime connections when pool is not shut down");
		}
		
		if (!shutdown.compareAndSet(true, false)) {
			// Someone already beat us to it. Simple return
			return 0;
		}
		
		Logger.info("Priming connections: " + cpConfig.getMaxConnsPerHost());
		
		List<Connection<CL>> connections = new ArrayList<Connection<CL>>();

		int successCount = 0;
		
		for (int i=0; i<cpConfig.getMaxConnsPerHost(); i++) {
			try { 
				connections.add(connFactory.createConnection(this, connObservor));
				cpMonitor.incConnectionCreated(host);
				successCount++;
			} catch (DynoException e) {
				cpMonitor.incConnectionCreateFailed(host, e);
			}
		}
		
		mClients.swapWithList(connections);
		
		return successCount;
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

	@Override
	public OperationMonitor getOperationMonitor() {
		return operationMonitor;
	}

	@SuppressWarnings("unchecked")
	public static class UnitTest { 
	
		// TEST UTILS SETUP
		private static final Host TestHost = new Host("TestHost", 1234);
		
		private class TestClient {
		}
		
		private static AtomicBoolean stop = new AtomicBoolean(false);
		private static ExecutorService threadPool;

		private static MemcachedConnectionPool<TestClient> pool;
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
		}

		@AfterClass
		public static void afterClass() {
			stop.set(true);
			threadPool.shutdownNow();
		}
		

		private class BasicResult { 
			
			private AtomicInteger opCount = new AtomicInteger(0);
			private AtomicInteger successCount = new AtomicInteger(0);
			private AtomicInteger failureCount = new AtomicInteger(0);
			private AtomicBoolean lastSuccess = new AtomicBoolean(false);

			@Override
			public String toString() {
				return "BasicResult [opCount=" + opCount + ", successCount=" + successCount + 
						", failureCount=" + failureCount + ", lastSuccess=" + lastSuccess.get() + "]";
			}
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
					} finally {
						result.opCount.incrementAndGet();
					}
				}
				
				return result;
			}
		}
	
		@Test
		public void testPrimeConnections() throws Exception {
				
			Connection<TestClient> mockConnection = mock(Connection.class);
			ConnectionFactory<TestClient> mockFactory = mock(ConnectionFactory.class);
			
			pool = new MemcachedConnectionPool<TestClient>(TestHost, config, cpMonitor, mockFactory, null, null);

			when(mockFactory.createConnection(pool, null)).thenReturn(mockConnection);

			int numConns = pool.primeConnections();
			System.out.println("numConns: " + numConns);

			verify(mockFactory, times(3)).createConnection(pool, null);
			
			Connection<TestClient> connection = pool.borrowConnection(1, TimeUnit.MILLISECONDS);
			assertTrue(mockConnection.equals(connection));
		}

		@Test
		public void testShutdown() throws Exception {
				
			Connection<TestClient> mockConnection = mock(Connection.class);
			ConnectionFactory<TestClient> mockFactory = mock(ConnectionFactory.class);
			
			pool = new MemcachedConnectionPool<TestClient>(TestHost, config, cpMonitor, mockFactory, null, null);

			when(mockFactory.createConnection(pool, null)).thenReturn(mockConnection);
			
			int numConns = pool.primeConnections();
			System.out.println("numConns: " + numConns);

			verify(mockFactory, times(3)).createConnection(pool, null);
			
			pool.shutdown();
			verify(mockConnection, times(3)).close();
		}

		@Test
		public void testConnectionClose() throws Exception {
				
			Connection<TestClient> mockConnection = mock(Connection.class);
			ConnectionFactory<TestClient> mockFactory = mock(ConnectionFactory.class);
			
			pool = new MemcachedConnectionPool<TestClient>(TestHost, config, cpMonitor, mockFactory, null, null);
			
			when(mockFactory.createConnection(pool, null)).thenReturn(mockConnection);
			
			pool.primeConnections();
			
			Connection<TestClient> connection = pool.borrowConnection(1, TimeUnit.MILLISECONDS);
			pool.closeConnection(connection);
			
			verify(mockConnection, times(1)).close();
		}
		
		@Test
		public void testRegularProcess() throws Exception {

			
			Connection<TestClient> mockConnection = mock(Connection.class);
			ConnectionFactory<TestClient> mockFactory = mock(ConnectionFactory.class);
			
			pool = new MemcachedConnectionPool<TestClient>(TestHost, config, cpMonitor, mockFactory, null, null);
			
			when(mockFactory.createConnection(pool, null)).thenReturn(mockConnection);

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
	}
}
