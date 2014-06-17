package com.netflix.dyno.memcache;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import net.spy.memcached.MemcachedClient;

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
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.impl.lb.CircularList;

/**
 * Simpler implementation of {@link HostConnectionPool} for MemcachedConnecitons. 
 * The class maintains a circular list of Memcached {@link Connection}s underneath and just round robins traffic over them.
 * 
 * Each connection is in the circular list is responsible for the entire list of hosts supplied to it. That's how the spy Memcached client
 * works, hence the {@link Connection} object wrapping the {@link MemcachedClient} can handle requests for all hosts backed by that client. 
 * 
 * We have a circular list of these in case we need more than one dedicated connection to each {@link Host}
 * The no of connections in the circular list is determined by {@link ConnectionPoolConfiguration#getMaxConnsPerHost()}
 * 
 * Another important note:  
 *    The connections managed underneath are assumed to be async. Hence this class does not have to ensure thread safe access to any of the 
 *    connections, since we can just share the async connections for multiple requests.
 *     
 *    This greatly simplifies the design of this class.
 *     
 *    All the pool does is round robin over the available connections so that there is a fair distribution of load on the multiple connections
 *    in the circular list.
 *    
 * 
 * The class still makes use of generics which makes testing easier. 
 * This also allows the entire functionality to be reused for another similar connection which multiplexes and de-multiplexes 
 * requests for a list of hosts underneath. 
 *  
 * @author poberai
 *
 * @param <CL>
 */
public class MemcachedConnectionPool<CL> implements HostConnectionPool<CL> {

	private static final Logger Logger = LoggerFactory.getLogger(MemcachedConnectionPool.class);
	
	// The circular list tracking the individual connections. Size = config.getMaxConnsPerHost()
	private final CircularList<Connection<CL>> mClients = new CircularList<Connection<CL>>(null);
	
	// The connection factory for vending config.getMaxConnsPerHost() number of connections
	private final ConnectionFactory<CL> connFactory;
	// The config for this pool
	private final ConnectionPoolConfiguration cpConfig;
	// The host group that is associated for this connection pool. Note that the host group encapsulates a list of hosts
	// see {@link Host}
	private final Host host;
	// A connection observor for tracking when connections appear and disappear
	private final ConnectionObservor connObservor;
	// Monitor for tracking stats
	private final ConnectionPoolMonitor cpMonitor;
	// Operation monitor for tracking operation level counters and latencies
	private final OperationMonitor operationMonitor;
	
	// Control variable for turning the pool on and off. Note that connections cannot be borrowed when the pool is shutdown
	private final AtomicBoolean shutdown = new AtomicBoolean(true);
	
	/**
	 * Constuctor 
	 * 
	 * @param host
	 * @param config 
	 * @param monitor
	 * @param connFactory
	 * @param observor
	 * @param opMonitor
	 */
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
	
	/**
	 * Borrow a connection from the pool. 
	 * This will consult the circular list and hence round robin on the available connections. 
	 * No of connections = config.getMaxConnsPerHost()
	 * 
	 * Note that unlike other connection pool implementations, we do NOT need to guarantee thread safe
	 * access to the connection, since the underlying connections are assumed  to be async in nature. 
	 * 
	 * @return Connection<CL> 
	 */
	@Override
	public Connection<CL> borrowConnection(int duration, TimeUnit unit) throws DynoException {

		try { 
			if (shutdown.get()) {
				throw new DynoConnectException("Cannot borrow connection from pool when it is shutdown");
			}
			cpMonitor.incConnectionBorrowed(host, 0);
			return mClients.getNextElement();
		} finally {
			
		}
	}

	/**
	 * Nothing to do here since the underlying connections are async in nature. 
	 * Just records the stats
	 * @return true/false indicating whether the connection was closed
	 */
	@Override
	public boolean returnConnection(Connection<CL> connection) {
		cpMonitor.incConnectionReturned(host);
		return false;
	}

	/**
	 * Simple calls connection.close() and records stats
	 */
	@Override
	public boolean closeConnection(Connection<CL> connection) {
		try { 
			connection.close();
		} finally {
			cpMonitor.incConnectionClosed(host, null);
		}
		return true;
	}

	/**
	 * Causes the connection pool to shutdown
	 */
	@Override
	public void markAsDown(DynoException reason) {
		shutdown();
	}

	@Override
	public void reconnect() {
		shutdown();
		primeConnections();
	}

	/**
	 * Shuts down the connection pool. Note that calling this method causes all the connections to be immediately closed. 
	 * Hence this will cancel any in flight requests. 
	 * 
	 * See {@link RollingMemcachedConnectionPoolImpl#updateHosts(java.util.Collection, java.util.Collection)} for details 
	 * on how it manages another active {@link MemcachedConnectionPool} when calling shutdown on this pool 
	 * and allows in flight requests on this pool to complete with a sufficient grace period configured using 
	 * {@link ConnectionPoolConfiguration#getPoolShutdownDelay()}. 
	 * 
	 */
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

		Logger.info("Shutting down connection pool");
		
		List<Connection<CL>> connections = mClients.getEntireList();
		
		if (connections != null) {
			for (Connection<CL> connection : connections) {
				closeConnection(connection);
			}
		}
	}

	/**
	 * Simple method for creation new connections as dictated by {@link ConnectionPoolConfiguration#getMaxConnsPerHost()}
	 * 
	 * @return int - how many connections were opened
	 */
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

	/**
	 * @return {@link Host} 
	 */
	@Override
	public Host getHost() {
		return host;
	}

	/**
	 * @return true/false
	 */
	@Override
	public boolean isActive() {
		return !shutdown.get();
	}

	/**
	 * @return true/false
	 */
	@Override
	public boolean isShutdown() {
		return shutdown.get();
	}

	/**
	 * @return {@link OperationMonitor}
	 */
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
			assertTrue("" + numConns, numConns == config.getMaxConnsPerHost());
			
			verify(mockFactory, times(config.getMaxConnsPerHost())).createConnection(pool, null);
			
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
			assertTrue("" + numConns, numConns == config.getMaxConnsPerHost());

			verify(mockFactory, times(config.getMaxConnsPerHost())).createConnection(pool, null);
			
			pool.shutdown();
			verify(mockConnection, times(config.getMaxConnsPerHost())).close();
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
			assertTrue("" + numConns, numConns == config.getMaxConnsPerHost());

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

			pool.shutdown();

			assertTrue("TotalOps: " + totalOps, totalOps == cpMonitor.getConnectionBorrowedCount());
			assertTrue("TotalOps: " + totalOps, totalOps == cpMonitor.getConnectionReturnedCount());
			assertTrue("" + cpMonitor.getConnectionCreatedCount(), cpMonitor.getConnectionCreatedCount() == config.getMaxConnsPerHost());
			assertTrue("" + cpMonitor.getConnectionClosedCount(), cpMonitor.getConnectionClosedCount() == config.getMaxConnsPerHost());
			assertTrue("" + cpMonitor.getConnectionCreateFailedCount(), cpMonitor.getConnectionCreateFailedCount() == 0);
		}
	}

	@Override
	public Collection<Connection<CL>> getAllConnections() {
		return mClients.getEntireList();
	}
}
