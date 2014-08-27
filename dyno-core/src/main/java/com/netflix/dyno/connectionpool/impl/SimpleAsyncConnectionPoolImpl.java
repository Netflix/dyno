package com.netflix.dyno.connectionpool.impl;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
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
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.FatalConnectionException;
import com.netflix.dyno.connectionpool.exception.ThrottledException;
import com.netflix.dyno.connectionpool.impl.lb.CircularList;

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
	private final AtomicBoolean active = new AtomicBoolean(false);
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
		
		if (!active.get()) {
			throw new DynoConnectException("Cannot connect to pool when pool is shutdown for host: " + host);
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
			if (!active.get()) {
				// Just close the connection
				return closeConnection(connection);
				
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
			Connection<CL> prevConnection = connMap.remove(connection);
			if (prevConnection != null) {
				connection.close();
				rrSelector.removeElement(connection);
				cpMonitor.incConnectionClosed(host, connection.getLastException());
			}
			return true;
		} catch (Exception e) {
			Logger.error("Failed to close connection for host: " + host, e);
			return false;
		} finally {
		}
	}


	@Override
	public void markAsDown(DynoException reason) {
		
		if (!active.get()) {
			return; // already marked as down
		}
		
		active.compareAndSet(true, false);
	}

	@Override
	public void reconnect() {
		
		if (active.get()) {
			Logger.info("Pool already active, ignoring reconnect connections request");
			return;
		}
		
		if (reconnecting.get()) {
			Logger.info("Pool already reconnecting, ignoring reconnect connections request");
			return;
		}
		
		if (!(reconnecting.compareAndSet(false, true))) {
			Logger.info("Pool already reconnecting, ignoring reconnect connections request");
			return;
		}
		
		try {
			shutdown();
			primeConnections();
		} finally {
			reconnecting.set(false);
		}
	}
	
	@Override
	public void shutdown() {
		
		Logger.info("Shutting down connection pool for host:" + host);
		active.set(false);
		
		for (Connection<CL> connection : connMap.keySet()) {
			closeConnection(connection);
		}
		
		connMap.clear();
	}

	@Override
	public int primeConnections() throws DynoException {

		Logger.info("Priming connection pool for host:" + host);

		if(active.get()) {
			throw new DynoException("Connection pool has already been inited, cannot prime connections for host:" + host);
		}
		
		int created = 0;
		for (int i=0; i<cpConfig.getMaxConnsPerHost(); i++) {
			try { 
				createConnection();
				created++;
			} catch (DynoConnectException e) {
				Logger.error("Failed to create connection", e);
				cpMonitor.incConnectionCreateFailed(host, e);
				throw e;
			}
		}
		active.compareAndSet(false, true);
		
		return created;
	}

	@Override
	public Collection<Connection<CL>> getAllConnections() {
		return connMap.keySet();
	}

	private Connection<CL> createConnection() throws DynoException {
		
		Connection<CL> connection = connFactory.createConnection((HostConnectionPool<CL>) this, null);
		connMap.put(connection, connection);
		connection.open();
		rrSelector.addElement(connection);

		cpMonitor.incConnectionCreated(host);
		return connection;
	}
	
	@Override
	public Host getHost() {
		return host;
	}

	@Override
	public boolean isActive() {
		return active.get();
	}

	@Override
	public boolean isShutdown() {
		return !active.get();
	}

	public static class UnitTest { 
		
		// TEST UTILS SETUP
		private static final Host TestHost = new Host("TestHost", 1234);
		
		private class TestClient {}
		
		private static SimpleAsyncConnectionPoolImpl<TestClient> pool;
		private static ExecutorService threadPool;
		
		private static ConnectionFactory<TestClient> connFactory = new ConnectionFactory<TestClient>() {

			@SuppressWarnings("unchecked")
			Connection<TestClient> connection = mock(Connection.class);
			@Override
			public Connection<TestClient> createConnection(HostConnectionPool<TestClient> pool, ConnectionObservor cObservor) throws DynoConnectException, ThrottledException {
				return connection;
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
			
			pool = new SimpleAsyncConnectionPoolImpl<TestClient>(TestHost, connFactory, config, cpMonitor);
			pool.primeConnections();

			int nThreads = 3; 
			final TestControl control = new TestControl(3);
			final BasicResult total = new BasicResult();
			
			for (int i=0; i<nThreads; i++) {
				threadPool.submit(new BasicWorker(total, control));
			}
			
			Thread.sleep(300);
			
			control.stop();
			control.waitOnFinish();
			
			Assert.assertEquals("Total: " + total, total.opCount.get(), total.successCount.get());
			Assert.assertEquals("Total: " + total, 0, total.failureCount.get());
			Assert.assertTrue("Total: " + total, total.lastSuccess.get());
			
			pool.shutdown();
			
			Assert.assertEquals("Conns borrowed: " + cpMonitor.getConnectionBorrowedCount(), total.successCount.get(), cpMonitor.getConnectionBorrowedCount());
			Assert.assertEquals("Conns returned: " + cpMonitor.getConnectionReturnedCount(), cpMonitor.getConnectionBorrowedCount(), cpMonitor.getConnectionReturnedCount());
			Assert.assertEquals("Conns created: " + cpMonitor.getConnectionCreatedCount(), config.getMaxConnsPerHost(), cpMonitor.getConnectionCreatedCount());
			Assert.assertEquals("Conns closed: " + cpMonitor.getConnectionClosedCount(), cpMonitor.getConnectionCreatedCount(), cpMonitor.getConnectionClosedCount());
			Assert.assertEquals("Conns create failed: " + cpMonitor.getConnectionCreateFailedCount(), 0, cpMonitor.getConnectionCreateFailedCount());
		}
		
		@Test
		public void testMarkHostAsDown() throws Exception {
			
			pool = new SimpleAsyncConnectionPoolImpl<TestClient>(TestHost, connFactory, config, cpMonitor);
			pool.primeConnections();

			int nThreads = 3; 
			final TestControl control = new TestControl(3);
			final BasicResult total = new BasicResult();
			
			for (int i=0; i<nThreads; i++) {
				threadPool.submit(new BasicWorker(total, control));
			}
			
			Thread.sleep(500);

			pool.markAsDown(new FatalConnectionException("mark pool as down"));
			
			Thread.sleep(200);
		
			control.stop();
			control.waitOnFinish();
		
			Assert.assertTrue("Total: " + total, total.failureCount.get() > 0);
			Assert.assertFalse("Total: " + total, total.lastSuccess.get());
			Assert.assertEquals("Total: " + total, total.opCount.get(),  (total.successCount.get() + total.failureCount.get()));
			
			pool.shutdown();
			
			Assert.assertEquals("Conns borrowed: " + cpMonitor.getConnectionBorrowedCount(), total.successCount.get(), cpMonitor.getConnectionBorrowedCount());
			Assert.assertEquals("Conns returned: " + cpMonitor.getConnectionReturnedCount(), cpMonitor.getConnectionBorrowedCount(), cpMonitor.getConnectionReturnedCount());
			Assert.assertEquals("Conns created: " + cpMonitor.getConnectionCreatedCount(), config.getMaxConnsPerHost(), cpMonitor.getConnectionCreatedCount());
			Assert.assertEquals("Conns closed: " + cpMonitor.getConnectionClosedCount(), cpMonitor.getConnectionCreatedCount(), cpMonitor.getConnectionClosedCount());
			Assert.assertEquals("Conns create failed: " + cpMonitor.getConnectionCreateFailedCount(), 0, cpMonitor.getConnectionCreateFailedCount());
		}
		
		@Test
		public void testReconnect() throws Exception {
			
			pool = new SimpleAsyncConnectionPoolImpl<TestClient>(TestHost, connFactory, config, cpMonitor);
			pool.primeConnections();
			
			int nThreads = 3; 
			TestControl control = new TestControl(3);
			BasicResult total = new BasicResult();
			
			for (int i=0; i<nThreads; i++) {
				threadPool.submit(new BasicWorker(total, control));
			}
			
			Thread.sleep(500);
			
			Assert.assertFalse("Total: " + total, total.failureCount.get() > 0);
			Assert.assertTrue("Total: " + total, total.lastSuccess.get());

			pool.markAsDown(new FatalConnectionException("mark pool as down"));
			
			Thread.sleep(200);
		
			control.stop();
			control.waitOnFinish();
		
			Assert.assertTrue("Total: " + total, total.failureCount.get() > 0);
			Assert.assertFalse("Total: " + total, total.lastSuccess.get());
			Assert.assertEquals("Total: " + total, total.opCount.get(),  (total.successCount.get() + total.failureCount.get()));

			pool.reconnect();
			Thread.sleep(100);
			
			control = new TestControl(3);
			total = new BasicResult();
			
			for (int i=0; i<nThreads; i++) {
				threadPool.submit(new BasicWorker(total, control));
			}

			Thread.sleep(500);
			
			control.stop();
			control.waitOnFinish();
			
			Assert.assertEquals("Total: " + total, total.opCount.get(), total.successCount.get());
			Assert.assertEquals("Total: " + total, 0, total.failureCount.get());
			Assert.assertTrue("Total: " + total, total.lastSuccess.get());

			pool.shutdown();
			
			Assert.assertEquals("Conns returned: " + cpMonitor.getConnectionReturnedCount(), cpMonitor.getConnectionBorrowedCount(), cpMonitor.getConnectionReturnedCount());
			Assert.assertEquals("Conns created: " + cpMonitor.getConnectionCreatedCount(), 2*config.getMaxConnsPerHost(), cpMonitor.getConnectionCreatedCount());
			Assert.assertEquals("Conns closed: " + cpMonitor.getConnectionClosedCount(), cpMonitor.getConnectionCreatedCount(), cpMonitor.getConnectionClosedCount());
			Assert.assertEquals("Conns create failed: " + cpMonitor.getConnectionCreateFailedCount(), 0, cpMonitor.getConnectionCreateFailedCount());
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
