package com.netflix.dyno.connectionpool.impl;

import java.util.concurrent.Callable;
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

import com.netflix.dyno.connectionpool.AsyncOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.ConnectionFactory;
import com.netflix.dyno.connectionpool.ConnectionObservor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.ListenableFuture;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.FatalConnectionException;
import com.netflix.dyno.connectionpool.exception.ThrottledException;

public class HostConnectionPoolImplTest {

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
		public <R> ListenableFuture<OperationResult<R>> executeAsync(AsyncOperation<TestClient, R> op) throws DynoException {
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
					connection = pool.borrowConnection(100, TimeUnit.MILLISECONDS);
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
