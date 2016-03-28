package com.netflix.dyno.connectionpool.impl;

import static org.mockito.Mockito.mock;

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

import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionFactory;
import com.netflix.dyno.connectionpool.ConnectionObservor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.FatalConnectionException;
import com.netflix.dyno.connectionpool.exception.ThrottledException;

public class SimpleAsyncConnectionPoolImplTest {

	// TEST UTILS SETUP
	private static final Host TestHost = new Host("TestHost", 1234);

	private class TestClient {}

	private static SimpleAsyncConnectionPoolImpl<TestClient> pool;
	private static ExecutorService threadPool;

	private static ConnectionFactory<TestClient> connFactory = new ConnectionFactory<TestClient>() {
        @SuppressWarnings("unchecked")
        @Override
		public Connection<TestClient> createConnection(HostConnectionPool<TestClient> pool, ConnectionObservor cObservor) throws DynoConnectException, ThrottledException {
			return mock(Connection.class);
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
