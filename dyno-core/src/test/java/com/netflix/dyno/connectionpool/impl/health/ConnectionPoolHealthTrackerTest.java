package com.netflix.dyno.connectionpool.impl.health;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.BasicConfigurator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.FatalConnectionException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;

public class ConnectionPoolHealthTrackerTest {

	private static ScheduledExecutorService threadPool;

	@BeforeClass
	public static void beforeClass() {
		BasicConfigurator.configure();
		threadPool = Executors.newScheduledThreadPool(1);
	}

	@AfterClass
	public static void afterClass() {
		BasicConfigurator.resetConfiguration();
		threadPool.shutdownNow();
	}

	@Test
	public void testConnectionPoolRecycle() throws Exception {

		ConnectionPoolConfiguration config = new ConnectionPoolConfigurationImpl("test");
		ConnectionPoolHealthTracker<Integer> tracker = new ConnectionPoolHealthTracker<Integer>(config, threadPool, 1000, -1);
		tracker.start();

		Host h1 = new Host("h1", Status.Up);
		AtomicBoolean poolStatus = new AtomicBoolean(false); 
		HostConnectionPool<Integer> hostPool = getMockConnectionPool(h1, poolStatus);

		FatalConnectionException e = new FatalConnectionException("fatal");

		for (int i=0; i<10; i++)  {
			tracker.trackConnectionError(hostPool, e);
		}
		Thread.sleep(2000);

		tracker.stop();

		verify(hostPool, atLeastOnce()).markAsDown(any(DynoException.class));
		verify(hostPool, times(1)).reconnect();

		Assert.assertTrue("Pool active? : " + hostPool.isActive(), hostPool.isActive());
		// Check that the reconnecting pool is no longer being tracked by the tracker
		Assert.assertNull(tracker.getReconnectingPools().get(h1));
	}

	@Test
	public void testBadConnectionPoolKeepsReconnecting() throws Exception {

		ConnectionPoolConfiguration config = new ConnectionPoolConfigurationImpl("test");
		ConnectionPoolHealthTracker<Integer> tracker = new ConnectionPoolHealthTracker<Integer>(config, threadPool, 1000, -1);
		tracker.start();

		Host h1 = new Host("h1", Status.Up);
		AtomicBoolean poolStatus = new AtomicBoolean(false); 
		HostConnectionPool<Integer> hostPool = getMockConnectionPool(h1, poolStatus, true);

		FatalConnectionException e = new FatalConnectionException("fatal");

		for (int i=0; i<10; i++)  {
			tracker.trackConnectionError(hostPool, e);
		}
		Thread.sleep(2000);

		tracker.stop();

		verify(hostPool, atLeastOnce()).markAsDown(any(DynoException.class));
		verify(hostPool, atLeastOnce()).reconnect();

		Assert.assertFalse("Pool active? : " + hostPool.isActive(), hostPool.isActive());
		// Check that the reconnecting pool is no still being tracked by the tracker
		Assert.assertNotNull(tracker.getReconnectingPools().get(h1));
	}

	private HostConnectionPool<Integer> getMockConnectionPool(final Host host, final AtomicBoolean active) {
		return getMockConnectionPool(host, active, false);
	}

	private HostConnectionPool<Integer> getMockConnectionPool(final Host host, final AtomicBoolean active, final Boolean badConnectionPool) {

		@SuppressWarnings("unchecked")
		HostConnectionPool<Integer> hostPool = mock(HostConnectionPool.class);
		when(hostPool.getHost()).thenReturn(host);

		doAnswer(new Answer<Boolean>() {

			@Override
			public Boolean answer(InvocationOnMock invocation) throws Throwable {
				return active.get();
			}
		}).when(hostPool).isActive();

		doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				active.set(false);
				return null;
			}
		}).when(hostPool).markAsDown(any(DynoException.class));

		doAnswer(new Answer<Void>() {

			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				if (badConnectionPool) {
					throw new RuntimeException();
				} else {
					active.set(true);
					return null;
				}
			}
		}).when(hostPool).reconnect();

		return hostPool;
	}
}

