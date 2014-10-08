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
package com.netflix.dyno.connectionpool.impl.health;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.BasicConfigurator;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.FatalConnectionException;
import com.netflix.dyno.connectionpool.exception.TimeoutException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;

/**
 * This class tracks the error rates for any {@link HostConnectionPool} via the {@link ErrorRateMonitor}
 * The error rates are recorded directly by the class but the error rates are checked asynchronously in another thread. 
 * Once the thread decides that the error rates have crossed a configured threshold, then the {@link HostConnectionPool} is recycled.
 * i.e  it is first marked as DOWN to prevent any new connections from being borrowed from it. Then the pool is reconnect()'d
 * 
 * Hence callers to {@link HostConnectionPool} should take it's isActive() state into account when using this class. 
 * i.e before borrowing a connection check for isActive(). If not active, then use a fallback pool else throw an ex to the caller. 
 * Resume executing operations against the pool only once the pool becomes active. 
 * 
 * @author poberai
 *
 * @param <CL>
 */
public class ConnectionPoolHealthTracker<CL> {
	
	private static final Logger Logger = LoggerFactory.getLogger(ConnectionPoolHealthTracker.class);
	
	private final ConnectionPoolConfiguration cpConfiguration;
	private final ScheduledExecutorService threadPool;
	private final AtomicBoolean stop = new AtomicBoolean(false);
	private final ConcurrentHashMap<Host, ErrorMonitor> errorRates = new ConcurrentHashMap<Host, ErrorMonitor>();
	private final ConcurrentHashMap<Host, HostConnectionPool<CL>> reconnectingPools = new ConcurrentHashMap<Host, HostConnectionPool<CL>>();
	private final ConcurrentHashMap<Host, HostConnectionPool<CL>> pingingPools = new ConcurrentHashMap<Host, HostConnectionPool<CL>>();

	private final AtomicBoolean startedPing = new AtomicBoolean(false);
	
	private static final Integer DEFAULT_SLEEP_MILLIS = 10*1000; 
	private static final Integer DEFAULT_POOL_RECONNECT_WAIT_MILLIS = 5*1000; 
	private final Integer SleepMillis; 
	private final Integer PoolReconnectWaitMillis; 
	
	public ConnectionPoolHealthTracker(ConnectionPoolConfiguration config, ScheduledExecutorService thPool) {
		this(config, thPool, DEFAULT_SLEEP_MILLIS, DEFAULT_POOL_RECONNECT_WAIT_MILLIS);
	}
		
	public ConnectionPoolHealthTracker(ConnectionPoolConfiguration config, ScheduledExecutorService thPool, int sleepMillis, int poolReconnectWaitMillis) {
		cpConfiguration = config;	
		threadPool = thPool;
		SleepMillis = sleepMillis;
		PoolReconnectWaitMillis = poolReconnectWaitMillis;
	}


	public void removeHost(Host host) {
		HostConnectionPool<CL> destPool = reconnectingPools.get(host);
		if (destPool != null) {
			Logger.info("Health tracker marking host as down " + host);
			destPool.getHost().setStatus(Status.Down);
		}
	}

	public void start() {
		
		threadPool.scheduleWithFixedDelay(new Runnable() {

			@Override
			public void run() {
				
				if(stop.get() || Thread.currentThread().isInterrupted()) {
					return;
				}
				
				Logger.debug("Running, pending pools size: " + reconnectingPools.size());
					
				for (Host host : reconnectingPools.keySet()) {
						
					if (!host.isUp()) {
						Logger.info("Host: " + host + " is marked as down, evicting host from reconnection pool");
						reconnectingPools.remove(host);
						continue;
					}
						
					HostConnectionPool<CL> pool = reconnectingPools.get(host);
					Logger.info("Checking for reconnecting pool for host: " + host + ", pool active? " + pool.isActive());
					if (pool.isActive()) {
						// Pool is already active. Move on
						reconnectingPools.remove(host);
					} else {
						try {
							Logger.info("Reconnecting pool : " + pool);
							pool.markAsDown(null);
							if (PoolReconnectWaitMillis > 0) {
								Logger.debug("Sleeping to allow enough time to drain connections");
								Thread.sleep(PoolReconnectWaitMillis);
							}
							pool.reconnect();
							if (pool.isActive()) {
								Logger.info("Host pool reactivated: " + host);
								reconnectingPools.remove(host);
							} else {
								Logger.info("Could not re-activate pool, will try again later");
							}
						} catch (Exception e) {
							// do nothing, will retry again once thread wakes up
							Logger.warn("Failed to reconnect pool for host: " + host + " " +  e.getMessage());
						}
					}
				}
			}
			
		}, 1000, SleepMillis, TimeUnit.MILLISECONDS);
	}
	
	public void stop() {
		stop.set(true);
	}
	
	public void trackConnectionError(HostConnectionPool<CL> hostPool, DynoException e) {
			
		if (e != null && e instanceof TimeoutException) {
			// don't track timeouts, since that may not be indicative of an actual n/w problem
			// that may just be a slowdown due to pool saturation of larger payloads
			return; 
		}
		
		if (e != null && e instanceof FatalConnectionException) {

			Logger.error("FAIL: " + e.getMessage());
			
			Host host = hostPool.getHost();
			
			ErrorMonitor errorMonitor = errorRates.get(host);

			if (errorMonitor == null) {
				
				errorMonitor = cpConfiguration.getErrorMonitorFactory().createErrorMonitor();
				errorRates.putIfAbsent(host, errorMonitor);
				errorMonitor = errorRates.get(host);
			}

			boolean errorRateOk = errorMonitor.trackError(1);

			if (!errorRateOk) {
				reconnectPool(hostPool);
			}
		}
	}
	
	public void reconnectPool(HostConnectionPool<CL> hostPool) {
		Host host = hostPool.getHost();
		Logger.error("Enqueueing host cp for recycling due to too many errors: " + hostPool);
		hostPool.markAsDown(null);
		reconnectingPools.put(host, hostPool);
	}
	
	public void initialPingHealthchecksForPool(HostConnectionPool<CL> hostPool) {
		
		pingingPools.putIfAbsent(hostPool.getHost(), hostPool);
		if (startedPing.get()) {
			return;
		}
		
		if (pingingPools.size() > 0) {
			if (startedPing.compareAndSet(false, true)) {
				
				threadPool.scheduleWithFixedDelay(new Runnable() {

					@Override
					public void run() {
						for (HostConnectionPool<CL> hostPool : pingingPools.values()) {
							pingHostPool(hostPool);
						}
					}
				}, 1, cpConfiguration.getPingFrequencySeconds(), TimeUnit.SECONDS);
				
			} else {
				return;
			}
		} else {
			return;  // no pools to ping
		}
	}

	private void pingHostPool(HostConnectionPool<CL> hostPool) {
		for (Connection<CL> connection : hostPool.getAllConnections()) {
			try { 
				connection.execPing();
			} catch (DynoException e) {
				trackConnectionError(hostPool, e);
			}
		}
	}

	
	public static class UnitTest {
	
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
			Assert.assertNull(tracker.reconnectingPools.get(h1));
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
			Assert.assertNotNull(tracker.reconnectingPools.get(h1));
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

}
