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
package com.netflix.dyno.connectionpool.impl.lb;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.exception.PoolExhaustedException;
import com.netflix.dyno.connectionpool.exception.PoolOfflineException;
import com.netflix.dyno.connectionpool.exception.PoolTimeoutException;
import com.netflix.dyno.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.impl.HostSelectionStrategy;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Predicate;

/**
 * Class that implements the {@link HostSelectionStrategy} interface. 
 * It acts as a co-ordinator over multiple HostSelectionStrategy impls where each maps to a certain "DC" in the dynomite topology.
 * Hence this class doesn't actually implement the logic (e.g Round Robin or Token Aware) to actually borrow the connections. 
 * It relies on a local HostSelectionStrategy impl and a collection of remote HostSelectionStrategy(s) 
 * It gives preference to the "local" HostSelectionStrategy but if the local dc pool is offline or hosts are down etc, then it 
 * falls back to the remote HostSelectionStrategy. Also it uses pure round robin for distributing load on the fall back HostSelectionStrategy
 * impls for even distribution of load on the remote DCs in the event of an outage in the local dc. 
 * Note that this class does not prefer any one remote HostSelectionStrategy over the other.  
 *  
 * @author poberai
 *
 * @param <CL>
 */
public class HostSelectionWithFallback<CL> implements HostSelectionStrategy<CL> {

	private static final Logger Logger = LoggerFactory.getLogger(HostSelectionWithFallback.class);

	// tracks the local zone
	private final String localDC;
	// The selector for the local zone
	private final HostSelectionStrategy<CL> localSelector;
	// Track selectors for each remote DC
	private final ConcurrentHashMap<String, HostSelectionStrategy<CL>> remoteDCSelectors = new ConcurrentHashMap<String, HostSelectionStrategy<CL>>();
	// The map of all Host -> HostConnectionPool 
	private final ConcurrentHashMap<Host, HostConnectionPool<CL>> hostPools = new ConcurrentHashMap<Host, HostConnectionPool<CL>>();

	private final ConnectionPoolMonitor cpMonitor; 

	// list of names of remote zones. Used for RoundRobin over remote zones when local zone host is down
	private final CircularList<String> remoteDCNames = new CircularList<String>(new ArrayList<String>());

	private final HostSelectionStrategyFactory<CL> selectorFactory;

	public HostSelectionWithFallback(HostSelectionStrategyFactory<CL> sFactory, ConnectionPoolMonitor monitor) {

		this(sFactory, monitor, new Callable<String>() {

			@Override
			public String call() throws Exception {
				return System.getenv("EC2_AVAILABILITY_ZONE");
			}
		});
	}

	public HostSelectionWithFallback(HostSelectionStrategyFactory<CL> sFactory, ConnectionPoolMonitor monitor, Callable<String> localDCFunc) {

		try {
			localDC = localDCFunc.call();
			Logger.info("Local dc: "  + localDC);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		selectorFactory = sFactory;
		cpMonitor = monitor;

		localSelector = selectorFactory.vendSelectionStrategy();
	}

	@Override
	public void initWithHosts(Map<Host, HostConnectionPool<CL>> hPools) {

		hostPools.putAll(hPools);

		Set<String> remoteDCs = new HashSet<String>();

		for (Host host : hPools.keySet()) {
			String dc = host.getDC();
			if (localDC != null && !localDC.isEmpty() && dc != null && !dc.isEmpty() && !localDC.equals(dc)) {
				remoteDCs.add(dc);
			}
		}

		Map<Host, HostConnectionPool<CL>> localPools = getHostPoolsForDC(localDC);
		localSelector.initWithHosts(localPools);

		for (String dc : remoteDCs) {

			Map<Host, HostConnectionPool<CL>> dcPools = getHostPoolsForDC(dc);

			HostSelectionStrategy<CL> remoteSelector = selectorFactory.vendSelectionStrategy();
			remoteSelector.initWithHosts(dcPools);

			remoteDCSelectors.put(dc, remoteSelector);
		}

		remoteDCNames.swapWithList(remoteDCSelectors.keySet());
	}

	@Override
	public Connection<CL> getConnection(BaseOperation<CL, ?> op, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {

		Connection<CL> connection = null; 
		DynoConnectException lastEx = null;
		
		try {
			connection = localSelector.getConnection(op, duration, unit);
		} catch (NoAvailableHostsException e) {
			lastEx = e;
			cpMonitor.incOperationFailure(null, e);
		} catch (PoolOfflineException e) {
			lastEx = e;
			cpMonitor.incOperationFailure(null, e);
		} catch (PoolTimeoutException e) {
			lastEx = e;
			cpMonitor.incOperationFailure(null, e);
		} catch (PoolExhaustedException e) {
			lastEx = e;
			cpMonitor.incOperationFailure(null, e);
		}

		if (lastEx != null) {
			
			Host host = lastEx.getHost();
			if (host != null) { 
				cpMonitor.incFailover(host, null);
			}
			
			int numRemotes = remoteDCNames.getEntireList().size();
			if (numRemotes == 0) {
				throw lastEx; // give up
			}

			connection = getFallbackConnection(op, duration, unit);
			
		} else if (!isConnectionPoolActive(connection)) {
			
			Host host = connection != null ? connection.getHost() : null;
			cpMonitor.incFailover(host, null);
			connection = getFallbackConnection(op, duration, unit);
		}
		
		if (connection != null) {
			return connection;
		}
		
		if (lastEx != null) {
			throw lastEx;
		} else {
			throw new PoolExhaustedException("No available connections in connection pool");
		}
	}


	@Override
	public Connection<CL> getFallbackConnection(BaseOperation<CL, ?> op, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {

		int numRemotes = remoteDCNames.getEntireList().size();

		Connection<CL> connection = null;
		DynoException lastEx = null;
		
		if (numRemotes == 0) {
			throw new NoAvailableHostsException("Could not find any remote hosts for fallback connection");
		}

		while ((numRemotes > 0) && (connection == null)) {

			numRemotes--;
			String dc = remoteDCNames.getNextElement();
			HostSelectionStrategy<CL> remoteDCSelector = remoteDCSelectors.get(dc);

			try {
				connection = remoteDCSelector.getConnection(op, duration, unit);

				if (!isConnectionPoolActive(connection)) {
					connection = null; // look for another connection
				}

			} catch (NoAvailableHostsException e) {
				cpMonitor.incOperationFailure(null, e);
				lastEx = e;
			} catch (PoolExhaustedException e) {
				cpMonitor.incOperationFailure(null, e);
				lastEx = e;
			}
		}

		if (connection == null) {
			if (lastEx != null) {
				throw lastEx;
			} else {
				throw new PoolExhaustedException("Local zone host is down and no remote zone hosts for fallback");
			}
		}

		return connection;
	}

	@Override
	public void addHost(Host host, HostConnectionPool<CL> hostPool) {
		hostPools.put(host, hostPool);
		HostSelectionStrategy<CL> selector = findSelector(host);
		if (selector != null) {
			selector.addHost(host, hostPool);
		}
	}

	@Override
	public void removeHost(Host host, HostConnectionPool<CL> hostPool) {

		hostPools.remove(host);
		HostSelectionStrategy<CL> selector = findSelector(host);
		if (selector != null) {
			selector.removeHost(host, hostPool);
		}
	}

	private HostSelectionStrategy<CL> findSelector(Host host) {
		String dc = host.getDC();
		if (localDC == null) {
			return localSelector;
		}

		if (localDC.equals(dc)) {
			return localSelector;
		}

		HostSelectionStrategy<CL> remoteSelector = remoteDCSelectors.get(dc);
		return remoteSelector;
	}

	private boolean isConnectionPoolActive(Connection<CL> connection) {
		if (connection == null) {
			return false;
		}
		HostConnectionPool<CL> hPool = connection.getParentConnectionPool();
		Host host = hPool.getHost();

		if (!host.isUp()) {
			return false;
		} else {
			return hPool.isActive();
		}
	}


	@Override
	public Map<BaseOperation<CL, ?>, Connection<CL>> getConnection(Collection<BaseOperation<CL, ?>> ops, int duration, TimeUnit unit)
			throws NoAvailableHostsException, PoolExhaustedException {

		Map<BaseOperation<CL, ?>, Connection<CL>> map = new HashMap<BaseOperation<CL, ?>, Connection<CL>>();

		for (BaseOperation<CL, ?> op : ops) {
			Connection<CL> connectionForOp = getConnection(op, duration, unit);
			map.put(op, connectionForOp);
		}
		return map;
	}

	private Map<Host, HostConnectionPool<CL>> getHostPoolsForDC(final String dc) {

		Map<Host, HostConnectionPool<CL>> dcPools = 
				CollectionUtils.filterKeys(hostPools, new Predicate<Host>() {

					@Override
					public boolean apply(Host x) {
						if (localDC == null) {
							return true;
						}
						return dc.equals(x.getDC());
					}

				});
		return dcPools;
	}


	public Long getKeyHash(String key) {
		return null;
//		TokenAwareSelector tSelector = (TokenAwareSelector) localSelector;
//		return tSelector.getKeyHash(key);
	}
	
	public static class UnitTest {
		
		private Map<Host, AtomicBoolean> poolStatus = new HashMap<Host, AtomicBoolean>();
		
		private BaseOperation<Integer, Integer> testOperation = new BaseOperation<Integer, Integer>() {

			@Override
			public String getName() {
				return "test";
			}

			@Override
			public String getKey() {
				return "11";
			}
		};
		
		@Test
		public void testFallbackToRemotePoolWhenPoolInactive() throws Exception {
			
			Callable<String> localDCFunc = new Callable<String>() {
				@Override
				public String call() throws Exception {
					return "localTestDC";
				}
			};
			
			HostSelectionStrategyFactory<Integer> sFactory = new HostSelectionStrategyFactory<Integer>() {

				@Override
				public HostSelectionStrategy<Integer> vendSelectionStrategy() {
					return new RoundRobinSelection<Integer>();
				}
			};
			
			ConnectionPoolMonitor cpMon = new CountingConnectionPoolMonitor();
			
			HostSelectionWithFallback<Integer> selection = new HostSelectionWithFallback<Integer>(sFactory, cpMon, localDCFunc);
			
			Map<Host, HostConnectionPool<Integer>> pools = new HashMap<Host, HostConnectionPool<Integer>>();
			
			Host h1 = new Host("h1", Status.Up).setDC("localTestDC");
			Host h2 = new Host("h2", Status.Up).setDC("localTestDC");
			Host h3 = new Host("h3", Status.Up).setDC("remoteDC1");
			Host h4 = new Host("h4", Status.Up).setDC("remoteDC1");
			Host h5 = new Host("h5", Status.Up).setDC("remoteDC2");
			Host h6 = new Host("h6", Status.Up).setDC("remoteDC2");
			
			Host[] arr = {h1, h2, h3, h4, h5, h6};
			List<Host> hosts = Arrays.asList(arr);
			
			for (Host host : hosts) {
				poolStatus.put(host, new AtomicBoolean(true));
				pools.put(host, getMockHostConnectionPool(host, poolStatus.get(host)));
			}
			
			selection.initWithHosts(pools);
			
			Set<String> hostnames = new HashSet<String>();
			
			for (int i=0; i<10; i++) {
				Connection<Integer> conn = selection.getConnection(testOperation, 1, TimeUnit.MILLISECONDS);
				hostnames.add(conn.getHost().getHostName());
			}
			
			verifyExactly(hostnames, "h1", "h2");
			
			// Now mark h1 and h2 both as "DOWN"
			poolStatus.get(h1).set(false);
			poolStatus.get(h2).set(false);
			hostnames.clear();
			
			for (int i=0; i<10; i++) {
				Connection<Integer> conn = selection.getConnection(testOperation, 1, TimeUnit.MILLISECONDS);
				hostnames.add(conn.getHost().getHostName());
			}
			
			verifyExactly(hostnames, "h3", "h4", "h5", "h6");
			
			// Now bring h1 back up
			poolStatus.get(h1).set(true);
			hostnames.clear();
			
			for (int i=0; i<10; i++) {
				Connection<Integer> conn = selection.getConnection(testOperation, 1, TimeUnit.MILLISECONDS);
				hostnames.add(conn.getHost().getHostName());
			}
			
			verifyExactly(hostnames, "h1");

			// Now bring h2 back up
			poolStatus.get(h2).set(true);
			hostnames.clear();
			for (int i=0; i<10; i++) {
				Connection<Integer> conn = selection.getConnection(testOperation, 1, TimeUnit.MILLISECONDS);
				hostnames.add(conn.getHost().getHostName());
			}
			
			verifyExactly(hostnames, "h1", "h2");
		}
		
		@Test
		public void testFallbackToRemotePoolWhenHostDown() throws Exception {
			
			Callable<String> localDCFunc = new Callable<String>() {
				@Override
				public String call() throws Exception {
					return "localTestDC";
				}
			};
			
			HostSelectionStrategyFactory<Integer> sFactory = new HostSelectionStrategyFactory<Integer>() {

				@Override
				public HostSelectionStrategy<Integer> vendSelectionStrategy() {
					return new RoundRobinSelection<Integer>();
				}
			};
			
			ConnectionPoolMonitor cpMon = new CountingConnectionPoolMonitor();
			
			HostSelectionWithFallback<Integer> selection = new HostSelectionWithFallback<Integer>(sFactory, cpMon, localDCFunc);
			
			Map<Host, HostConnectionPool<Integer>> pools = new HashMap<Host, HostConnectionPool<Integer>>();
			
			Host h1 = new Host("h1", Status.Up).setDC("localTestDC");
			Host h2 = new Host("h2", Status.Up).setDC("localTestDC");
			Host h3 = new Host("h3", Status.Up).setDC("remoteDC1");
			Host h4 = new Host("h4", Status.Up).setDC("remoteDC1");
			Host h5 = new Host("h5", Status.Up).setDC("remoteDC2");
			Host h6 = new Host("h6", Status.Up).setDC("remoteDC2");
			
			Host[] arr = {h1, h2, h3, h4, h5, h6};
			List<Host> hosts = Arrays.asList(arr);
			
			for (Host host : hosts) {
				poolStatus.put(host, new AtomicBoolean(true));
				pools.put(host, getMockHostConnectionPool(host, poolStatus.get(host)));
			}
			
			selection.initWithHosts(pools);
			
			Set<String> hostnames = new HashSet<String>();
			
			for (int i=0; i<10; i++) {
				Connection<Integer> conn = selection.getConnection(testOperation, 1, TimeUnit.MILLISECONDS);
				hostnames.add(conn.getHost().getHostName());
			}
			
			verifyExactly(hostnames, "h1", "h2");
			
			// Now mark h1 and h2 both as "DOWN"
			h1.setStatus(Status.Down); 
			h2.setStatus(Status.Down); 
			hostnames.clear();
			
			for (int i=0; i<10; i++) {
				Connection<Integer> conn = selection.getConnection(testOperation, 1, TimeUnit.MILLISECONDS);
				hostnames.add(conn.getHost().getHostName());
			}
			
			verifyExactly(hostnames, "h3", "h4", "h5", "h6");
			
			// Now bring h1 back up
			h1.setStatus(Status.Up); 
			hostnames.clear();
			
			for (int i=0; i<10; i++) {
				Connection<Integer> conn = selection.getConnection(testOperation, 1, TimeUnit.MILLISECONDS);
				hostnames.add(conn.getHost().getHostName());
			}
			
			System.out.println(" " + hostnames);
			verifyExactly(hostnames, "h1");

			// Now bring h2 back up
			h2.setStatus(Status.Up); 
			hostnames.clear();
			for (int i=0; i<10; i++) {
				Connection<Integer> conn = selection.getConnection(testOperation, 1, TimeUnit.MILLISECONDS);
				hostnames.add(conn.getHost().getHostName());
			}
			
			verifyExactly(hostnames, "h1", "h2");
		}
		
		private void verifyExactly(Set<String> result, String ... hostnames) {
			
			Set<String> all = new HashSet<String>();
			all.add("h1"); all.add("h2"); all.add("h3");
			all.add("h4"); all.add("h5"); all.add("h6");
				
			Set<String> expected = new HashSet<String>(Arrays.asList(hostnames));
			Set<String> notExpected = new HashSet<String>(all);
			notExpected.removeAll(expected);
			
			for (String e : expected) {
				Assert.assertTrue(result.contains(e));
			}
			for (String ne : notExpected) {
				Assert.assertFalse(result.contains(ne));
			}
		}
		
		private HostConnectionPool<Integer> getMockHostConnectionPool(final Host host, final AtomicBoolean status) {
			
			Connection<Integer> mockConnection = mock(Connection.class); 
			when(mockConnection.getHost()).thenReturn(host);

			HostConnectionPool<Integer> mockPool = mock(HostConnectionPool.class); 
			when(mockPool.isActive()).thenAnswer(new Answer<Boolean>() {

				@Override
				public Boolean answer(InvocationOnMock invocation) throws Throwable {
					return status.get();
				}
				
			});
			when(mockPool.borrowConnection(any(Integer.class), any(TimeUnit.class))).thenReturn(mockConnection);
			when(mockPool.getHost()).thenReturn(host);
			
			when(mockConnection.getParentConnectionPool()).thenReturn(mockPool);
			
			return mockPool;
		}
		
	}
}
