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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration.LoadBalancingStrategy;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.exception.PoolExhaustedException;
import com.netflix.dyno.connectionpool.exception.PoolOfflineException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.impl.HostSelectionStrategy;
import com.netflix.dyno.connectionpool.impl.HostSelectionStrategy.HostSelectionStrategyFactory;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Predicate;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Transform;

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

public class HostSelectionWithFallback<CL> {

	private static final Logger Logger = LoggerFactory.getLogger(HostSelectionWithFallback.class);

	// tracks the local zone
	private final String localDC;
	// The selector for the local zone
	private final HostSelectionStrategy<CL> localSelector;
	// Track selectors for each remote DC
	private final ConcurrentHashMap<String, HostSelectionStrategy<CL>> remoteDCSelectors = new ConcurrentHashMap<String, HostSelectionStrategy<CL>>();

	private final ConcurrentHashMap<Host, HostToken> hostTokens = new ConcurrentHashMap<Host, HostToken>();

	private final TokenMapSupplier tokenSupplier; 
	private final ConnectionPoolConfiguration cpConfig;
	private final ConnectionPoolMonitor cpMonitor; 

	// list of names of remote zones. Used for RoundRobin over remote zones when local zone host is down
	private final CircularList<String> remoteDCNames = new CircularList<String>(new ArrayList<String>());

	private final HostSelectionStrategyFactory<CL> selectorFactory;

	public HostSelectionWithFallback(ConnectionPoolConfiguration config, ConnectionPoolMonitor monitor) {

		cpMonitor = monitor;
		cpConfig = config;
		localDC = cpConfig.getLocalDC();
		tokenSupplier = cpConfig.getTokenSupplier();

		selectorFactory = new DefaultSelectionFactory(cpConfig);
		localSelector = selectorFactory.vendPoolSelectionStrategy();
	}

	public Connection<CL> getConnection(BaseOperation<CL, ?> op, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {
		return getConnection(op, null, duration, unit);
	}

	private Connection<CL> getConnection(BaseOperation<CL, ?> op, Long token, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {

		HostConnectionPool<CL> hostPool = null; 
		DynoConnectException lastEx = null;
		
		boolean useFallback = false;
		
		try {
			hostPool = (op != null) ? localSelector.getPoolForOperation(op) : localSelector.getPoolForToken(token);
			useFallback = !isConnectionPoolActive(hostPool);
			
		} catch (NoAvailableHostsException e) {
			lastEx = e;
			cpMonitor.incOperationFailure(null, e);
			useFallback = true;
		}
		
		if (!useFallback) {
			try { 
				return hostPool.borrowConnection(duration, unit);
			} catch (DynoConnectException e) {
				lastEx = e;
				cpMonitor.incOperationFailure(null, e);
				useFallback = true;
			}
		}
		
		if (useFallback && cpConfig.getMaxFailoverCount() > 0) {
			// Check if we have any remotes to fallback to
			int numRemotes = remoteDCNames.getEntireList().size();
			if (numRemotes == 0) {
				if (lastEx != null) {
					throw lastEx; // give up
				} else {
					throw new PoolOfflineException(hostPool.getHost(), "host pool is offline and no DCs available for fallback");
				}
			} else {
				hostPool = getFallbackHostPool(op, token);
			}
		}
		
		if (hostPool == null) {
			throw new NoAvailableHostsException("Found no hosts when using fallback DC");
		}
		
		return hostPool.borrowConnection(duration, unit);
	}

	private HostConnectionPool<CL> getFallbackHostPool(BaseOperation<CL, ?> op, Long token) {
		
		int numRemotes = remoteDCNames.getEntireList().size();
		if (numRemotes == 0) {
			throw new NoAvailableHostsException("Could not find any remote DCs for fallback");
		}

		int numTries = Math.min(numRemotes, cpConfig.getMaxFailoverCount());
		
		DynoException lastEx = null;
		
		while ((numTries > 0)) {

			numTries--;
			String remoteDC = remoteDCNames.getNextElement();
			HostSelectionStrategy<CL> remoteDCSelector = remoteDCSelectors.get(remoteDC);

			try {
				
				HostConnectionPool<CL> fallbackHostPool = 
						(op != null) ? remoteDCSelector.getPoolForOperation(op) : remoteDCSelector.getPoolForToken(token);
				
				if (isConnectionPoolActive(fallbackHostPool)) {
					return fallbackHostPool;
				}

			} catch (NoAvailableHostsException e) {
				cpMonitor.incOperationFailure(null, e);
				lastEx = e;
			}
		}
		
		if (lastEx != null) {
			throw lastEx;
		} else {
			throw new NoAvailableHostsException("Local zone host offline and could not find any remote hosts for fallback connection");
		}
	}

	public Collection<Connection<CL>> getConnectionsToRing(int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {
		
		final Collection<HostToken> localZoneTokens = CollectionUtils.filter(hostTokens.values(), new Predicate<HostToken>() {
			@Override
			public boolean apply(HostToken x) {
				return localDC != null ? localDC.equalsIgnoreCase(x.getHost().getDC()) : true; 
			}
		});
		
		final Collection<Long> tokens = CollectionUtils.transform(localZoneTokens, new Transform<HostToken, Long>() {
			@Override
			public Long get(HostToken x) {
				return x.getToken();
			}
		});
		
		DynoConnectException lastEx = null;
		
		List<Connection<CL>> connections = new ArrayList<Connection<CL>>();
				
		for (Long token : tokens) {
			try { 
				connections.add(getConnection(null, token, duration, unit));
			} catch (DynoConnectException e) {
				Logger.warn("Failed to get connection when getting all connections from ring", e.getMessage());
				lastEx = e;
				break;
			}
		}
		
		if (lastEx != null) {
			// Return all previously borrowed connection to avoid any conneciton leaks
			for (Connection<CL> connection : connections) {
				try {
					connection.getParentConnectionPool().returnConnection(connection);
				} catch (DynoConnectException e) {
					// do nothing
				}
			}
			throw lastEx;
			
		} else {
			return connections;
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

	private boolean isConnectionPoolActive(HostConnectionPool<CL> hPool) {
		if (hPool == null) {
			return false;
		}
		Host host = hPool.getHost();

		if (!host.isUp()) {
			return false;
		} else {
			return hPool.isActive();
		}
	}

	private Map<HostToken, HostConnectionPool<CL>> getHostPoolsForDC(final Map<HostToken, HostConnectionPool<CL>> map, final String dc) {

		Map<HostToken, HostConnectionPool<CL>> dcPools = 
				CollectionUtils.filterKeys(map, new Predicate<HostToken>() {

					@Override
					public boolean apply(HostToken x) {
						if (localDC == null) {
							return true;
						}
						return dc.equals(x.getHost().getDC());
					}
				});
		return dcPools;
	}
	
	public void initWithHosts(Map<Host, HostConnectionPool<CL>> hPools) {

		// Get the list of tokens for these hosts
		tokenSupplier.initWithHosts(hPools.keySet());
		List<HostToken> allHostTokens = tokenSupplier.getTokens();

		Map<HostToken, HostConnectionPool<CL>> tokenPoolMap = new HashMap<HostToken, HostConnectionPool<CL>>();
		
		// Update inner state with the host tokens.
		
		for (HostToken hToken : allHostTokens) {
			hostTokens.put(hToken.getHost(), hToken);
			tokenPoolMap.put(hToken, hPools.get(hToken.getHost()));
		}
		
		Set<String> remoteDCs = new HashSet<String>();

		for (Host host : hPools.keySet()) {
			String dc = host.getDC();
			if (localDC != null && !localDC.isEmpty() && dc != null && !dc.isEmpty() && !localDC.equals(dc)) {
				remoteDCs.add(dc);
			}
		}

		Map<HostToken, HostConnectionPool<CL>> localPools = getHostPoolsForDC(tokenPoolMap, localDC);
		localSelector.initWithHosts(localPools);

		for (String dc : remoteDCs) {

			Map<HostToken, HostConnectionPool<CL>> dcPools = getHostPoolsForDC(tokenPoolMap, dc);

			HostSelectionStrategy<CL> remoteSelector = selectorFactory.vendPoolSelectionStrategy();
			remoteSelector.initWithHosts(dcPools);

			remoteDCSelectors.put(dc, remoteSelector);
		}

		remoteDCNames.swapWithList(remoteDCSelectors.keySet());
	}


	public void addHost(Host host, HostConnectionPool<CL> hostPool) {
		
		HostToken hostToken = tokenSupplier.getTokenForHost(host);
		if (hostToken == null) {
			throw new DynoConnectException("Could not find host token for host: " + host);
		}
		
		hostTokens.put(hostToken.getHost(), hostToken);
		
		HostSelectionStrategy<CL> selector = findSelector(host);
		if (selector != null) {
			selector.addHostPool(hostToken, hostPool);
		}
	}

	public void removeHost(Host host, HostConnectionPool<CL> hostPool) {

		HostToken hostToken = hostTokens.remove(host);
		if (hostToken != null) {
			HostSelectionStrategy<CL> selector = findSelector(host);
			if (selector != null) {
				selector.removeHostPool(hostToken);
			}
		}
	}

	private class DefaultSelectionFactory implements HostSelectionStrategyFactory<CL> {

		private final LoadBalancingStrategy lbStrategy;
		private DefaultSelectionFactory(ConnectionPoolConfiguration config) {
			lbStrategy = config.getLoadBalancingStrategy();
		}
		@Override
		public HostSelectionStrategy<CL> vendPoolSelectionStrategy() {
			
			switch (lbStrategy) {
			case RoundRobin:
				return new RoundRobinSelection<CL>();
			case TokenAware:
				return new TokenAwareSelection<CL>();
			default :
				throw new RuntimeException("LoadBalancing strategy not supported! " + cpConfig.getLoadBalancingStrategy().name());
			}
		}
		
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
		
		private final ConnectionPoolConfigurationImpl cpConfig = new ConnectionPoolConfigurationImpl("test");
		private final ConnectionPoolMonitor cpMonitor = new CountingConnectionPoolMonitor();

		Host h1 = new Host("h1", Status.Up).setDC("localTestDC");
		Host h2 = new Host("h2", Status.Up).setDC("localTestDC");
		Host h3 = new Host("h3", Status.Up).setDC("remoteDC1");
		Host h4 = new Host("h4", Status.Up).setDC("remoteDC1");
		Host h5 = new Host("h5", Status.Up).setDC("remoteDC2");
		Host h6 = new Host("h6", Status.Up).setDC("remoteDC2");
		
		Host[] arr = {h1, h2, h3, h4, h5, h6};
		List<Host> hosts = Arrays.asList(arr);

		@Before
		public void beforeTest() {
			cpConfig.setLocalDC("localTestDC");
			cpConfig.setLoadBalancingStrategy(LoadBalancingStrategy.RoundRobin);
			cpConfig.withTokenSupplier(getTokenMapSupplier());
		}
		
		@Test
		public void testFallbackToRemotePoolWhenPoolInactive() throws Exception {
			
			HostSelectionWithFallback<Integer> selection = new HostSelectionWithFallback<Integer>(cpConfig, cpMonitor);
			
			Map<Host, HostConnectionPool<Integer>> pools = new HashMap<Host, HostConnectionPool<Integer>>();
			
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
			
			HostSelectionWithFallback<Integer> selection = new HostSelectionWithFallback<Integer>(cpConfig, cpMonitor);
			
			Map<Host, HostConnectionPool<Integer>> pools = new HashMap<Host, HostConnectionPool<Integer>>();
			
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
		
		@Test
		public void testGetConnectionsFromRingNormal() throws Exception {
			
			HostSelectionWithFallback<Integer> selection = new HostSelectionWithFallback<Integer>(cpConfig, cpMonitor);
			
			Map<Host, HostConnectionPool<Integer>> pools = new HashMap<Host, HostConnectionPool<Integer>>();
			
			for (Host host : hosts) {
				poolStatus.put(host, new AtomicBoolean(true));
				pools.put(host, getMockHostConnectionPool(host, poolStatus.get(host)));
			}
			
			selection.initWithHosts(pools);

			Collection<String> hostnames = runConnectionsToRingTest(selection);
			verifyExactly(hostnames, "h1", "h2");
		}
		
		@Test
		public void testGetConnectionsFromRingWhenPrimaryHostPoolInactive() throws Exception {
			
			HostSelectionWithFallback<Integer> selection = new HostSelectionWithFallback<Integer>(cpConfig, cpMonitor);
			
			Map<Host, HostConnectionPool<Integer>> pools = new HashMap<Host, HostConnectionPool<Integer>>();
			
			for (Host host : hosts) {
				poolStatus.put(host, new AtomicBoolean(true));
				pools.put(host, getMockHostConnectionPool(host, poolStatus.get(host)));
			}
			
			selection.initWithHosts(pools);

			// Put Host H1 as DOWN
			poolStatus.get(h1).set(false);
			
			Collection<String> hostnames = runConnectionsToRingTest(selection);
			verifyPresent(hostnames, "h2");
			verifyAtLeastOnePresent(hostnames, "h3", "h5");
			
			// Put Host H2 as DOWN
			selection.initWithHosts(pools);
			poolStatus.get(h1).set(true);
			poolStatus.get(h2).set(false);
			
			hostnames = runConnectionsToRingTest(selection);
			
			verifyPresent(hostnames, "h1");
			verifyAtLeastOnePresent(hostnames, "h4", "h6");
		
			// Put Hosts H1 and H2 as DOWN
			selection.initWithHosts(pools);
			poolStatus.get(h1).set(false);
			poolStatus.get(h2).set(false);

			hostnames = runConnectionsToRingTest(selection);
			verifyAtLeastOnePresent(hostnames, "h3", "h5");
			verifyAtLeastOnePresent(hostnames, "h4", "h6");
			
			// Put Hosts H1,H2,H3 as DOWN
			selection.initWithHosts(pools);
			poolStatus.get(h1).set(false);
			poolStatus.get(h2).set(false);
			poolStatus.get(h3).set(false);

			hostnames = runConnectionsToRingTest(selection);
			verifyPresent(hostnames, "h4", "h5");

			// Put Hosts H1,H2,H3,H4 as DOWN
			selection.initWithHosts(pools);
			poolStatus.get(h1).set(false);
			poolStatus.get(h2).set(false);
			poolStatus.get(h3).set(false);
			poolStatus.get(h4).set(false);

			hostnames = runConnectionsToRingTest(selection);
			verifyExactly(hostnames, "h5", "h6");
		}
		
		@Test
		public void testGetConnectionsFromRingWhenHostDown() throws Exception {
			
			HostSelectionWithFallback<Integer> selection = new HostSelectionWithFallback<Integer>(cpConfig, cpMonitor);
			
			Map<Host, HostConnectionPool<Integer>> pools = new HashMap<Host, HostConnectionPool<Integer>>();
			
			for (Host host : hosts) {
				poolStatus.put(host, new AtomicBoolean(true));
				pools.put(host, getMockHostConnectionPool(host, poolStatus.get(host)));
			}
			
			selection.initWithHosts(pools);

			// Put Host H1 as DOWN
			h1.setStatus(Status.Down);
			
			Collection<String> hostnames = runConnectionsToRingTest(selection);
			verifyPresent(hostnames, "h2");
			verifyAtLeastOnePresent(hostnames, "h3", "h5");
			
			// Put Host H2 as DOWN
			selection.initWithHosts(pools);
			h1.setStatus(Status.Up);
			h2.setStatus(Status.Down);
			
			hostnames = runConnectionsToRingTest(selection);
			
			verifyPresent(hostnames, "h1");
			verifyAtLeastOnePresent(hostnames, "h4", "h6");
		
			// Put Hosts H1 and H2 as DOWN
			selection.initWithHosts(pools);
			h1.setStatus(Status.Down);
			h2.setStatus(Status.Down);

			hostnames = runConnectionsToRingTest(selection);
			verifyAtLeastOnePresent(hostnames, "h3", "h5");
			verifyAtLeastOnePresent(hostnames, "h4", "h6");
			
			// Put Hosts H1,H2,H3 as DOWN
			selection.initWithHosts(pools);
			h1.setStatus(Status.Down);
			h2.setStatus(Status.Down);
			h3.setStatus(Status.Down);

			hostnames = runConnectionsToRingTest(selection);
			verifyPresent(hostnames, "h4", "h5");

			// Put Hosts H1,H2,H3,H4 as DOWN
			selection.initWithHosts(pools);
			h1.setStatus(Status.Down);
			h2.setStatus(Status.Down);
			h3.setStatus(Status.Down);
			h4.setStatus(Status.Down);

			hostnames = runConnectionsToRingTest(selection);
			verifyExactly(hostnames, "h5", "h6");
		}

		private Collection<String> runConnectionsToRingTest(HostSelectionWithFallback<Integer> selection) {
			
			Collection<Connection<Integer>> connections = selection.getConnectionsToRing(10, TimeUnit.MILLISECONDS);
			
			return CollectionUtils.transform(connections, new Transform<Connection<Integer>, String>() {
				@Override
				public String get(Connection<Integer> x) {
					return x.getHost().getHostName();
				}
			});
			
		}
		
		private void verifyExactly(Collection<String> resultCollection, String ... hostnames) {
			
			Set<String> result = new HashSet<String>(resultCollection);
			Set<String> all = new HashSet<String>();
			all.add("h1"); all.add("h2"); all.add("h3");
			all.add("h4"); all.add("h5"); all.add("h6");
				
			Set<String> expected = new HashSet<String>(Arrays.asList(hostnames));
			Set<String> notExpected = new HashSet<String>(all);
			notExpected.removeAll(expected);
			
			for (String e : expected) {
				Assert.assertTrue("Result: " + result + ", expected: " + e, result.contains(e));
			}
			for (String ne : notExpected) {
				Assert.assertFalse("Result: " + result, result.contains(ne));
			}
		}
		
		private void verifyPresent(Collection<String> resultCollection, String ... hostnames) {
			
			Set<String> result = new HashSet<String>(resultCollection);
			for (String h : hostnames) {
				Assert.assertTrue("Result: " + result + ", expected: " + h, result.contains(h));
			}
		}
	
		private void verifyAtLeastOnePresent(Collection<String> resultCollection, String ... hostnames) {
			
			Set<String> result = new HashSet<String>(resultCollection);
			boolean present = false;
			for (String h : hostnames) {
				if (result.contains(h)) {
					present = true;
					break;
				}
			}
			Assert.assertTrue("Result: " + result + ", expected at least one of: " + hostnames, present);
		}

		@SuppressWarnings("unchecked")
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
		

		/**
		cqlsh:dyno_bootstrap> select "availabilityZone","hostname","token" from tokens where "appId" = 'dynomite_redis_puneet';

			availabilityZone | hostname                                   | token
			------------------+--------------------------------------------+------------
   			us-east-1c |  ec2-54-83-179-213.compute-1.amazonaws.com | 1383429731
   			us-east-1c |  ec2-54-224-184-99.compute-1.amazonaws.com |  309687905
   			us-east-1c |  ec2-54-91-190-159.compute-1.amazonaws.com | 3530913377
   			us-east-1c |   ec2-54-81-31-218.compute-1.amazonaws.com | 2457171554
   			us-east-1e | ec2-54-198-222-153.compute-1.amazonaws.com |  309687905
   			us-east-1e | ec2-54-198-239-231.compute-1.amazonaws.com | 2457171554
   			us-east-1e |  ec2-54-226-212-40.compute-1.amazonaws.com | 1383429731
   			us-east-1e | ec2-54-197-178-229.compute-1.amazonaws.com | 3530913377

		cqlsh:dyno_bootstrap> 
		 */
	
		private TokenMapSupplier getTokenMapSupplier() {
			
			final Map<Host, HostToken> tokenMap = new HashMap<Host, HostToken>();

			return new TokenMapSupplier () {
				@Override
				public List<HostToken> getTokens() {
					return new ArrayList<HostToken>(tokenMap.values());
				}

				@Override
				public HostToken getTokenForHost(Host host) {
					return tokenMap.get(host);
				}

				@Override
				public void initWithHosts(Collection<Host> hosts) {
					
					tokenMap.clear();
					
					tokenMap.put(h1, new HostToken(1383429731L, h1));
					tokenMap.put(h2, new HostToken(3530913377L, h2));
					tokenMap.put(h3, new HostToken(1383429731L, h3));
					tokenMap.put(h4, new HostToken(3530913377L, h4));
					tokenMap.put(h5, new HostToken(1383429731L, h5));
					tokenMap.put(h6, new HostToken(3530913377L, h6));
				}
			};
		}
	}
		
}
