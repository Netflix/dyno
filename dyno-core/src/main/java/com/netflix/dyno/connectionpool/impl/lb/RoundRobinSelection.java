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
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.exception.PoolExhaustedException;
import com.netflix.dyno.connectionpool.exception.PoolTimeoutException;
import com.netflix.dyno.connectionpool.impl.HostSelectionStrategy;

/**
 * Simple impl of {@link HostSelectionStrategy} that uses ROUND ROBIN. It employs the {@link CircularList} data structure
 * to provide RR balancing in a thread safe manner. 
 * Note that the class can also support dynamically adding and removing {@link Host}
 * @author poberai
 *
 * @param <CL>
 */
public class RoundRobinSelection<CL> implements HostSelectionStrategy<CL> {
	
	// The total set of host pools. Once the host is selected, we ask it's corresponding pool to vend a connection
	private final ConcurrentHashMap<Host, HostConnectionPool<CL>> hostPools = new ConcurrentHashMap<Host, HostConnectionPool<CL>>();
	// the circular list of Host over which we load balance in a round robin fashion
	private final CircularList<Host> circularList = new CircularList<Host>(null);

	public RoundRobinSelection() {
	}
	
	
	@Override
	public Connection<CL> getConnection(BaseOperation<CL, ?> op, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {

		int n = circularList.getSize();
		Connection<CL> connection = null;
		
		while ((n > 0)  && (connection == null)) {
			try {
				connection = getNextConnection(duration, unit);
			} catch (PoolTimeoutException e) {
				
			} finally {
				n--;
			}
		}
		
		if (connection == null) {
			throw new PoolExhaustedException("No host pool connections were available");
		}
		return connection;
	}
	
	private Connection<CL> getNextConnection(int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {

		Host host = circularList.getNextElement();

		HostConnectionPool<CL> hostPool = hostPools.get(host);
		if (hostPool == null || !hostPool.isActive() || !host.isUp()) {
			return null;
		}
		
		Connection<CL> connection = hostPool.borrowConnection(duration, unit);
		return connection;
	}

	@Override
	public Map<BaseOperation<CL, ?>, Connection<CL>> getConnection(Collection<BaseOperation<CL, ?>> ops, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {
		Map<BaseOperation<CL, ?>, Connection<CL>> map = new HashMap<BaseOperation<CL, ?>, Connection<CL>>();
		for (BaseOperation<CL, ?> op : ops) {
			map.put(op, getConnection(op, duration, unit));
		}
		return map;
	}

	@Override
	public Connection<CL> getFallbackConnection(BaseOperation<CL, ?> op, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {
		throw new RuntimeException("Not Implemented");
	}

	@Override
	public void initWithHosts(Map<Host, HostConnectionPool<CL>> hPools) {
		hostPools.putAll(hPools);
		circularList.swapWithList(hostPools.keySet());
	}

	@Override
	public void addHost(Host host, HostConnectionPool<CL> hostPool) {
		
		hostPools.put(host, hostPool);
		List<Host> newHostList = new ArrayList<Host>(circularList.getEntireList());
		newHostList.add(host);
		circularList.swapWithList(newHostList);
	}

	@Override
	public void removeHost(Host host, HostConnectionPool<CL> hostPool) {

		hostPools.remove(host);
		List<Host> newHostList = new ArrayList<Host>(circularList.getEntireList());
		newHostList.remove(host);
		circularList.swapWithList(newHostList);
	}

	
	public String toString() {
		return "RoundRobinSelector: list: " + circularList.toString();
	}

	
	public static class UnitTest { 
		
		private final BaseOperation<Integer, Integer> testOperation = new BaseOperation<Integer, Integer>() {

			@Override
			public String getName() {
				return "TestOperation";
			}

			@Override
			public String getKey() {
				return null;
			}
		};
		
		@Test
		public void testRoundRobin() throws Exception {
			
			TreeMap<Host, HostConnectionPool<Integer>> pools = new TreeMap<Host, HostConnectionPool<Integer>>(new Comparator<Host>() {

				@Override
				public int compare(Host o1, Host o2) {
					return o1.getHostName().compareTo(o2.getHostName());
				}
			});

			// instantiate 3 host connection pools
			for (int i=1; i<=3; i++) {
				Host h = new Host("h"+i, Status.Up);
				pools.put(h, getMockHostConnectionPool(h));
			}
			
			RoundRobinSelection<Integer> rrSelection = new RoundRobinSelection<Integer>();
			rrSelection.initWithHosts(pools);
			
			Map<String, Integer> result = new HashMap<String, Integer>();
			
			runTest(300, result, rrSelection);
			verifyTest(result, hostCount("h1", 100), hostCount("h2", 100), hostCount("h3", 100));
			
			// Add a new host
			Host h4 = new Host("h4", Status.Up);
			rrSelection.addHost(h4, getMockHostConnectionPool(h4));
			
			runTest(400, result, rrSelection);
			verifyTest(result, hostCount("h1", 200), hostCount("h2", 200), hostCount("h3", 200), hostCount("h4", 100));

			// remove an old host
			Host h2 = new Host("h2", Status.Up);
			rrSelection.removeHost(h2, getMockHostConnectionPool(h2));

			runTest(600, result, rrSelection);
			verifyTest(result, hostCount("h1", 400), hostCount("h2", 200), hostCount("h3", 400), hostCount("h4", 300));
		}
		
		private void runTest(int iterations, Map<String, Integer> result, RoundRobinSelection<Integer> rrSelection) {
			
			for (int i=1; i<=iterations; i++) {
				
				Connection<Integer> connection = rrSelection.getConnection(testOperation, 1, TimeUnit.MILLISECONDS);
				String hostName = connection.getHost().getHostName();
				
				Integer count = result.get(hostName);
				if (count == null) {
					count = 0;
				}
				result.put(hostName, ++count);
			}
		}
		
		private void verifyTest(Map<String, Integer> result, HostCount ... hostCounts) {
			
			for (HostCount hostCount : hostCounts) {
				Integer resultCount = result.get(hostCount.host);
				Assert.assertEquals(hostCount.count, resultCount);
			}
		}
		
		private static class HostCount {
			private final String host; 
			private final Integer count; 
			private HostCount(String host, Integer count) {
				this.host = host;
				this.count = count;
			}
		}
		
		private HostCount hostCount(String host, Integer count) {
			return new HostCount(host, count);
		}
		
		@SuppressWarnings("unchecked")
		private HostConnectionPool<Integer> getMockHostConnectionPool(final Host host) {
			
			Connection<Integer> mockConnection = mock(Connection.class);
			when(mockConnection.getHost()).thenReturn(host);
			
			HostConnectionPool<Integer> mockHostPool = mock(HostConnectionPool.class);
			when(mockHostPool.isActive()).thenReturn(true);
			when(mockHostPool.borrowConnection(any(Integer.class), any(TimeUnit.class))).thenReturn(mockConnection);
			
			return mockHostPool;
		}
	}
}
