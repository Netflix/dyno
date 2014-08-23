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

import org.junit.Assert;
import org.junit.Test;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
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
	private final ConcurrentHashMap<Long, HostConnectionPool<CL>> tokenPools = new ConcurrentHashMap<Long, HostConnectionPool<CL>>();

	// the circular list of Host over which we load balance in a round robin fashion
	private final CircularList<HostToken> circularList = new CircularList<HostToken>(null);

	public RoundRobinSelection() {
	}

	@Override
	public HostConnectionPool<CL> getPoolForOperation(BaseOperation<CL, ?> op) throws NoAvailableHostsException {
		
		int numTries = circularList.getSize();
		HostConnectionPool<CL> lastPool = null;
		
		while (numTries > 0) {
			lastPool = getNextConnectionPool();
			numTries--;
			if (lastPool.isActive() && lastPool.getHost().isUp()) {
				return lastPool;
			}
		}
		
		// If we reach here then we haven't found an active pool. Return the last inactive pool anyways, 
		// and HostSelectionWithFallback can choose a fallback pool from another dc
		return lastPool; 
	}

	@Override
	public Map<HostConnectionPool<CL>, BaseOperation<CL, ?>> getPoolsForOperationBatch(Collection<BaseOperation<CL, ?>> ops) throws NoAvailableHostsException {
		Map<HostConnectionPool<CL>, BaseOperation<CL, ?>> map = new HashMap<HostConnectionPool<CL>, BaseOperation<CL, ?>>();
		for (BaseOperation<CL, ?> op : ops) {
			map.put(getNextConnectionPool(), op);
		}
		return map;
	}


	@Override
	public List<HostConnectionPool<CL>> getOrderedHostPools() {
		return new ArrayList<HostConnectionPool<CL>>(tokenPools.values());
	}


	@Override
	public HostConnectionPool<CL> getPoolForToken(Long token) {
		return tokenPools.get(token);
	}


	@Override
	public List<HostConnectionPool<CL>> getPoolsForTokens(Long start, Long end) {
		throw new NotImplementedException();
	}

	private HostConnectionPool<CL> getNextConnectionPool() throws NoAvailableHostsException {

		HostToken hostToken = circularList.getNextElement();

		HostConnectionPool<CL> hostPool = tokenPools.get(hostToken.getToken());
		if (hostPool == null) {
			throw new NoAvailableHostsException("Could not find host connection pool for host token: " + hostToken);
		}
		return hostPool;
	}
	
	@Override
	public void initWithHosts(Map<HostToken, HostConnectionPool<CL>> hPools) {
		
		for (HostToken token : hPools.keySet()) {
			tokenPools.put(token.getToken(), hPools.get(token));
		}
		circularList.swapWithList(hPools.keySet());
	}

	@Override
	public boolean addHostPool(HostToken host, HostConnectionPool<CL> hostPool) {
		
		HostConnectionPool<CL> prevPool = tokenPools.put(host.getToken(), hostPool);
		if (prevPool == null) {
			List<HostToken> newHostList = new ArrayList<HostToken>(circularList.getEntireList());
			newHostList.add(host);
			circularList.swapWithList(newHostList);
		}
		return prevPool == null;
	}

	@Override
	public boolean removeHostPool(HostToken host) {

		HostConnectionPool<CL> prevPool = tokenPools.get(host.getToken());
		if (prevPool != null) {
			List<HostToken> newHostList = new ArrayList<HostToken>(circularList.getEntireList());
			newHostList.remove(host);
			circularList.swapWithList(newHostList);
			tokenPools.remove(host.getToken());
		}
		return prevPool != null;
	}
	
	public String toString() {
		return "RoundRobinSelector: list: " + circularList.toString();
	}

	
	public static class UnitTest { 
		
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


		private final HostToken h1 = new HostToken(309687905L, new Host("h1", -1, Status.Up));
		private final HostToken h2 = new HostToken(1383429731L, new Host("h2", -1, Status.Up));
		private final HostToken h3 = new HostToken(2457171554L, new Host("h3", -1, Status.Up));
		private final HostToken h4 = new HostToken(3530913377L, new Host("h4", -1, Status.Up));
	
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
			
			TreeMap<HostToken, HostConnectionPool<Integer>> pools = new TreeMap<HostToken, HostConnectionPool<Integer>>(new Comparator<HostToken>() {

				@Override
				public int compare(HostToken o1, HostToken o2) {
					return o1.getHost().getHostName().compareTo(o2.getHost().getHostName());
				}
			});

			// instantiate 3 host connection pools
			pools.put(h1, getMockHostConnectionPool(h1));
			pools.put(h2, getMockHostConnectionPool(h2));
			pools.put(h3, getMockHostConnectionPool(h3));
			
			RoundRobinSelection<Integer> rrSelection = new RoundRobinSelection<Integer>();
			rrSelection.initWithHosts(pools);
			
			Map<String, Integer> result = new HashMap<String, Integer>();
			
			runTest(300, result, rrSelection);
			verifyTest(result, hostCount("h1", 100), hostCount("h2", 100), hostCount("h3", 100));
			
			// Add a new host
			rrSelection.addHostPool(h4, getMockHostConnectionPool(h4));
			
			runTest(400, result, rrSelection);
			verifyTest(result, hostCount("h1", 200), hostCount("h2", 200), hostCount("h3", 200), hostCount("h4", 100));

			// remove an old host
			rrSelection.removeHostPool(h2);

			runTest(600, result, rrSelection);
			verifyTest(result, hostCount("h1", 400), hostCount("h2", 200), hostCount("h3", 400), hostCount("h4", 300));
		}
		
		private void runTest(int iterations, Map<String, Integer> result, RoundRobinSelection<Integer> rrSelection) {
			
			for (int i=1; i<=iterations; i++) {
				
				HostConnectionPool<Integer> pool = rrSelection.getPoolForOperation(testOperation);
				String hostName = pool.getHost().getHostName();
				
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
		private HostConnectionPool<Integer> getMockHostConnectionPool(final HostToken hostToken) {
			
			HostConnectionPool<Integer> mockHostPool = mock(HostConnectionPool.class);
			when(mockHostPool.isActive()).thenReturn(true);
			when(mockHostPool.getHost()).thenReturn(hostToken.getHost());
			
			return mockHostPool;
		}
	}
}
