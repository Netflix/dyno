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

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.impl.HostSelectionStrategy;
import com.netflix.dyno.connectionpool.impl.hash.BinarySearchTokenMapper;
import com.netflix.dyno.connectionpool.impl.hash.Murmur1HashPartitioner;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Transform;

/**
 * Simple class that implements {@link HostSelectionStrategy} using the TOKEN AWARE algorithm. 
 * Note that this component needs to be aware of the dynomite ring topology to be able to 
 * successfully map to the corrent token owner for any key of an {@link Operation}
 * 
 * @author poberai
 *
 * @param <CL>
 */
public class TokenAwareSelection<CL> implements HostSelectionStrategy<CL> {

	private final BinarySearchTokenMapper tokenMapper;

	private final ConcurrentHashMap<Long, HostConnectionPool<CL>> tokenPools = new ConcurrentHashMap<Long, HostConnectionPool<CL>>();
	
	public TokenAwareSelection() {
		
		this.tokenMapper = new BinarySearchTokenMapper(new Murmur1HashPartitioner());
	}

	@Override
	public void initWithHosts(Map<HostToken, HostConnectionPool<CL>> hPools) {
		
		tokenPools.putAll(CollectionUtils.transformMapKeys(hPools, new Transform<HostToken, Long>() {

			@Override
			public Long get(HostToken x) {
				return x.getToken();
			}
			
		}));

		this.tokenMapper.initSearchMecahnism(hPools.keySet());
	}

	@Override
	public HostConnectionPool<CL> getPoolForOperation(BaseOperation<CL, ?> op) throws NoAvailableHostsException {
		
		String key = op.getKey();
		Long keyHash = tokenMapper.hash(key);
		HostToken hToken = tokenMapper.getToken(keyHash);
		
		HostConnectionPool<CL> hostPool = null;
		if (hToken != null) {
			hostPool = tokenPools.get(hToken.getToken());
		}
		
		if (hostPool == null) {
			throw new NoAvailableHostsException("Could not find host connection pool for key: " + key + ", hash: " + keyHash);
		}
		
		return hostPool;
	}

	@Override
	public Map<HostConnectionPool<CL>,BaseOperation<CL,?>> getPoolsForOperationBatch(Collection<BaseOperation<CL, ?>> ops) throws NoAvailableHostsException {
		throw new RuntimeException("Not Implemented");
	}
	
	@Override
	public List<HostConnectionPool<CL>> getOrderedHostPools() {
		return new ArrayList<HostConnectionPool<CL>>(tokenPools.values());
	}
	
	@Override
	public HostConnectionPool<CL> getPoolForToken(Long token) {
		return tokenPools.get(token);
	}
	
	public List<HostConnectionPool<CL>> getPoolsForTokens(Long start, Long end) {
		throw new RuntimeException("Not Implemented");
	}

	@Override
	public boolean addHostPool(HostToken hostToken, HostConnectionPool<CL> hostPool) {
		
		HostConnectionPool<CL> prevPool = tokenPools.put(hostToken.getToken(), hostPool);
		if (prevPool == null) {
			tokenMapper.addHostToken(hostToken);
			return true;
		}  else {
			return false;
		}
	}

	@Override
	public boolean removeHostPool(HostToken hostToken) {

		HostConnectionPool<CL> prev = tokenPools.get(hostToken.getToken());
		if (prev != null) {
			tokenPools.remove(hostToken.getToken());
			return true;
		} else {
			return false;
		}
	}

	public Long getKeyHash(String key) {
		Long keyHash = tokenMapper.hash(key);
		return keyHash;
	}

	
	public String toString() {
		return "TokenAwareSelection: " + tokenMapper.toString();
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
		
		private final Murmur1HashPartitioner m1Hash = new Murmur1HashPartitioner();
		
		@Test
		public void testTokenAware() throws Exception {
			
			TreeMap<HostToken, HostConnectionPool<Integer>> pools = new TreeMap<HostToken, HostConnectionPool<Integer>>(new Comparator<HostToken>() {

				@Override
				public int compare(HostToken o1, HostToken o2) {
					return o1.getHost().getHostName().compareTo(o2.getHost().getHostName());
				}
			});

			pools.put(h1, getMockHostConnectionPool(h1));
			pools.put(h2, getMockHostConnectionPool(h2));
			pools.put(h3, getMockHostConnectionPool(h3));
			pools.put(h4, getMockHostConnectionPool(h4));
			
			TokenAwareSelection<Integer> tokenAwareSelector = new TokenAwareSelection<Integer>();
			tokenAwareSelector.initWithHosts(pools);
			
			Map<String, Integer> result = new HashMap<String, Integer>();
			runTest(0L, 100000L, result, tokenAwareSelector);
			
			System.out.println("Token distribution: " + result);
			
			verifyTokenDistribution(result);
		}
		
		private BaseOperation<Integer, Long> getTestOperation(final Long n) {
			
			return new BaseOperation<Integer, Long>() {

				@Override
				public String getName() {
					return "TestOperation" + n;
				}

				@Override
				public String getKey() {
					return "" + n;
				}
			};
		}
		
		private void runTest(long start, long end, Map<String, Integer> result, TokenAwareSelection<Integer> tokenAwareSelector) {
			
			for (long i=start; i<=end; i++) {
				
				BaseOperation<Integer, Long> op = getTestOperation(i);
				HostConnectionPool<Integer> pool = tokenAwareSelector.getPoolForOperation(op);

				String hostName = pool.getHost().getHostName();
				
				verifyKeyHash(op.getKey(), hostName);
				
				Integer count = result.get(hostName);
				if (count == null) {
					count = 0;
				}
				result.put(hostName, ++count);
			}
		}

		private void verifyKeyHash(String key, String hostname) {
			
			Long keyHash = m1Hash.hash(key);
			
			String expectedHostname = null;
			
			if (keyHash <= 309687905L) {
				expectedHostname = "h1";
			} else if (keyHash <= 1383429731L) {
				expectedHostname = "h2";
			} else if (keyHash <= 2457171554L) {
				expectedHostname = "h3";
			} else if (keyHash <= 3530913377L) {
				expectedHostname = "h4";
			} else {
				expectedHostname = "h1";
			}
			
			if (!expectedHostname.equals(hostname)) {
				Assert.fail("FAILED! for key: " + key + ", got hostname: " + hostname + ", expected: " + expectedHostname + " for hash: " + keyHash);
			}
		}
		
		private void verifyTokenDistribution(Map<String, Integer> result) {
		
			int sum = 0;  int count = 0;
			for (int n : result.values()) {
				sum += n;
				count++;
			}
			
			double mean = (sum/count);
			
			for (int n : result.values()) {
				double percentageDiff = 100*((mean-n)/mean);
				Assert.assertTrue(percentageDiff < 1.0);
			}
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
