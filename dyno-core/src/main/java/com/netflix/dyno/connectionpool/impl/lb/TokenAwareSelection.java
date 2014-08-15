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
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.exception.PoolExhaustedException;
import com.netflix.dyno.connectionpool.impl.ConnectionContextImpl;
import com.netflix.dyno.connectionpool.impl.HostSelectionStrategy;
import com.netflix.dyno.connectionpool.impl.hash.BinarySearchTokenMapper;
import com.netflix.dyno.connectionpool.impl.hash.Murmur1HashPartitioner;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Predicate;

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

	private final TokenMapSupplier tokenSupplier;
	private final BinarySearchTokenMapper tokenMapper;
	private String myDC = null;

	private final ConcurrentHashMap<Host, HostConnectionPool<CL>> hostPools = new ConcurrentHashMap<Host, HostConnectionPool<CL>>();
	
	private static final String KeyHash = "KeyHash";
	
	public TokenAwareSelection() {
		this(new TokenMapSupplierImpl());
	}
	
	public TokenAwareSelection(TokenMapSupplier tokenMapSupplier) {
		
		if( tokenMapSupplier == null) {
			throw new DynoConnectException("TokenSupplier cannot be null when using TOKEN AWARE. See ConnectionPoolConfiguration.withTokenSupplier()");
		}
		
		this.tokenSupplier = tokenMapSupplier;
		this.tokenMapper = new BinarySearchTokenMapper(new Murmur1HashPartitioner());
	}

	@Override
	public void initWithHosts(Map<Host, HostConnectionPool<CL>> hPools) {
		
		Host host = hPools.keySet().iterator().next();
		myDC = host != null ? host.getDC() : null;
		
		hostPools.putAll(hPools);
		
		List<Host> hosts = new ArrayList<Host>(hostPools.keySet());
		
		this.tokenSupplier.initWithHosts(hosts);
		List<HostToken> allHostTokens = tokenSupplier.getTokens();
		
		if (allHostTokens == null || allHostTokens.isEmpty()) {
			throw new NoAvailableHostsException("Could not find any hosts from token supplier");
		}
		
		Collection<HostToken> localZoneTokens = CollectionUtils.filter(allHostTokens,  new Predicate<HostToken>() {

			@Override
			public boolean apply(HostToken x) {
				String hostDC = x.getHost().getDC();
				return myDC != null ? myDC.equalsIgnoreCase(hostDC) : true;
			}
		});
		
		this.tokenMapper.initSearchMecahnism(localZoneTokens);
	}

	@Override
	public Connection<CL> getConnection(BaseOperation<CL, ?> op, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {
		
		String key = op.getKey();
		Long keyHash = tokenMapper.hash(key);
		HostToken hToken = tokenMapper.getToken(keyHash);
		
		if (hToken == null) {
			return null;
		}
		
		HostConnectionPool<CL> hostPool = hostPools.get(hToken.getHost());
		
		if (hostPool == null || !hostPool.isActive()) {
			return null;
		}
		
		Connection<CL> connection = hostPool.borrowConnection(duration, unit);
		connection.getContext().setMetadata(KeyHash, keyHash);
		return connection;
	}

	@Override
	public Map<BaseOperation<CL, ?>, Connection<CL>> getConnection(Collection<BaseOperation<CL, ?>> ops, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {
		throw new RuntimeException("Not Implemented");
	}

	@Override
	public Connection<CL> getFallbackConnection(BaseOperation<CL, ?> op, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {
		throw new RuntimeException("Not Implemented");
	}

	@Override
	public void addHost(Host host, HostConnectionPool<CL> hostPool) {
		
		String dc = host.getDC();
		boolean isSameDc = myDC != null ? myDC.equalsIgnoreCase(dc) : true;
		
		if (isSameDc) {
			HostToken hostToken = tokenSupplier.getTokenForHost(host);
			if (hostToken != null) {
				tokenMapper.addHostToken(hostToken);
				hostPools.put(host, hostPool);
			}
		}
	}

	@Override
	public void removeHost(Host host, HostConnectionPool<CL> hostPool) {
		String dc = host.getDC();
		boolean isSameDc = myDC != null ? myDC.equalsIgnoreCase(dc) : true;
		
		if (isSameDc) {
			HostConnectionPool<CL> prev = hostPools.remove(host);
			if (prev != null) {
				tokenMapper.removeHost(host);
			}
		}
	}

	public Long getKeyHash(String key) {
		Long keyHash = tokenMapper.hash(key);
		return keyHash;
	}

	
	public String toString() {
		return "dc: " + myDC + " " + tokenMapper.toString();
	}
	
	
	public static class UnitTest { 
		
		@Test
		public void testTokenAware() throws Exception {
			
			TreeMap<Host, HostConnectionPool<Integer>> pools = new TreeMap<Host, HostConnectionPool<Integer>>(new Comparator<Host>() {

				@Override
				public int compare(Host o1, Host o2) {
					return o1.getHostName().compareTo(o2.getHostName());
				}
			});

			for (int i=1; i<=4; i++) {
				Host h = new Host("h"+i, Status.Up);
				pools.put(h, getMockHostConnectionPool(h));
			}
			
			Map<Host, HostToken> map = getTestTokenMap();
			TokenAwareSelection<Integer> tokenAwareSelector = new TokenAwareSelection<Integer>(new TestTokenMapSupplier(map));
			tokenAwareSelector.initWithHosts(pools);
			
			Map<String, Integer> result = new HashMap<String, Integer>();
			
			runTest(0L, 100000L, result, tokenAwareSelector);
			
			System.out.println("Token distribution: " + result);
			
			verifyTokenDistribution(result);
		}
		
		private class TestTokenMapSupplier implements TokenMapSupplier {
			
			private final Map<Host, HostToken> tokenMap = new HashMap<Host, HostToken>();
			
			private TestTokenMapSupplier(Map<Host, HostToken> map) {
				tokenMap.putAll(map);
			}
			
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
			}
		}
		
		
		private Map<Host, HostToken> getTestTokenMap() {

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

			HostToken h1 = new HostToken(309687905L, new Host("h1", -1, Status.Up));
			HostToken h2 = new HostToken(1383429731L, new Host("h2", -1, Status.Up));
			HostToken h3 = new HostToken(2457171554L, new Host("h3", -1, Status.Up));
			HostToken h4 = new HostToken(3530913377L, new Host("h4", -1, Status.Up));

			Map<Host, HostToken> map = new HashMap<Host, HostToken>();
			map.put(h1.getHost(), h1);
			map.put(h2.getHost(), h2);
			map.put(h3.getHost(), h3);
			map.put(h4.getHost(), h4);

			return map;
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
				Connection<Integer> connection = tokenAwareSelector.getConnection(op, 1, TimeUnit.MILLISECONDS);

				Long keyHash = (Long) connection.getContext().getMetadata(KeyHash);
				String hostName = connection.getHost().getHostName();
				
				verifyKeyHash(op.getKey(), keyHash, hostName);
				
				Integer count = result.get(hostName);
				if (count == null) {
					count = 0;
				}
				result.put(hostName, ++count);
			}
		}

		private void verifyKeyHash(String key, Long keyHash, String hostname) {
			
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
		private HostConnectionPool<Integer> getMockHostConnectionPool(final Host host) {
			
			final ConnectionContextImpl context = new ConnectionContextImpl();

			Connection<Integer> mockConnection = mock(Connection.class);
			
			when(mockConnection.getHost()).thenReturn(host);
			when(mockConnection.getContext()).thenReturn(context);
			
			HostConnectionPool<Integer> mockHostPool = mock(HostConnectionPool.class);
			when(mockHostPool.isActive()).thenReturn(true);
			when(mockHostPool.borrowConnection(any(Integer.class), any(TimeUnit.class))).thenReturn(mockConnection);
			
			return mockHostPool;
		}
	}
}
