package com.netflix.dyno.connectionpool.impl.lb;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.impl.hash.Murmur1HashPartitioner;

public class TokenAwareSelectionTest {

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
