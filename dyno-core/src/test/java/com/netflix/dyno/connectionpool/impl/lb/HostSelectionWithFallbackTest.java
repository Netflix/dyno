/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.dyno.connectionpool.RetryPolicy;
import com.netflix.dyno.connectionpool.impl.RetryNTimes;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration.LoadBalancingStrategy;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.HashPartitioner;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Transform;
import static org.junit.Assert.assertEquals;

public class HostSelectionWithFallbackTest {

	private Map<Host, AtomicBoolean> poolStatus = new HashMap<Host, AtomicBoolean>();

	private BaseOperation<Integer, Integer> testOperation = new BaseOperation<Integer, Integer>() {

		@Override
		public String getName() {
			return "test";
		}

		@Override
		public String getStringKey() {
			return "11";
		}

		@Override
		public byte[] getBinaryKey() {
			return null;
		}
	};
        
	private final ConnectionPoolConfigurationImpl cpConfig = new ConnectionPoolConfigurationImpl("test");
	private final ConnectionPoolMonitor cpMonitor = new CountingConnectionPoolMonitor();

	Host h1 = new Host("h1", 8102,"localTestRack", Status.Up);
	Host h2 = new Host("h2", 8102, "localTestRack", Status.Up);
	Host h3 = new Host("h3", 8102,"remoteRack1", Status.Up);
	Host h4 = new Host("h4", 8102,"remoteRack1", Status.Up);
	Host h5 = new Host("h5", 8102, "remoteRack2", Status.Up);
	Host h6 = new Host("h6", 8102, "remoteRack2", Status.Up);

	Host[] arr = {h1, h2, h3, h4, h5, h6};
	List<Host> hosts = Arrays.asList(arr);

	@Before
	public void beforeTest() {
		cpConfig.setLocalRack("localTestRack");
                cpConfig.setLocalDataCenter("localTestRack");
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
			hostnames.add(conn.getHost().getHostAddress());
		}

		verifyExactly(hostnames, "h1", "h2");

		// Now mark h1 and h2 both as "DOWN"
		poolStatus.get(h1).set(false);
		poolStatus.get(h2).set(false);
		hostnames.clear();

		for (int i=0; i<10; i++) {
			Connection<Integer> conn = selection.getConnection(testOperation, 1, TimeUnit.MILLISECONDS);
			hostnames.add(conn.getHost().getHostAddress());
		}

		verifyExactly(hostnames, "h3", "h4", "h5", "h6");

		// Now bring h1 back up
		poolStatus.get(h1).set(true);
		hostnames.clear();

		for (int i=0; i<10; i++) {
			Connection<Integer> conn = selection.getConnection(testOperation, 1, TimeUnit.MILLISECONDS);
			hostnames.add(conn.getHost().getHostAddress());
		}

		verifyExactly(hostnames, "h1");

		// Now bring h2 back up
		poolStatus.get(h2).set(true);
		hostnames.clear();
		for (int i=0; i<10; i++) {
			Connection<Integer> conn = selection.getConnection(testOperation, 1, TimeUnit.MILLISECONDS);
			hostnames.add(conn.getHost().getHostAddress());
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
			hostnames.add(conn.getHost().getHostAddress());
		}

		verifyExactly(hostnames, "h1", "h2");

		// Now mark h1 and h2 both as "DOWN"
		h1.setStatus(Status.Down); 
		h2.setStatus(Status.Down); 
		hostnames.clear();

		for (int i=0; i<10; i++) {
			Connection<Integer> conn = selection.getConnection(testOperation, 1, TimeUnit.MILLISECONDS);
			hostnames.add(conn.getHost().getHostAddress());
		}

		verifyExactly(hostnames, "h3", "h4", "h5", "h6");

		// Now bring h1 back up
		h1.setStatus(Status.Up); 
		hostnames.clear();

		for (int i=0; i<10; i++) {
			Connection<Integer> conn = selection.getConnection(testOperation, 1, TimeUnit.MILLISECONDS);
			hostnames.add(conn.getHost().getHostAddress());
		}

		System.out.println(" " + hostnames);
		verifyExactly(hostnames, "h1");

		// Now bring h2 back up
		h2.setStatus(Status.Up); 
		hostnames.clear();
		for (int i=0; i<10; i++) {
			Connection<Integer> conn = selection.getConnection(testOperation, 1, TimeUnit.MILLISECONDS);
			hostnames.add(conn.getHost().getHostAddress());
		}

		verifyExactly(hostnames, "h1", "h2");

        // Verify that failover metric has increased
        Assert.assertTrue(cpMonitor.getFailoverCount() > 0);
	}

	@Test
	public void testCrossRackFallback() throws Exception {

		HostSelectionWithFallback<Integer> selection = new HostSelectionWithFallback<Integer>(cpConfig, cpMonitor);
        RetryPolicy retry = new RetryNTimes(3, true);

		Map<Host, HostConnectionPool<Integer>> pools = new HashMap<Host, HostConnectionPool<Integer>>();

		for (Host host : hosts) {
			poolStatus.put(host, new AtomicBoolean(true));
			pools.put(host, getMockHostConnectionPool(host, poolStatus.get(host)));
		}

		selection.initWithHosts(pools);

		Set<String> hostnames = new HashSet<String>();

		for (int i = 0; i < 10; i++) {
			Connection<Integer> conn = selection.getConnectionUsingRetryPolicy(testOperation, 1, TimeUnit.MILLISECONDS,
                    retry);
			hostnames.add(conn.getHost().getHostAddress());
		}

		verifyExactly(hostnames, "h1", "h2");

        // Record a failure so that retry attempt is not 0 and get another connection
        retry.failure(new Exception("Unit Test Retry Exception"));
        Connection<Integer> conn = selection.getConnectionUsingRetryPolicy(testOperation, 1, TimeUnit.MILLISECONDS,
                retry);
        String fallbackHost = conn.getHost().getHostAddress();

        Assert.assertTrue(!fallbackHost.equals("h1") && !fallbackHost.equals("h2"));
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
        verifyAtLeastOnePresent(hostnames, "h4", "h6");
		verifyPresent(hostnames, "h5");

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
        verifyAtLeastOnePresent(hostnames, "h4", "h6");
        verifyPresent(hostnames, "h5");

		// Put Hosts H1,H2,H3,H4 as DOWN
		selection.initWithHosts(pools);
		h1.setStatus(Status.Down);
		h2.setStatus(Status.Down);
		h3.setStatus(Status.Down);
		h4.setStatus(Status.Down);

		hostnames = runConnectionsToRingTest(selection);
		verifyExactly(hostnames, "h5", "h6");
	}

    @Test
    public void testReplicationFactorOf3WithDupes() {
        cpConfig.setLoadBalancingStrategy(LoadBalancingStrategy.TokenAware);
        cpConfig.withTokenSupplier(getTokenMapSupplier());

        HostSelectionWithFallback<Integer> selection = new HostSelectionWithFallback<Integer>(cpConfig, cpMonitor);

        List<HostToken> hostTokens = Arrays.asList(
                new HostToken(1383429731L, new Host("host-1", -1, "localTestRack")), // Use -1 otherwise the port is opened which works
                new HostToken(2815085496L, new Host("host-2", -1, "localTestRack")),
                new HostToken(4246741261L, new Host("host-3", -1, "localTestRack" )),
                new HostToken(1383429731L, new Host("host-4", -1, "localTestRack")),
                new HostToken(2815085496L, new Host("host-5", -1, "localTestRack")),
                new HostToken(4246741261L, new Host("host-6", -1, "localTestRack")),
                new HostToken(1383429731L, new Host("host-7", -1, "localTestRack")),
                new HostToken(2815085496L, new Host("host-8", -1, "localTestRack")),
                new HostToken(4246741261L, new Host("host-9", -1, "localTestRack")),
                new HostToken(1383429731L, new Host("host-7", -1, "localTestRack")),
                new HostToken(2815085496L, new Host("host-8", -1, "localTestRack")),
                new HostToken(4246741261L, new Host("host-9", -1, "localTestRack")),
                new HostToken(1383429731L, new Host("host-7", -1, "localTestRack")),
                new HostToken(2815085496L, new Host("host-8", -1, "localTestRack")),
                new HostToken(4246741261L, new Host("host-9", -1, "localTestRack"))

        );

        int rf = selection.calculateReplicationFactor(hostTokens);

        Assert.assertEquals(3, rf);
    }

	@Test
    public void testReplicationFactorOf3() {
        cpConfig.setLocalRack("us-east-1c");
		cpConfig.setLoadBalancingStrategy(LoadBalancingStrategy.TokenAware);
		cpConfig.withTokenSupplier(getTokenMapSupplier());

		HostSelectionWithFallback<Integer> selection = new HostSelectionWithFallback<Integer>(cpConfig, cpMonitor);

        List<HostToken> hostTokens = Arrays.asList(
                new HostToken(1111L, new Host("host-1", -1, "localTestRack")),
                new HostToken(1111L, new Host("host-2", -1, "localTestRack")),
                new HostToken(1111L, new Host("host-3", -1, "localTestRack")),
                new HostToken(2222L, new Host("host-4", -1, "localTestRack")),
                new HostToken(2222L, new Host("host-5", -1, "localTestRack")),
                new HostToken(2222L, new Host("host-6", -1, "localTestRack"))
        );

		int rf = selection.calculateReplicationFactor(hostTokens);

        Assert.assertEquals(3, rf);
	}

    @Test
    public void testReplicationFactorOf2() {
        cpConfig.setLocalRack("us-east-1c");
        cpConfig.setLoadBalancingStrategy(LoadBalancingStrategy.TokenAware);
        cpConfig.withTokenSupplier(getTokenMapSupplier());

        HostSelectionWithFallback<Integer> selection = new HostSelectionWithFallback<Integer>(cpConfig, cpMonitor);

        List<HostToken> hostTokens = Arrays.asList(
                new HostToken(1111L, new Host("host-1", -1, "localTestRack")),
                new HostToken(1111L, new Host("host-2", -1, "localTestRack")),
                new HostToken(2222L, new Host("host-4", -1, "localTestRack")),
                new HostToken(2222L, new Host("host-5", -1, "localTestRack"))
        );

        int rf = selection.calculateReplicationFactor(hostTokens);

        Assert.assertEquals(2, rf);
    }

    @Test
    public void testReplicationFactorOf1() {
        cpConfig.setLoadBalancingStrategy(LoadBalancingStrategy.TokenAware);
        cpConfig.withTokenSupplier(getTokenMapSupplier());

        HostSelectionWithFallback<Integer> selection = new HostSelectionWithFallback<Integer>(cpConfig, cpMonitor);

        List<HostToken> hostTokens = Arrays.asList(
                new HostToken(1111L, h1),
                new HostToken(2222L, h2),
                new HostToken(3333L, h3),
                new HostToken(4444L, h4)
        );

        int rf = selection.calculateReplicationFactor(hostTokens);

        Assert.assertEquals(1, rf);
    }

    @Test(expected = RuntimeException.class)
    public void testIllegalReplicationFactor() {
        cpConfig.setLocalRack("us-east-1c");
        cpConfig.setLoadBalancingStrategy(LoadBalancingStrategy.TokenAware);
        cpConfig.withTokenSupplier(getTokenMapSupplier());

        HostSelectionWithFallback<Integer> selection = new HostSelectionWithFallback<Integer>(cpConfig, cpMonitor);

        List<HostToken> hostTokens = Arrays.asList(
                new HostToken(1111L, new Host("host-1", -1, "localTestRack")),
                new HostToken(1111L, new Host("host-2", -1, "localTestRack")),
                new HostToken(2222L, new Host("host-4", -1, "localTestRack"))
        );

        selection.calculateReplicationFactor(hostTokens);

    }

    @Test
	public void testReplicationFactorForMultiRegionCluster() {
        cpConfig.setLocalRack("us-east-1d");
        cpConfig.setLoadBalancingStrategy(LoadBalancingStrategy.TokenAware);
        cpConfig.withTokenSupplier(getTokenMapSupplier());

        HostSelectionWithFallback<Integer> selection = new HostSelectionWithFallback<Integer>(cpConfig, cpMonitor);

        List<HostToken> hostTokens = Arrays.asList(
                new HostToken(3530913378L, new Host("host-1", -1, "localTestRack")),
                new HostToken(1383429731L, new Host("host-1", -1, "localTestRack")),
                new HostToken(3530913378L, new Host("host-2", -1, "localTestRack")),
                new HostToken(1383429731L, new Host("host-2", -1, "localTestRack")),
                new HostToken(1383429731L, new Host("host-3", -1, "localTestRack")),
                new HostToken(3530913378L, new Host("host-3", -1, "localTestRack")),

                new HostToken(3530913378L, new Host("host-4", -1, "remoteRack1")),
                new HostToken(1383429731L, new Host("host-4", -1, "remoteRack1")),
                new HostToken(3530913378L, new Host("host-5", -1, "remoteRack1")),
                new HostToken(1383429731L, new Host("host-5", -1, "remoteRack1")),
                new HostToken(3530913378L, new Host("host-6", -1, "remoteRack1")),
                new HostToken(1383429731L, new Host("host-6", -1, "remoteRack1")),

                new HostToken(3530913378L, new Host("host-7", -1, "remoteRack2")),
                new HostToken(1383429731L, new Host("host-7", -1, "remoteRack2")),
                new HostToken(1383429731L, new Host("host-8", -1, "remoteRack2")),
                new HostToken(3530913378L, new Host("host-8", -1, "remoteRack1")),
                new HostToken(3530913378L, new Host("host-9", -1, "remoteRack2")),
                new HostToken(1383429731L, new Host("host-9", -1, "remoteRack2"))
        );

        int rf = selection.calculateReplicationFactor(hostTokens);
        Assert.assertEquals(3, rf);
    }
        
    @Test
    public void testChangingHashPartitioner() {
        cpConfig.setLoadBalancingStrategy(LoadBalancingStrategy.TokenAware);
        cpConfig.withHostSupplier(getHostSupplier());
        cpConfig.withTokenSupplier(getTokenMapSupplier());
        cpConfig.withHashPartitioner(getMockHashPartitioner(1000000000L));

        HostSelectionWithFallback<Integer> selection = new HostSelectionWithFallback<Integer>(cpConfig, cpMonitor);
        Map<Host, HostConnectionPool<Integer>> pools = new HashMap<Host, HostConnectionPool<Integer>>();

        for (Host host : hosts) {
            poolStatus.put(host, new AtomicBoolean(true));
            pools.put(host, getMockHostConnectionPool(host, poolStatus.get(host)));
        }

        selection.initWithHosts(pools); 

        Connection<Integer> connection = selection.getConnection(testOperation, 10, TimeUnit.MILLISECONDS);

        // Verify that h1 has been selected instead of h2
        assertEquals("h1", connection.getHost().getHostAddress());
    }     

	private Collection<String> runConnectionsToRingTest(HostSelectionWithFallback<Integer> selection) {

		Collection<Connection<Integer>> connections = selection.getConnectionsToRing(10, TimeUnit.MILLISECONDS);

		return CollectionUtils.transform(connections, new Transform<Connection<Integer>, String>() {
			@Override
			public String get(Connection<Integer> x) {
				return x.getHost().getHostAddress();
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

        tokenMap.put(h1, new HostToken(1383429731L, h1));
        tokenMap.put(h2, new HostToken(3530913377L, h2));
        tokenMap.put(h3, new HostToken(1383429731L, h3));
        tokenMap.put(h4, new HostToken(3530913377L, h4));
        tokenMap.put(h5, new HostToken(1383429731L, h5));
        tokenMap.put(h6, new HostToken(3530913377L, h6));

		return new TokenMapSupplier () {
            @Override
			public List<HostToken> getTokens(Set<Host> activeHosts) {
				return new ArrayList<HostToken>(tokenMap.values());
			}

			@Override
			public HostToken getTokenForHost(Host host, Set<Host> activeHosts) {
				return tokenMap.get(host);
			}

		};
	}
        
        private HostSupplier getHostSupplier() {
            return new HostSupplier () {
                @Override
                public List<Host> getHosts() {
                    return Arrays.asList(h1, h2, h3, h4);
                }
            };
        }           
        
        private HashPartitioner getMockHashPartitioner(final Long hash) {
            return new HashPartitioner() {
                @Override
                public Long hash(int key) {
                    return hash;
                }

                @Override
                public Long hash(long key) {
                    return hash;
                }

                @Override
                public Long hash(String key) {
                    return hash;
                }
                
				@Override
				public Long hash(byte[] key) {
					return hash;
				}

                @Override
                public HostToken getToken(Long keyHash) {
                    throw new RuntimeException("NotImplemented");
                }


            };
        }
}

