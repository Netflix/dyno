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

import com.netflix.dyno.connectionpool.*;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration.LoadBalancingStrategy;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.exception.PoolExhaustedException;
import com.netflix.dyno.connectionpool.exception.PoolOfflineException;
import com.netflix.dyno.connectionpool.exception.PoolTimeoutException;
import com.netflix.dyno.connectionpool.impl.HostSelectionStrategy;
import com.netflix.dyno.connectionpool.impl.HostSelectionStrategy.HostSelectionStrategyFactory;
import com.netflix.dyno.connectionpool.impl.RunOnce;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Acts as a coordinator over multiple HostSelectionStrategy implementations, where each maps to a particular rack.
 * This class doesn't actually implement the logic (e.g Round Robin or Token Aware) to borrow the connections. It
 * relies on a local HostSelectionStrategy implementation and a collection of remote HostSelectionStrategy(s).
 * It gives preference to the "local" HostSelectionStrategy but if the local pool is offline or hosts are down etc, then
 * it falls back to the remote HostSelectionStrategy. Also it uses pure round robin for distributing load on the fall
 * back HostSelectionStrategy implementations for even distribution of load on the remote racks in the event of an
 * outage in the local rack.
 * <p>
 * Note that this class does not prefer any one remote HostSelectionStrategy over another.
 *
 * @author poberai
 * @author jcacciatore
 *
 * @param <CL>
 */

public class HostSelectionWithFallback<CL> {

	private static final Logger logger = LoggerFactory.getLogger(HostSelectionWithFallback.class);

    // Only used in calculating replication factor
    private final String localDataCenter;
	// tracks the local zone
	private final String localRack;
	// The selector for the local zone
	private final HostSelectionStrategy<CL> localSelector;
	// Track selectors for each remote zone
	private final ConcurrentHashMap<String, HostSelectionStrategy<CL>> remoteRackSelectors = new ConcurrentHashMap<String, HostSelectionStrategy<CL>>();

	private final ConcurrentHashMap<Host, HostToken> hostTokens = new ConcurrentHashMap<Host, HostToken>();

	private final TokenMapSupplier tokenSupplier;
	private final ConnectionPoolConfiguration cpConfig;
	private final ConnectionPoolMonitor cpMonitor;

    private final AtomicInteger replicationFactor = new AtomicInteger(-1);

    // Represents the *initial* topology from the token supplier. This does not affect selection of a host connection
    // pool for traffic. It only affects metrics such as failover/fallback
    private final AtomicReference<TokenPoolTopology> topology = new AtomicReference<>(null);

	// list of names of remote zones. Used for RoundRobin over remote zones when local zone host is down
	private final CircularList<String> remoteDCNames = new CircularList<String>(new ArrayList<String>());

	private final HostSelectionStrategyFactory<CL> selectorFactory;

	public HostSelectionWithFallback(ConnectionPoolConfiguration config, ConnectionPoolMonitor monitor) {

		cpMonitor = monitor;
		cpConfig = config;
		localRack = cpConfig.getLocalRack();
        localDataCenter = cpConfig.getLocalDataCenter();
		tokenSupplier = cpConfig.getTokenSupplier();

		selectorFactory = new DefaultSelectionFactory(cpConfig);
		localSelector = selectorFactory.vendPoolSelectionStrategy();

	}

	public Connection<CL> getConnection(BaseOperation<CL, ?> op, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {
		return getConnection(op, null, duration, unit, cpConfig.getRetryPolicyFactory().getRetryPolicy());
	}

	public Connection<CL> getConnectionUsingRetryPolicy(BaseOperation<CL, ?> op, int duration, TimeUnit unit, RetryPolicy retry) throws NoAvailableHostsException, PoolExhaustedException {
		return getConnection(op, null, duration, unit, retry);
	}

	private Connection<CL> getConnection(BaseOperation<CL, ?> op, Long token, int duration, TimeUnit unit, RetryPolicy retry)
			throws NoAvailableHostsException, PoolExhaustedException, PoolTimeoutException, PoolOfflineException {
		DynoConnectException lastEx = null;
		HostConnectionPool<CL> hostPool = null;

		if (retry.getAttemptCount() == 0 || (retry.getAttemptCount() > 0 && !retry.allowCrossZoneFallback())) {
			// By default zone affinity is enabled; if the local rack is not known at startup it is disabled
			if (cpConfig.localZoneAffinity()) {
				hostPool = getHostPoolForOperationOrTokenInLocalZone(op, token);
			} else {
				hostPool = getFallbackHostPool(op, token);
			}
		}

		if (hostPool != null) {
			try {
				// Note that if a PoolExhaustedException is thrown it is caught by the calling
				// ConnectionPoolImpl#executeXXX() method
				return hostPool.borrowConnection(duration, unit);
			} catch (PoolTimeoutException pte) {
				lastEx = pte;
				cpMonitor.incOperationFailure(null, pte);
			}
		}

		if (attemptFallback()) {
			if (topology.get().getTokensForRack(localRack) != null) {
				cpMonitor.incFailover(null, lastEx);
			}

			hostPool = getFallbackHostPool(op, token);

			if (hostPool != null) {
				return hostPool.borrowConnection(duration, unit);
			}
		}

		if (lastEx == null) {
			throw new PoolOfflineException(hostPool == null ? null : hostPool.getHost(), "host pool is offline and no Racks available for fallback");
		} else {
			throw lastEx;
		}
	}

	// Should be called when a connection is required on that particular zone with no fall backs what so ever
	private Connection<CL> getConnectionForTokenOnRackNoFallback(BaseOperation<CL, ?> op, Long token, String rack, int duration, TimeUnit unit, RetryPolicy retry)
            throws NoAvailableHostsException, PoolExhaustedException, PoolTimeoutException, PoolOfflineException {
        DynoConnectException lastEx = null;

        // find the selector for that rack,
		HostSelectionStrategy<CL> selector = findSelectorForRack(rack);
		// get the host using that selector
		HostConnectionPool<CL>  hostPool = selector.getPoolForToken(token);

        if (hostPool != null) {
            try {
                // Note that if a PoolExhaustedException is thrown it is caught by the calling
                // ConnectionPoolImpl#executeXXX() method
                return hostPool.borrowConnection(duration, unit);
            } catch (PoolTimeoutException pte) {
                lastEx = pte;
                cpMonitor.incOperationFailure(null, pte);
            }
        }

        if (lastEx == null) {
            throw new PoolOfflineException(hostPool == null ? null : hostPool.getHost(), "host pool is offline and we are forcing no fallback");
        } else {
            throw lastEx;
        }
    }

    private HostConnectionPool<CL> getHostPoolForOperationOrTokenInLocalZone(BaseOperation<CL, ?> op, Long token) {
        HostConnectionPool<CL> hostPool;
        try {
            if (!localSelector.isEmpty()) {
                hostPool = (op != null) ? localSelector.getPoolForOperation(op, cpConfig.getHashtag()) : localSelector.getPoolForToken(token);
                if (isConnectionPoolActive(hostPool)) {
                    return hostPool;
                }
			}

        } catch (NoAvailableHostsException e) {
            cpMonitor.incOperationFailure(null, e);
        }

        return null;
    }

    private boolean attemptFallback() {
        return cpConfig.getMaxFailoverCount() > 0 &&
                (cpConfig.localZoneAffinity() && remoteDCNames.getEntireList().size() > 0) ||
                (!cpConfig.localZoneAffinity() && !localSelector.isEmpty());
    }

    private HostConnectionPool<CL> getFallbackHostPool(BaseOperation<CL, ?> op, Long token) {
        int numRemotes = remoteDCNames.getEntireList().size();
		if (numRemotes == 0) {
			throw new NoAvailableHostsException("Could not find any remote Racks for fallback");
		}

		int numTries = Math.min(numRemotes, cpConfig.getMaxFailoverCount());

		DynoException lastEx = null;

		while ((numTries > 0)) {

			numTries--;
			String remoteDC = remoteDCNames.getNextElement();
			HostSelectionStrategy<CL> remoteDCSelector = remoteRackSelectors.get(remoteDC);

			try {

				HostConnectionPool<CL> fallbackHostPool =
						(op != null) ? remoteDCSelector.getPoolForOperation(op,cpConfig.getHashtag()) : remoteDCSelector.getPoolForToken(token);

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
			throw new NoAvailableHostsException("Local rack host offline and could not find any remote hosts for fallback connection");
		}
	}

	public Collection<Connection<CL>> getConnectionsToRing(TokenRackMapper tokenRackMapper, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {
		String targetRack = localRack;
		if (targetRack == null) {
			// get tokens for random rack
			targetRack = topology.get().getRandomRack();
		}
		final Set<Long> tokens = topology.get().getTokenHostsForRack(targetRack).keySet();
		DynoConnectException lastEx = null;

		final List<Connection<CL>> connections = new ArrayList<>();

		for (Long token : tokens) {
			try {
				// Cursor has a map of token to rack which indicates an affinity to a zone for that token.
				// This is valid in case of an iterator based query like scan.
				// Try to use that same rack if it is specified.
				String rack = null;
				if (tokenRackMapper != null)
					rack = tokenRackMapper.getRackForToken(token);
				if (rack != null) {
					connections.add(getConnectionForTokenOnRackNoFallback(null, token, rack, duration, unit, new RunOnce()));
				} else {
					Connection<CL> c = getConnection(null, token, duration, unit, new RunOnce());
					if (tokenRackMapper != null) {
						tokenRackMapper.setRackForToken(token, c.getHost().getRack());
					}
					connections.add(c);
				}
			} catch (DynoConnectException e) {
				logger.warn("Failed to get connection when getting all connections from ring", e.getMessage());
				lastEx = e;
				break;
			}
		}

		if (lastEx != null) {
			// Return all previously borrowed connection to avoid any connection leaks
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


	private HostSelectionStrategy<CL> findSelectorForRack(String rack) {
		if (localRack == null) {
			return localSelector;
		}

		if (localRack.equals(rack)) {
			return localSelector;
		}

		HostSelectionStrategy<CL> remoteSelector = remoteRackSelectors.get(rack);
		return remoteSelector;
	}

	private boolean isConnectionPoolActive(HostConnectionPool<CL> hPool) {
		if (hPool == null) {
			return false;
		}
		Host host = hPool.getHost();
		return host.isUp() && hPool.isActive();
	}

	private Map<HostToken, HostConnectionPool<CL>> getHostPoolsForRack(final Map<HostToken, HostConnectionPool<CL>> map, final String rack) {

		Map<HostToken, HostConnectionPool<CL>> dcPools =
				CollectionUtils.filterKeys(map, new Predicate<HostToken>() {

					@Override
					public boolean apply(HostToken x) {
						if (localRack == null) {
							return true;
						}
						return rack.equals(x.getHost().getRack());
					}
				});
		return dcPools;
	}

	/**
	 * hPools comes from discovery.
	 * @param hPools
	 */
	public void initWithHosts(Map<Host, HostConnectionPool<CL>> hPools) {

		// Get the list of tokens for these hosts
		//tokenSupplier.initWithHosts(hPools.keySet());
		List<HostToken> allHostTokens = tokenSupplier.getTokens(hPools.keySet());
		Map<HostToken, HostConnectionPool<CL>> tokenPoolMap = new HashMap<HostToken, HostConnectionPool<CL>>();

		// Update inner state with the host tokens.
		for (HostToken hToken : allHostTokens) {
			hostTokens.put(hToken.getHost(), hToken);
			tokenPoolMap.put(hToken, hPools.get(hToken.getHost()));
		}

		// Initialize Local selector
		Map<HostToken, HostConnectionPool<CL>> localPools = getHostPoolsForRack(tokenPoolMap, localRack);
		localSelector.initWithHosts(localPools);
		if (localSelector.isTokenAware()) {
		   replicationFactor.set(calculateReplicationFactorForDC(allHostTokens, cpConfig.getLocalDataCenter()));
		}

		// Initialize Remote selectors
		Set<String> remoteRacks = new HashSet<String>();
		for (Host host : hPools.keySet()) {
			String rack = host.getRack();
			if(localRack == null && rack != null) {
				remoteRacks.add(rack);
			}
			else if (localRack != null && !localRack.isEmpty() && rack != null && !rack.isEmpty() && !localRack.equals(rack)) {
				remoteRacks.add(rack);
			}
		}

		for (String rack : remoteRacks) {
			Map<HostToken, HostConnectionPool<CL>> dcPools = getHostPoolsForRack(tokenPoolMap, rack);
			HostSelectionStrategy<CL> remoteSelector = selectorFactory.vendPoolSelectionStrategy();
			remoteSelector.initWithHosts(dcPools);
			remoteRackSelectors.put(rack, remoteSelector);
		}

		remoteDCNames.swapWithList(remoteRackSelectors.keySet());

        topology.set(createTokenPoolTopology(allHostTokens));
	}

	int calculateReplicationFactor(List<HostToken> allHostTokens) {
		return calculateReplicationFactorForDC(allHostTokens, null);
	}

	int calculateReplicationFactorForDC(List<HostToken> allHostTokens, String dataCenter) {
		Map<Long, Integer> groups = new HashMap<>();

		Set<HostToken> uniqueHostTokens = new HashSet<>(allHostTokens);
		if (dataCenter == null) {
		    if(localRack != null) {
				dataCenter = localRack.substring(0, localRack.length() - 1);
			}
		}

		for (HostToken hostToken: uniqueHostTokens) {
		    if(dataCenter == null || hostToken.getHost().getRack().contains(dataCenter)) {
				Long token = hostToken.getToken();
				if (groups.containsKey(token)) {
					int current = groups.get(token);
					groups.put(token, current + 1);
				} else {
					groups.put(token, 1);
				}
			}
		}

		Set<Integer> uniqueCounts = new HashSet<>(groups.values());

		if (uniqueCounts.size() > 1) {
			throw new RuntimeException("Invalid configuration - replication factor cannot be asymmetric");
		}

		int rf = uniqueCounts.toArray(new Integer[uniqueCounts.size()])[0];

		if (rf > 3) {
			logger.warn("Replication Factor is high: " + uniqueHostTokens);
		}

		return rf;

	}

    public void addHost(Host host, HostConnectionPool<CL> hostPool) {

		HostToken hostToken = tokenSupplier.getTokenForHost(host, hostTokens.keySet());
		if (hostToken == null) {
			throw new DynoConnectException("Could not find host token for host: " + host);
		}

		hostTokens.put(hostToken.getHost(), hostToken);

		HostSelectionStrategy<CL> selector = findSelectorForRack(host.getRack());
		if (selector != null) {
			selector.addHostPool(hostToken, hostPool);
		}
		topology.get().addHostToken(hostToken.getHost().getRack(), hostToken.getToken(), hostToken.getHost());
	}

	public void removeHost(Host host) {

		HostToken hostToken = hostTokens.remove(host);
		if (hostToken != null) {
			HostSelectionStrategy<CL> selector = findSelectorForRack(host.getRack());
			if (selector != null) {
				selector.removeHostPool(hostToken);
			}
			topology.get().removeHost(hostToken.getHost().getRack(), hostToken.getToken(), hostToken.getHost());
		}
	}

	private class DefaultSelectionFactory implements HostSelectionStrategyFactory<CL> {

		private final LoadBalancingStrategy lbStrategy;
                private final HashPartitioner hashPartitioner;

		private DefaultSelectionFactory(ConnectionPoolConfiguration config) {
			lbStrategy = config.getLoadBalancingStrategy();
			hashPartitioner = config.getHashPartitioner();
		}

		@Override
		public HostSelectionStrategy<CL> vendPoolSelectionStrategy() {

			switch (lbStrategy) {
			case RoundRobin:
				return new RoundRobinSelection<CL>();
			case TokenAware:
				return hashPartitioner != null
                                        ? new TokenAwareSelection<CL>(hashPartitioner)
                                        : new TokenAwareSelection<CL>();
			default :
				throw new RuntimeException("LoadBalancing strategy not supported! " + cpConfig.getLoadBalancingStrategy().name());
			}
		}
	}

	private void updateTokenPoolTopology(TokenPoolTopology topology) {
		if (localRack != null) {
			addTokens(topology, localRack, localSelector);
		}
		for (String remoteRack : remoteRackSelectors.keySet()) {
			addTokens(topology, remoteRack, remoteRackSelectors.get(remoteRack));
		}
	}

	public TokenPoolTopology createTokenPoolTopology(List<HostToken> allHostTokens) {
		TokenPoolTopology topology = new TokenPoolTopology(replicationFactor.get());
		for (HostToken hostToken : allHostTokens) {
			String rack = hostToken.getHost().getRack();
			topology.addHostToken(rack, hostToken.getToken(), hostToken.getHost());
		}
		updateTokenPoolTopology(topology);
        return topology;

    }

	public TokenPoolTopology getTokenPoolTopology() {
		TokenPoolTopology topology = new TokenPoolTopology(replicationFactor.get());
		updateTokenPoolTopology(topology);
		return topology;

	}

	private void addTokens(TokenPoolTopology topology, String rack, HostSelectionStrategy<CL> selectionStrategy) {

		Collection<HostConnectionPool<CL>> pools = selectionStrategy.getOrderedHostPools();
		for (HostConnectionPool<CL> pool : pools) {
			if (pool == null) {
				continue;
			}
			HostToken hToken = hostTokens.get(pool.getHost());
			if (hToken == null) {
				continue;
			}
			topology.addToken(rack, hToken.getToken(), pool);
		}
	}

    public Long getTokenForKey(String key) {
        return localSelector.getTokenForKey(key).getToken();
    }

	@Override
	public String toString() {
		return "HostSelectionWithFallback{" +
				"localDataCenter='" + localDataCenter + '\'' +
				", localRack='" + localRack + '\'' +
				", localSelector=" + localSelector +
				", remoteDCSelectors=" + remoteRackSelectors +
				", hostTokens=" + hostTokens +
				", tokenSupplier=" + tokenSupplier +
				", cpConfig=" + cpConfig +
				", cpMonitor=" + cpMonitor +
				", replicationFactor=" + replicationFactor +
				", topology=" + topology +
				", remoteDCNames=" + remoteDCNames +
				", selectorFactory=" + selectorFactory +
				'}';
	}
}
