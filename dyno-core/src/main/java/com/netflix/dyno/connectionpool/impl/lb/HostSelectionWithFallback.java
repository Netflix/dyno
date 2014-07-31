package com.netflix.dyno.connectionpool.impl.lb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.exception.PoolExhaustedException;
import com.netflix.dyno.connectionpool.exception.PoolTimeoutException;
import com.netflix.dyno.connectionpool.impl.HostSelectionStrategy;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Predicate;

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

		localDC = System.getenv("EC2_AVAILABILITY_ZONE");
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
		} catch (PoolTimeoutException e) {
			lastEx = e;
			cpMonitor.incOperationFailure(null, e);
		} catch (PoolExhaustedException e) {
			lastEx = e;
			cpMonitor.incOperationFailure(null, e);
		}

		if (!isConnectionPoolActive(connection)) {
			
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
		TokenAwareSelector tSelector = (TokenAwareSelector) localSelector;
		return tSelector.getKeyHash(key);
	}
}
