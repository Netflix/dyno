package com.netflix.dyno.connectionpool.impl.lb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.exception.PoolExhaustedException;
import com.netflix.dyno.connectionpool.impl.HostSelectionStrategy;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.MapEntryTransform;

public class SelectionWIthRemoteZoneFallback<CL> implements HostSelectionStrategy<CL> {

	private static final Logger Logger = LoggerFactory.getLogger(SelectionWIthRemoteZoneFallback.class);
	
	// tracks the local zone
	private final String localZone;
	// The selector for the local zone
	private final SingleDCSelector localSelector;
	// Track selectors for each remote DC
	private final ConcurrentHashMap<String, SingleDCSelector> remoteDCSelectors = new ConcurrentHashMap<String, SingleDCSelector>();
	// The map of all Host -> HostConnectionPool 
	private final ConcurrentHashMap<Host, HostConnectionPool<CL>> hostPools = new ConcurrentHashMap<Host, HostConnectionPool<CL>>();
	
	private final ConnectionPoolMonitor cpMonitor; 

	// list of names of remote zones. Used for RoundRobin over remote zones when local zone host is down
	private final CircularList<String> remoteDCNames = new CircularList<String>(new ArrayList<String>());

	private final SingleDCSelectorFactory selectorFactory;

	public interface SingleDCSelector { 

		public void init(String zone, List<Host> hosts);

		public Host getHostForKey(String key);

		public boolean isEmpty();

		public void addHost(Host host);

		public void removeHost(Host host);
	}

	public interface SingleDCSelectorFactory {

		public SingleDCSelector vendSelector(); 
	}


	public SelectionWIthRemoteZoneFallback(ConcurrentHashMap<Host, HostConnectionPool<CL>> hPools, SingleDCSelectorFactory sFactory, ConnectionPoolMonitor monitor) {

		localZone = System.getenv("EC2_AVAILABILITY_ZONE");
		hostPools.putAll(hPools);
		selectorFactory = sFactory;
		cpMonitor = monitor;
		
		Set<Host> allHosts = hPools.keySet();

		List<Host> localHosts = new ArrayList<Host>();
		Map<String, List<Host>> remoteDCHosts = new HashMap<String, List<Host>>();

		for (Host host : allHosts) {

			String dc = host.getDC();

			if (isLocalZoneHost(host)) {
				localHosts.add(host);

			} else {

				if (dc == null || dc.isEmpty()) {
					continue;
				}

				List<Host> remoteDCHostList = remoteDCHosts.get(dc);
				if (remoteDCHostList == null) {
					remoteDCHostList = new ArrayList<Host>();
					remoteDCHosts.put(dc, remoteDCHostList);
				}

				remoteDCHostList.add(host);
			}
		}

		localSelector = selectorFactory.vendSelector();
		localSelector.init(localZone, localHosts);

		CollectionUtils.transform(remoteDCHosts, remoteDCSelectors, new MapEntryTransform<String, List<Host>, SingleDCSelector>() {

			@Override
			public SingleDCSelector get(String zone, List<Host> hosts) {
				SingleDCSelector selector = selectorFactory.vendSelector();
				selector.init(zone, hosts);
				return selector;
			}
		});
		
		remoteDCNames.swapWithList(remoteDCSelectors.keySet());
	}

	@Override
	public Connection<CL> getConnection(BaseOperation<CL, ?> op, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {

		String key = op.getKey();
		Host host = localSelector.getHostForKey(key);

		if (!isHostAndHostPoolActive(host)) {
			
			cpMonitor.incFailover(host, null);
			return getFallbackConnection(op, duration, unit);
		}

		HostConnectionPool<CL> hPool = hostPools.get(host);
		return hPool.borrowConnection(duration, unit);
	}

	public Long getKeyHash(String key) {
		TokenAwareSelector tSelector = (TokenAwareSelector) localSelector;
		return tSelector.getKeyHash(key);
	}
	
	@Override
	public Connection<CL> getFallbackConnection(BaseOperation<CL, ?> op, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {

		String key = op.getKey();

		Host remoteHost = getActiveFallbackHost(key);
		if (remoteHost == null) {
			throw new NoAvailableHostsException("Local zone host is down and no remote zone hosts for fallback");
		} else {
			HostConnectionPool<CL> hPool = hostPools.get(remoteHost);
			return hPool.borrowConnection(duration, unit);
		}
	}
	
	@Override
	public void addHost(Host host, HostConnectionPool<CL> hostPool) {
		hostPools.put(host, hostPool);
		String dc = host.getDC();
		if (dc != null) {
			remoteDCSelectors.get(dc).addHost(host);
		}
	}

	@Override
	public void removeHost(Host host, HostConnectionPool<CL> hostPool) {
		hostPools.put(host, hostPool);
		String dc = host.getDC();
		if (dc != null) {
			remoteDCSelectors.get(dc).removeHost(host);
		}
	}

	private boolean isLocalZoneHost(Host host) {
		if (localZone == null) {
			return true;
		}
		String dc = host.getDC();
		if (dc == null || dc.isEmpty()) {
			return true;
		}
		return (localZone.equals(dc));
	}



	private boolean isHostAndHostPoolActive(Host host) {
		if (!host.isUp()) {
			if (Logger.isDebugEnabled()) {
				Logger.debug("Host: " + host.getHostName() + " " + host.getStatus());
			}
			return false;
		} else {
			HostConnectionPool<CL> hPool = hostPools.get(host);
			if (Logger.isDebugEnabled()) {
				Logger.debug("Host: " + host.getHostName() + " " + host.getStatus() + " active: " + hPool.isActive());
			}
			return hPool != null && hPool.isActive();
		}
	}

	private Host getActiveFallbackHost(String key) {

		int numRemotes = remoteDCNames.getEntireList().size();
		Host host = null; 

		while (numRemotes > 0) {

			numRemotes--;
			String dc = remoteDCNames.getNextElement();

			if (dc != null) {
				SingleDCSelector remoteDCSelector = remoteDCSelectors.get(dc);
				if (remoteDCSelector.isEmpty()) {
					continue;
				} else {
					Host remoteHost = remoteDCSelector.getHostForKey(key);
					if (isHostAndHostPoolActive(remoteHost)) {
						host = remoteHost;
						break;
					}
				}
			} else {
				break;
			}
		}

		return host;
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
}
