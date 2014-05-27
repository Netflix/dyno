package com.netflix.dyno.connectionpool.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.HostToken;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.exception.PoolExhaustedException;
import com.netflix.dyno.connectionpool.impl.hash.BinarySearchTokenMapper;
import com.netflix.dyno.connectionpool.impl.hash.Murmur1Hash;
import com.netflix.dyno.connectionpool.impl.hash.Murmur1HashPartitioner;

/**
 * Class that encapsulates a selection strategy when load balancing requests to dyno {@link HostConnectionPool}s
 * It uses the {@link Murmur1Hash} to compute the hash of the key for the {@link Operation} and then employs 
 * a {@link BinarySearchTokenMapper} to quickly find the host that is responsible for the key. 
 * 
 * Note that if the selected host is down, it then performs the same search on the remote zones. 
 * If no hosts are found, it throws a {@link NoAvailableHostsException}
 * 
 * @author poberai
 *
 * @param <CL>
 */
public class TokenAwareSelection<CL> implements HostSelectionStrategy<CL> {

	// The map of all Host -> HostConnectionPool 
	private final Map<Host, HostConnectionPool<CL>> hostPools;
	
	// Binary search over the token hosts
	private final BinarySearchTokenMapper tokenMapper = new BinarySearchTokenMapper(new Murmur1HashPartitioner());

	// list of hosts and their tokens for the local zone
	private final List<HostToken> localHostTokens = new ArrayList<HostToken>();
	// list of names of remote zones. Used for RoundRobin over remote zones when local zone host is down
	private final CircularList<String> remoteZoneNames = new CircularList<String>(new ArrayList<String>());
	// Mapping of all Host and token lists for all remote zones 
	private final ConcurrentHashMap<String, List<HostToken>> remoteZoneTokens = new ConcurrentHashMap<String, List<HostToken>>();

	/**
	 * Constructor
	 * @param hostSupplier
	 * @param hostPools
	 */
	public TokenAwareSelection(HostSupplier hostSupplier, Map<Host, HostConnectionPool<CL>> hostPools) {
		
		this.hostPools = hostPools;
		
		List<HostToken> allHostTokens = new TokenMapSupplierImpl(hostSupplier).getTokens();
		
		String localZone = System.getenv("EC2_AVAILABILITY_ZONE");

		if (localZone == null || localZone.isEmpty()) {
			localHostTokens.addAll(allHostTokens);
			
		} else {

			for (HostToken hToken : allHostTokens) {
				
				Host host = hToken.getHost();
				String dc = host.getDC();
				
				if (isLocalZoneHost(localZone, dc)) {
					localHostTokens.add(hToken);
				} else {
					
					if (dc == null || dc.isEmpty()) {
						continue;
					}
					
					List<HostToken> remoteZone = remoteZoneTokens.get(dc);
					if (remoteZone == null) {
						remoteZone = new ArrayList<HostToken>();
						remoteZoneTokens.put(dc, remoteZone);
					}
					
					remoteZone.add(hToken);
				}
			}
			remoteZoneNames.swapWithList(remoteZoneTokens.keySet());
		}
	}
	
	private boolean isLocalZoneHost(String localZone, String dc) {
		if (localZone == null) {
			return true;
		}
		return (localZone.equals(dc));
	}
	
	@Override
	public Connection<CL> getConnection(BaseOperation<CL, ?> op, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {
		
		String key = op.getKey();
		Long keyHash = tokenMapper.hash(key);
		
		HostToken hToken = tokenMapper.getToken(localHostTokens, keyHash);

		if (!hToken.getHost().isUp()) {
			
			String remoteZone = remoteZoneNames.getNextElement();
			
			if (remoteZone != null) {
				
				List<HostToken> remoteHostTokens = remoteZoneTokens.get(remoteZone);
				if (remoteHostTokens.size() > 0) {
					hToken = tokenMapper.getToken(remoteHostTokens, keyHash);
				} else {
					throw new NoAvailableHostsException("Cannot find host and local zone host is down");
				}
			} else {
				throw new NoAvailableHostsException("Local zone host is down and no remote zone hosts for fallback");
			}
		}
		
		HostConnectionPool<CL> hPool = hostPools.get(hToken.getHost());
		if (hPool == null) {
			throw new NoAvailableHostsException("");
		}

		return hPool.borrowConnection(duration, unit);
	}

	@Override
	public void addHost(Host host, HostConnectionPool<CL> hostPool) {
		// do nothing, we can't yet deal with token map changes
	}

	@Override
	public void removeHost(Host host, HostConnectionPool<CL> hostPool) {
		// do nothing, we can't yet deal with token map changes
	}
}
