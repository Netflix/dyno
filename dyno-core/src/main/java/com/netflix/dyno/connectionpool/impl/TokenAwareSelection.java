package com.netflix.dyno.connectionpool.impl;

import java.util.ArrayList;
import java.util.HashMap;
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
	private final ConcurrentHashMap<Host, HostConnectionPool<CL>> hostPools = new ConcurrentHashMap<Host, HostConnectionPool<CL>>();

	// tracks the local zone
	private final String localZone;
	// Binary search over the local hosts for the token
	private final ZoneTokenMapper localZoneMapper;

	// list of names of remote zones. Used for RoundRobin over remote zones when local zone host is down
	private final CircularList<String> remoteZoneNames = new CircularList<String>(new ArrayList<String>());
	// Mapping of all Host and token lists for all remote zones 
	private final ConcurrentHashMap<String, ZoneTokenMapper> remoteZoneMappers = new ConcurrentHashMap<String, ZoneTokenMapper>();

	private final TokenMapSupplierImpl tokenMapSupplier; 
	/**
	 * Constructor
	 * @param hostSupplier
	 * @param hostPools
	 */
	public TokenAwareSelection(HostSupplier hostSupplier, Map<Host, HostConnectionPool<CL>> hPools) {
		
		this.hostPools.putAll(hPools);
		
		tokenMapSupplier =  new TokenMapSupplierImpl(hostSupplier);
		List<HostToken> allHostTokens = tokenMapSupplier.getTokens();
		
		localZone = System.getenv("EC2_AVAILABILITY_ZONE");

		List<HostToken> localZoneTokens = new ArrayList<HostToken>();
		Map<String, List<HostToken>> remoteZoneTokens = new HashMap<String, List<HostToken>>();
		
		if (localZone == null || localZone.isEmpty()) {
			localZoneTokens.addAll(allHostTokens);
			
		} else {

			for (HostToken hToken : allHostTokens) {
				
				Host host = hToken.getHost();
				String dc = host.getDC();
				
				if (isLocalZoneHost(host)) {
					localZoneTokens.add(hToken);
					
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
		}
			
		localZoneMapper = new ZoneTokenMapper(localZone, localZoneTokens);
		remoteZoneNames.swapWithList(remoteZoneTokens.keySet());
		for (String remoteZone : remoteZoneTokens.keySet()) {
			remoteZoneMappers.put(remoteZone, new ZoneTokenMapper(remoteZone, remoteZoneTokens.get(remoteZone)));
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
	
	@Override
	public Connection<CL> getConnection(BaseOperation<CL, ?> op, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {
		
		String key = op.getKey();
		
		HostToken hToken = localZoneMapper.getToken(key);

		if (!hToken.getHost().isUp()) {
			
			// Fallback to remote zone
			
			String remoteZone = remoteZoneNames.getNextElement();
			
			if (remoteZone != null) {
				
				ZoneTokenMapper remoteTokenMapper = remoteZoneMappers.get(remoteZone);
				if (remoteTokenMapper.isEmpty()) {
					throw new NoAvailableHostsException("Cannot find host and local zone host is down");
				} else {
					hToken = remoteTokenMapper.getToken(key);
				}
			} else {
				throw new NoAvailableHostsException("Local zone host is down and no remote zone hosts for fallback");
			}
		}
		
		HostConnectionPool<CL> hPool = hostPools.get(hToken.getHost());
		if (hPool == null) {
			throw new NoAvailableHostsException("Cannot find host for specified key");
		}
		
		return hPool.borrowConnection(duration, unit);
	}

	@Override
	public void addHost(Host host, HostConnectionPool<CL> hostPool) {
		
		HostConnectionPool<CL> oldPool = hostPools.putIfAbsent(host, hostPool);
		if (oldPool == null) {
			// this is really a new host that we are seeing.
			HostToken hostToken = tokenMapSupplier.getTokenForHost(host);
			
			if (isLocalZoneHost(host)) {
				localZoneMapper.addHost(hostToken);
			} else {
				ZoneTokenMapper remoteZoneMapper = remoteZoneMappers.get(host.getDC());
				if (remoteZoneMapper != null) {
					remoteZoneMapper.addHost(hostToken);
				}
			}
		}
	}

	@Override
	public void removeHost(Host host, HostConnectionPool<CL> hostPool) {
		// do nothing, we can't yet deal with token map changes
	}
	
	private class ZoneTokenMapper {
		
		private final String zone; 
		private final BinarySearchTokenMapper mapper;
		
		private ZoneTokenMapper(String theZone, List<HostToken> tokensForZone) {
			this.zone = theZone;
			this.mapper = new BinarySearchTokenMapper(new Murmur1HashPartitioner());
			this.mapper.initSearchMecahnism(tokensForZone);
		}
	
		private HostToken getToken(String key) {
			Long keyHash = mapper.hash(key);
			return mapper.getToken(keyHash);
		}
		
		private void addHost(HostToken hostToken) {
			this.mapper.addHostToken(hostToken);
		}
	
		private boolean isEmpty() {
			return this.mapper.isEmpty();
		}

		@Override
		public int hashCode() {
			return zone.hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			ZoneTokenMapper other = (ZoneTokenMapper) obj;
			return this.zone.equals(other.zone);
		}
		
		public String toString() {
			return "ZoneTokenMapper: " + this.zone;
		}
	}
}
