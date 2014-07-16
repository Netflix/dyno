package com.netflix.dyno.connectionpool.impl.lb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.exception.PoolExhaustedException;
import com.netflix.dyno.connectionpool.impl.HostSelectionStrategy;
import com.netflix.dyno.connectionpool.impl.hash.BinarySearchTokenMapper;
import com.netflix.dyno.connectionpool.impl.hash.Murmur1HashPartitioner;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Predicate;

public class TokenAwareSelection<CL> implements HostSelectionStrategy<CL> {

	private TokenMapSupplierImpl tokenSupplier; 
	private final BinarySearchTokenMapper tokenMapper;
	private String myDC = null;

	private final ConcurrentHashMap<Host, HostConnectionPool<CL>> hostPools = new ConcurrentHashMap<Host, HostConnectionPool<CL>>();
	
	private static final String KeyHash = "KeyHash";
	
	public TokenAwareSelection() {
		this.tokenMapper = new BinarySearchTokenMapper(new Murmur1HashPartitioner());
	}
	
	@Override
	public void initWithHosts(Map<Host, HostConnectionPool<CL>> hPools) {
		
		Host host = hPools.keySet().iterator().next();
		myDC = host != null ? host.getDC() : null;
		
		hostPools.putAll(hPools);
		
		List<Host> hosts = new ArrayList<Host>(hostPools.keySet());
		
		this.tokenSupplier =  new TokenMapSupplierImpl(hosts);
		List<HostToken> allHostTokens = tokenSupplier.getTokens();
		
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
}
