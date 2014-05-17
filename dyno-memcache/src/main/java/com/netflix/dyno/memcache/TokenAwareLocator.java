package com.netflix.dyno.memcache;

import java.util.ArrayList;
import java.util.List;

import net.spy.memcached.MemcachedNode;

import com.netflix.dyno.connectionpool.HostToken;
import com.netflix.dyno.connectionpool.impl.CircularList;
import com.netflix.dyno.connectionpool.impl.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.hash.BinarySearchTokenMapper;
import com.netflix.dyno.connectionpool.impl.hash.Murmur1HashPartitioner;
import com.netflix.dyno.memcache.SpyMemcachedConnectionFactory.InstrumentedLocator;

public class TokenAwareLocator extends InstrumentedLocator {

	private final SpyMemcachedConnectionFactory connFactory;
	
	private final BinarySearchTokenMapper tokenMapper = new BinarySearchTokenMapper(new Murmur1HashPartitioner());
	private final List<HostToken> hostTokens = new ArrayList<HostToken>();

	private final CircularList<MemcachedNode> remoteZoneNodes;
	
	public TokenAwareLocator(SpyMemcachedConnectionFactory cFactory) {

		super(cFactory);
		
		this.connFactory = cFactory;
		hostTokens.addAll(new TokenMapSupplier().getTokens());
		remoteZoneNodes = new CircularList<MemcachedNode>(cFactory.getAllRemoteZoneNodes());
	}

	@Override
	public MemcachedNode getPrimaryNode(String key) {
		
		Long keyHash = tokenMapper.hash(key);
		HostToken hostToken = tokenMapper.getToken(hostTokens, keyHash);
		return connFactory.getMemcachedNodeForHost(hostToken.getHost());
	}

	@Override
	public CircularList<MemcachedNode> getNodeSequence(String key) {
		return remoteZoneNodes;
	}


}




