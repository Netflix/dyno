package com.netflix.dyno.memcache;

import net.spy.memcached.MemcachedNode;

import com.netflix.dyno.connectionpool.impl.CircularList;
import com.netflix.dyno.memcache.SpyMemcachedConnectionFactory.InstrumentedLocator;

public class RoundRobinLocator extends InstrumentedLocator {
	
	private final CircularList<MemcachedNode> localZoneList; 
	private final CircularList<MemcachedNode> remoteZoneList; 

	public RoundRobinLocator(SpyMemcachedConnectionFactory cFactory) {
		super(cFactory);
		localZoneList = new CircularList<MemcachedNode>(cFactory.getAllLocalZoneNodes());
		remoteZoneList = new CircularList<MemcachedNode>(cFactory.getAllRemoteZoneNodes());
	}

	@Override
	public MemcachedNode getPrimaryNode(String key) {
		return localZoneList.getNextElement();
	}

	@Override
	public CircularList<MemcachedNode> getNodeSequence(String key) {
		return remoteZoneList;
	}

}
