package com.netflix.dyno.memcache;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import net.spy.memcached.MemcachedNode;

import org.apache.commons.lang.NotImplementedException;

import com.netflix.dyno.connectionpool.impl.lb.CircularList;
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
	public Iterator<MemcachedNode> getNodeSequence(String key) {
		
		final AtomicInteger count = new AtomicInteger(remoteZoneList.getEntireList().size());
		
		return new Iterator<MemcachedNode>() {

			@Override
			public boolean hasNext() {
				return count.get() > 0;
			}

			@Override
			public MemcachedNode next() {
				count.decrementAndGet();
				return remoteZoneList.getNextElement();
			}

			@Override
			public void remove() {
				throw new NotImplementedException();
			}
		};
	}

}
