package com.netflix.dyno.memcache;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import net.spy.memcached.MemcachedNode;

import org.apache.commons.lang.NotImplementedException;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.impl.hash.BinarySearchTokenMapper;
import com.netflix.dyno.connectionpool.impl.hash.Murmur1HashPartitioner;
import com.netflix.dyno.connectionpool.impl.lb.CircularList;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.connectionpool.impl.lb.TokenMapSupplierImpl;
import com.netflix.dyno.memcache.SpyMemcachedConnectionFactory.InstrumentedLocator;

public class TokenAwareLocator extends InstrumentedLocator {

	private final SpyMemcachedConnectionFactory connFactory;
	
	private final BinarySearchTokenMapper tokenMapper = new BinarySearchTokenMapper(new Murmur1HashPartitioner());
	private final List<HostToken> localHostTokens = new ArrayList<HostToken>();

	private final CircularList<String> remoteZoneNames = new CircularList<String>(new ArrayList<String>());
	private final ConcurrentHashMap<String, List<HostToken>> remoteZoneTokens = new ConcurrentHashMap<String, List<HostToken>>();
	
	public TokenAwareLocator(SpyMemcachedConnectionFactory cFactory) {

		super(cFactory);
		
		this.connFactory = cFactory;
		
		TokenMapSupplierImpl tokenSupplier = new TokenMapSupplierImpl();
		tokenSupplier.initWithHosts(cFactory.getCPConfig().getHostSupplier().getHosts());
		List<HostToken> allHostTokens = tokenSupplier.getTokens();
		
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
	public MemcachedNode getPrimaryNode(String key) {
		return getNodeFromHashRing(key, localHostTokens);
	}

	@Override
	public Iterator<MemcachedNode> getNodeSequence(String key) {
		
		String remoteZone = remoteZoneNames.getNextElement();
		if (remoteZone != null) {
			List<HostToken> remoteHostTokens = remoteZoneTokens.get(remoteZone);
			if (remoteHostTokens.size() > 0) {
				final MemcachedNode remoteZoneNode = getNodeFromHashRing(key, remoteHostTokens);
				return new SingleNodeIterator(remoteZoneNode);
			}
		}
		return ZeroNodeIterator;
	}
	
	private MemcachedNode getNodeFromHashRing(String key, List<HostToken> hostTokens) {
		
		Long keyHash = tokenMapper.hash(key);
		System.out.println("\nHashKey: " + keyHash);
		HostToken hostToken = null; //tokenMapper.getToken(hostTokens, keyHash);
		System.out.println("HostToken: " + hostToken +"\n");
		return connFactory.getMemcachedNodeForHost(hostToken.getHost());
	}

	
	private class SingleNodeIterator implements Iterator<MemcachedNode> {
		
		final AtomicBoolean hasNext = new AtomicBoolean(true);
		final MemcachedNode node;
		
		private SingleNodeIterator(MemcachedNode n) {
			this.node = n;
		}
		
		@Override
		public boolean hasNext() {
			return hasNext.get();
		}

		@Override
		public MemcachedNode next() {
			if (!hasNext()) {
				return null;
			}
			
			try {
				return node;
			} finally {
				hasNext.set(false);
			}
		}

		@Override
		public void remove() {
			throw new NotImplementedException();
		}
	}
	
	private Iterator<MemcachedNode> ZeroNodeIterator = new Iterator<MemcachedNode> () {
		
		
		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public MemcachedNode next() {
			return null;
		}

		@Override
		public void remove() {
			throw new NotImplementedException();
		}
	};

}




