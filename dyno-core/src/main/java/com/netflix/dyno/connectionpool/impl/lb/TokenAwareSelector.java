package com.netflix.dyno.connectionpool.impl.lb;

import java.util.Collection;
import java.util.List;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostToken;
import com.netflix.dyno.connectionpool.impl.TokenMapSupplierImpl;
import com.netflix.dyno.connectionpool.impl.hash.BinarySearchTokenMapper;
import com.netflix.dyno.connectionpool.impl.hash.Murmur1HashPartitioner;
import com.netflix.dyno.connectionpool.impl.lb.SelectionWIthRemoteZoneFallback.SingleDCSelector;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Predicate;

public class TokenAwareSelector implements SingleDCSelector {

	private TokenMapSupplierImpl tokenSupplier; 
	private final BinarySearchTokenMapper tokenMapper;
	private final String localZone;

	public TokenAwareSelector() {
		this.localZone = System.getenv("EC2_AVAILABILITY_ZONE");
		this.tokenMapper = new BinarySearchTokenMapper(new Murmur1HashPartitioner());
	}

	@Override
	public void init(List<Host> hosts) {

		this.tokenSupplier =  new TokenMapSupplierImpl(hosts);
		List<HostToken> allHostTokens = tokenSupplier.getTokens();
		
		Collection<HostToken> localZoneTokens = CollectionUtils.filter(allHostTokens,  new Predicate<HostToken>() {

			@Override
			public boolean apply(HostToken x) {
				String zone = x.getHost().getDC();
				return localZone != null ? localZone.equalsIgnoreCase(zone) : true;
			}
		});
		
		this.tokenMapper.initSearchMecahnism(localZoneTokens);
	}

	@Override
	public Host getHostForKey(String key) {

		Long keyHash = tokenMapper.hash(key);
		HostToken hToken = tokenMapper.getToken(keyHash);
		System.out.println("Key: " + key + ",  hash: " + keyHash + ", host: " + hToken.getHost().getHostName() + " token: " + hToken.getToken()) ;
		System.out.println(tokenMapper.toString());
		return hToken != null ? hToken.getHost() : null;
	}

	@Override
	public boolean isEmpty() {
		return this.tokenMapper.isEmpty();
	}

	@Override
	public void addHost(Host host) {
		String zone = host.getDC();
		boolean isLocal = localZone != null ? localZone.equalsIgnoreCase(zone) : true;
		
		if (isLocal) {
			HostToken hostToken = tokenSupplier.getTokenForHost(host);
			if (hostToken != null) {
				this.tokenMapper.addHostToken(hostToken);
			}
		}
	}

	@Override
	public void removeHost(Host host) {
		// do nothing. Not yet implemented
		throw new RuntimeException("Not yet implemented");
	}
}
