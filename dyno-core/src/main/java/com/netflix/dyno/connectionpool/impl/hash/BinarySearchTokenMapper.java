package com.netflix.dyno.connectionpool.impl.hash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.dyno.connectionpool.HashPartitioner;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;

public class BinarySearchTokenMapper implements HashPartitioner {

	private final HashPartitioner partitioner; 
	
	private final AtomicReference<DynoBinarySearch<Long>> binarySearch = new AtomicReference<DynoBinarySearch<Long>>(null); 
	private final ConcurrentHashMap<Long, HostToken> tokenMap = new ConcurrentHashMap<Long, HostToken>(); 
	
	public BinarySearchTokenMapper(HashPartitioner p) {
		this.partitioner = p;
	}
	
	@Override
	public Long hash(int key) {
		return partitioner.hash(key);
	}

	@Override
	public Long hash(long key) {
		return partitioner.hash(key);
	}

	@Override
	public Long hash(String key) {
		return partitioner.hash(key);
	}

	@Override
	public HostToken getToken(List<HostToken> hostTokens, Long keyHash) {
		
		if (binarySearch.get() == null) {
			initSearchMecahnism(hostTokens);
		}
		
		return getToken(keyHash);
	}

	public HostToken getToken(Long keyHash) {
		Long token = binarySearch.get().getTokenOwner(keyHash);
		return tokenMap.get(token);
	}

	public void initSearchMecahnism(Collection<HostToken> hostTokens) {

		for (HostToken hostToken : hostTokens) {
			tokenMap.put(hostToken.getToken(), hostToken);
		}
		initBinarySearch();
	}
	
	public void addHostToken(HostToken hostToken) {

		HostToken prevToken = tokenMap.put(hostToken.getToken(), hostToken);
		if (prevToken == null) {
			initBinarySearch();
		}
	}
	
	public void remoteHostToken(HostToken hostToken) {

		HostToken prevToken = tokenMap.remove(hostToken.getToken());
		if (prevToken != null) {
			initBinarySearch();
		}
	}
	
	public void removeHost(Host host) {
		
		HostToken theToken = null;
		
		for (HostToken token : tokenMap.values()) {
			if (token.getHost().getHostName().equals(host.getHostName())) {
				theToken = token;
				break;
			}
		}
		
		if (theToken != null) {
			remoteHostToken(theToken);
		}
	}

	private void initBinarySearch() {
		List<Long> tokens = new ArrayList<Long>(tokenMap.keySet());
		Collections.sort(tokens);
		
		binarySearch.set(new DynoBinarySearch<Long>(tokens));
	}

	public boolean isEmpty() {
		return this.tokenMap.size() == 0;
	}
	
	public String toString() {
		return binarySearch.toString();
	}
}
