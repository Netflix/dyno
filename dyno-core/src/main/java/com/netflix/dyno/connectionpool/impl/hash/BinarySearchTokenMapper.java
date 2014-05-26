package com.netflix.dyno.connectionpool.impl.hash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.dyno.connectionpool.HashPartitioner;
import com.netflix.dyno.connectionpool.HostToken;

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
		
		Long token = binarySearch.get().getTokenOwner(keyHash);
		return tokenMap.get(token);
	}

	private void initSearchMecahnism(Collection<HostToken> hostTokens) {

		for (HostToken hostToken : hostTokens) {
			tokenMap.put(hostToken.getToken(), hostToken);
		}
		
		List<Long> tokens = new ArrayList<Long>(tokenMap.keySet());
		Collections.sort(tokens);
		
		binarySearch.set(new DynoBinarySearch<Long>(tokens));
	}
}
