/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.dyno.connectionpool.impl.hash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.dyno.connectionpool.HashPartitioner;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;

/**
 * Impl of {@link HashPartitioner} that can be used to keys to the dynomite topology ring using the binary search mechanism. 
 * Note that the class only performs the function of binary search to locate a hash token on the dynomite topology ring. 
 * The hash token to be generated from the key is generated using the HashPartitioner provided to this class. 
 *  
 * @author poberai
 *
 */
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
	public HostToken getToken(Long keyHash) {
		Long token = binarySearch.get().getTokenOwner(keyHash);
		if (token == null) {
			throw new NoAvailableHostsException("Token not found for key hash: " + keyHash);
		}
		return tokenMap.get(token);
	}

	public void initSearchMecahnism(Collection<HostToken> hostTokens) {

		for (HostToken hostToken : hostTokens) {
			tokenMap.put(hostToken.getToken(), hostToken);
		}
		initBinarySearch();
	}
	
	public void addHostToken(HostToken hostToken) {

		HostToken prevToken = tokenMap.putIfAbsent(hostToken.getToken(), hostToken);
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
			if (token.getHost().equals(host)) {
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
