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
package com.netflix.dyno.connectionpool.impl.lb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.impl.HostSelectionStrategy;
import com.netflix.dyno.connectionpool.impl.hash.BinarySearchTokenMapper;
import com.netflix.dyno.connectionpool.impl.hash.Murmur1HashPartitioner;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Transform;

/**
 * Simple class that implements {@link HostSelectionStrategy} using the TOKEN AWARE algorithm. 
 * Note that this component needs to be aware of the dynomite ring topology to be able to 
 * successfully map to the correct token owner for any key of an {@link Operation}
 * 
 * @author poberai
 *
 * @param <CL>
 */
public class TokenAwareSelection<CL> implements HostSelectionStrategy<CL> {

	private final BinarySearchTokenMapper tokenMapper;

	private final ConcurrentHashMap<Long, HostConnectionPool<CL>> tokenPools = new ConcurrentHashMap<Long, HostConnectionPool<CL>>();
	
	public TokenAwareSelection() {
		
		this.tokenMapper = new BinarySearchTokenMapper(new Murmur1HashPartitioner());
	}

	@Override
	public void initWithHosts(Map<HostToken, HostConnectionPool<CL>> hPools) {
		
		tokenPools.putAll(CollectionUtils.transformMapKeys(hPools, new Transform<HostToken, Long>() {

			@Override
			public Long get(HostToken x) {
				return x.getToken();
			}
			
		}));

		this.tokenMapper.initSearchMecahnism(hPools.keySet());
	}

	@Override
	public HostConnectionPool<CL> getPoolForOperation(BaseOperation<CL, ?> op) throws NoAvailableHostsException {
		
		String key = op.getKey();
		HostToken hToken = this.getTokenForKey(key);
		
		HostConnectionPool<CL> hostPool = null;
		if (hToken != null) {
			hostPool = tokenPools.get(hToken.getToken());
		}
		
		if (hostPool == null) {
			throw new NoAvailableHostsException("Could not find host connection pool for key: " + key + ", hash: " +
                    tokenMapper.hash(key));
		}
		
		return hostPool;
	}

	@Override
	public Map<HostConnectionPool<CL>,BaseOperation<CL,?>> getPoolsForOperationBatch(Collection<BaseOperation<CL, ?>> ops) throws NoAvailableHostsException {
		throw new RuntimeException("Not Implemented");
	}
	
	@Override
	public List<HostConnectionPool<CL>> getOrderedHostPools() {
		return new ArrayList<HostConnectionPool<CL>>(tokenPools.values());
	}
	
	@Override
	public HostConnectionPool<CL> getPoolForToken(Long token) {
		return tokenPools.get(token);
	}
	
	public List<HostConnectionPool<CL>> getPoolsForTokens(Long start, Long end) {
		throw new RuntimeException("Not Implemented");
	}

    @Override
    public HostToken getTokenForKey(String key) throws UnsupportedOperationException {
        Long keyHash = tokenMapper.hash(key);
        return tokenMapper.getToken(keyHash);
    }

    @Override
	public boolean addHostPool(HostToken hostToken, HostConnectionPool<CL> hostPool) {
		
		HostConnectionPool<CL> prevPool = tokenPools.put(hostToken.getToken(), hostPool);
		if (prevPool == null) {
			tokenMapper.addHostToken(hostToken);
			return true;
		}  else {
			return false;
		}
	}

	@Override
	public boolean removeHostPool(HostToken hostToken) {

		HostConnectionPool<CL> prev = tokenPools.get(hostToken.getToken());
		if (prev != null) {
			tokenPools.remove(hostToken.getToken());
			return true;
		} else {
			return false;
		}
	}

	public Long getKeyHash(String key) {
		Long keyHash = tokenMapper.hash(key);
		return keyHash;
	}

	
	public String toString() {
		return "TokenAwareSelection: " + tokenMapper.toString();
	}
}
