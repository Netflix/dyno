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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.impl.HostSelectionStrategy;

/**
 * Simple impl of {@link HostSelectionStrategy} that uses ROUND ROBIN. It employs the {@link CircularList} data structure
 * to provide RR balancing in a thread safe manner. 
 * Note that the class can also support dynamically adding and removing {@link Host}
 * @author poberai
 *
 * @param <CL>
 */
public class RoundRobinSelection<CL> implements HostSelectionStrategy<CL> {
	
	// The total set of host pools. Once the host is selected, we ask it's corresponding pool to vend a connection
	private final ConcurrentHashMap<Long, HostConnectionPool<CL>> tokenPools = new ConcurrentHashMap<Long, HostConnectionPool<CL>>();

	// the circular list of Host over which we load balance in a round robin fashion
	private final CircularList<HostToken> circularList = new CircularList<HostToken>(null);

	public RoundRobinSelection() {
	}

	@Override
	public HostConnectionPool<CL> getPoolForOperation(BaseOperation<CL, ?> op) throws NoAvailableHostsException {
		
		int numTries = circularList.getSize();
		HostConnectionPool<CL> lastPool = null;
		
		while (numTries > 0) {
			lastPool = getNextConnectionPool();
			numTries--;
			if (lastPool.isActive() && lastPool.getHost().isUp()) {
				return lastPool;
			}
		}
		
		// If we reach here then we haven't found an active pool. Return the last inactive pool anyways, 
		// and HostSelectionWithFallback can choose a fallback pool from another dc
		return lastPool; 
	}

	@Override
	public Map<HostConnectionPool<CL>, BaseOperation<CL, ?>> getPoolsForOperationBatch(Collection<BaseOperation<CL, ?>> ops) throws NoAvailableHostsException {
		Map<HostConnectionPool<CL>, BaseOperation<CL, ?>> map = new HashMap<HostConnectionPool<CL>, BaseOperation<CL, ?>>();
		for (BaseOperation<CL, ?> op : ops) {
			map.put(getNextConnectionPool(), op);
		}
		return map;
	}


	@Override
	public List<HostConnectionPool<CL>> getOrderedHostPools() {
		return new ArrayList<HostConnectionPool<CL>>(tokenPools.values());
	}


	@Override
	public HostConnectionPool<CL> getPoolForToken(Long token) {
		return tokenPools.get(token);
	}


	@Override
	public List<HostConnectionPool<CL>> getPoolsForTokens(Long start, Long end) {
		throw new UnsupportedOperationException();
	}

    @Override
    public HostToken getTokenForKey(String key) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Not implemented for Round Robin load balancing strategy");
    }

    private HostConnectionPool<CL> getNextConnectionPool() throws NoAvailableHostsException {

		HostToken hostToken = circularList.getNextElement();

		HostConnectionPool<CL> hostPool = tokenPools.get(hostToken.getToken());
		if (hostPool == null) {
			throw new NoAvailableHostsException("Could not find host connection pool for host token: " + hostToken);
		}
		return hostPool;
	}
	
	@Override
	public void initWithHosts(Map<HostToken, HostConnectionPool<CL>> hPools) {
		
		for (HostToken token : hPools.keySet()) {
			tokenPools.put(token.getToken(), hPools.get(token));
		}
		circularList.swapWithList(hPools.keySet());
	}

	@Override
	public boolean addHostPool(HostToken host, HostConnectionPool<CL> hostPool) {
		
		HostConnectionPool<CL> prevPool = tokenPools.put(host.getToken(), hostPool);
		if (prevPool == null) {
			List<HostToken> newHostList = new ArrayList<HostToken>(circularList.getEntireList());
			newHostList.add(host);
			circularList.swapWithList(newHostList);
		}
		return prevPool == null;
	}

	@Override
	public boolean removeHostPool(HostToken host) {

		HostConnectionPool<CL> prevPool = tokenPools.get(host.getToken());
		if (prevPool != null) {
			List<HostToken> newHostList = new ArrayList<HostToken>(circularList.getEntireList());
			newHostList.remove(host);
			circularList.swapWithList(newHostList);
			tokenPools.remove(host.getToken());
		}
		return prevPool != null;
	}
	
	public String toString() {
		return "RoundRobinSelector: list: " + circularList.toString();
	}
}
