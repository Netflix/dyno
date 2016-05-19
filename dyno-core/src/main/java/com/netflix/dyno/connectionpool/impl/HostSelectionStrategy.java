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
package com.netflix.dyno.connectionpool.impl;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Interface that encapsulates a strategy for selecting a {@link Connection} to a {@link Host} for the given {@link BaseOperation}
 * @author poberai
 *
 * @param <CL>
 */
public interface HostSelectionStrategy<CL> {

	/**
	 * 
	 * @param op
	 * @return
	 * @throws NoAvailableHostsException
	 */
	HostConnectionPool<CL> getPoolForOperation(BaseOperation<CL, ?> op) throws NoAvailableHostsException;

	/**
	 * 
	 * @param ops
	 * @return
	 * @throws NoAvailableHostsException
	 */
	Map<HostConnectionPool<CL>,BaseOperation<CL,?>> getPoolsForOperationBatch(Collection<BaseOperation<CL, ?>> ops) throws NoAvailableHostsException;
	
	/**
	 * 
	 * @return
	 */
	List<HostConnectionPool<CL>> getOrderedHostPools();
	
	/**
	 * 
	 * @param token
	 * @return
	 */
	HostConnectionPool<CL> getPoolForToken(Long token);
	
	/**
	 * 
	 * @param start
	 * @param end
	 * @return
	 */
	List<HostConnectionPool<CL>> getPoolsForTokens(Long start, Long end);

    /**
     * Finds the server Host that owns the specified key.
     *
     * @param key
     * @return {@link HostToken}
     * @throws UnsupportedOperationException for non-token aware load balancing strategies
     */
    HostToken getTokenForKey(String key) throws UnsupportedOperationException;

	/**
	 * Init the connection pool with the set of hosts provided
	 * @param hostPools
	 */
	void initWithHosts(Map<HostToken, HostConnectionPool<CL>> hostPools);
	
	/**
	 * Add a host to the selection strategy. This is useful when the underlying dynomite topology changes.
	 * @param {@link com.netflix.dyno.connectionpool.impl.lb.HostToken}
	 * @param hostPool
	 * @return true/false indicating whether the pool was indeed added
	 */
	boolean addHostPool(HostToken host, HostConnectionPool<CL> hostPool);
	
	/**
	 * Remove a host from the selection strategy. This is useful when the underlying dynomite topology changes.
	 * @param {@link com.netflix.dyno.connectionpool.impl.lb.HostToken}
	 * @return true/false indicating whether the pool was indeed removed
	 */
	boolean removeHostPool(HostToken host);

	boolean isTokenAware();

	boolean isEmpty();

	interface HostSelectionStrategyFactory<CL> {
		
		/**
		 * Create/Return a HostSelectionStrategy 
		 * @return HostSelectionStrategy
		 */
		public HostSelectionStrategy<CL> vendPoolSelectionStrategy();
	}

}
