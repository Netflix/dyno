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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.exception.PoolExhaustedException;

/**
 * Interface that encapsulates a strategy for selecting a {@link Connection} to a {@link Host} for the given {@link BaseOperation}
 * @author poberai
 *
 * @param <CL>
 */
public interface HostSelectionStrategy<CL> {

	/**
	 * Get a connection to this host within the specified time duration
	 * @param op
	 * @param duration
	 * @param unit
	 * @return
	 * @throws NoAvailableHostsException if there are no connections or pool is not inited etc
	 * @throws PoolExhaustedException  if all connections within the pool are busy serving other requests and no connection becomes available 
	 *         within the specified time duration      
	 */
	public Connection<CL> getConnection(BaseOperation<CL, ?> op, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException;

	/**
	 * Get a map of connections for the specified operations. This is used for scatter gather type of operations. 
	 * Please note that the underlying connection pool must be sized appropriately when using this operation since this is a less available operations
	 * by design
	 * 
	 * @param ops
	 * @param duration
	 * @param unit
	 * @return
	 * @throws NoAvailableHostsException
	 * @throws PoolExhaustedException
	 */
	public Map<BaseOperation<CL,?>,Connection<CL>> getConnection(Collection<BaseOperation<CL, ?>> ops, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException;
	
	/**
	 * Explicitly fetch a fallback connection from the fallback dc or set of hosts for the specified operation within the specified time duration
	 * @param op
	 * @param duration
	 * @param unit
	 * @return
	 * @throws NoAvailableHostsException
	 * @throws PoolExhaustedException
	 */
	public Connection<CL> getFallbackConnection(BaseOperation<CL, ?> op, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException;

	/**
	 * Init the connection pool with the set of hosts provided
	 * @param hostPools
	 */
	public void initWithHosts(Map<Host, HostConnectionPool<CL>> hostPools);
	
	/**
	 * Add a host to the selection strategy. This is useful when the underlying dynomite topology changes.
	 * @param host
	 * @param hostPool
	 */
	public void addHost(Host host, HostConnectionPool<CL> hostPool);
	
	/**
	 * Remove a host from the selection strategy. This is useful when the underlying dynomite topology changes.
	 * @param host
	 * @param hostPool
	 */
	public void removeHost(Host host, HostConnectionPool<CL> hostPool);
	
	/**
	 * Interface that encapsulates a factory for vending {@link HostSelectionStrategy}
	 * @author poberai
	 *
	 * @param <CL>
	 */
	public static interface HostSelectionStrategyFactory<CL> {
		
		/**
		 * Create/Return a HostSelectionStrategy 
		 * @return HostSelectionStrategy
		 */
		public HostSelectionStrategy<CL> vendSelectionStrategy();
	}
}
