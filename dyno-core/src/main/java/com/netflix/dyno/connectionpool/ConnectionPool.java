package com.netflix.dyno.connectionpool;

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

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

import net.spy.memcached.ops.OperationException;

import com.netflix.dyno.connectionpool.exception.DynoException;

/**
 * Base interface for a pool of connections. A concrete connection pool will
 * track hosts in a cluster.
 * 
 * @author poberai
 * @param <CL>
 */
public interface ConnectionPool<CL> {
    
	/**
     * Add a host to the connection pool.
     * 
     * @param host
     * @returns True if host was added or false if host already exists
     * @throws DynoException
     */
    boolean addHost(Host host);

	/**
     * Remove a host from the connection pool.
     * 
     * @param host
     * @returns True if host was added or false if host already exists
     * @throws DynoException
     */
    boolean removeHost(Host host);

    /**
     * @return Return true if the host is up
     * @param host
     */
    boolean isHostUp(Host host);

    /**
     * @return Return true if host is contained within the connection pool
     * @param host
     */
    boolean hasHost(Host host);

    /**
     * @return Return list of active hosts on which connections can be created
     */
    List<HostConnectionPool<CL>> getActivePools();

    /**
     * @return Get all pools
     */
    List<HostConnectionPool<CL>> getPools();
    
    /**
     * Set the complete set of hosts in the ring
     * @param hosts
     */
    Future<Boolean> updateHosts(Collection<Host> activeHosts, Collection<Host> inactiveHosts);

    /**
     * @return Return an immutable connection pool for this host
     * @param host
     */
    HostConnectionPool<CL> getHostPool(Host host);

    /**
     * Execute an operation with failover within the context of the connection
     * pool. The operation will only fail over for connection pool errors and
     * not application errors.
     * 
     * @param <R>
     * @param op
     * @throws DynoException
     * @throws OperationException
     */
    <R> OperationResult<R> executeWithFailover(Operation<CL, R> op) throws DynoException;
            
    <R> Future<OperationResult<R>> executeAsync(AsyncOperation<CL, R> op) throws DynoException;

    /**
     * Shut down the connection pool and terminate all existing connections
     */
    void shutdown();

    /**
     * Setup the connection pool and start any maintenance threads
     */
    Future<Boolean> start();

}