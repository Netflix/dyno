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
package com.netflix.dyno.connectionpool;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;

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
    public boolean addHost(Host host);

	/**
     * Remove a host from the connection pool.
     * 
     * @param host
     * @returns True if host was added or false if host already exists
     * @throws DynoException
     */
    public boolean removeHost(Host host);

    /**
     * @return Return true if the host is up
     * @param host
     */
    public boolean isHostUp(Host host);

    /**
     * @return Return true if host is contained within the connection pool
     * @param host
     */
    public boolean hasHost(Host host);

    /**
     * @return Return list of active hosts on which connections can be created
     */
    public List<HostConnectionPool<CL>> getActivePools();

    /**
     * @return Get all pools
     */
    public List<HostConnectionPool<CL>> getPools();
    
    /**
     * Set the complete set of hosts in the ring
     * @param hosts
     */
    public Future<Boolean> updateHosts(Collection<Host> activeHosts, Collection<Host> inactiveHosts);

    /**
     * @return Return an immutable connection pool for this host
     * @param host
     */
    public HostConnectionPool<CL> getHostPool(Host host);

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
    
    /**
     * Scatter gather style operation
     * @param op
     * @return Collection<OperationResult<R>>
     * @throws DynoException
     */
    public <R> Collection<OperationResult<R>> executeWithRing(Operation<CL, R> op) throws DynoException;

    /**
     * Execute an operation asynchronously.
     * @param op
     * @return ListenableFuture<OperationResult<R>>
     * @throws DynoException
     */
    public <R> ListenableFuture<OperationResult<R>> executeAsync(AsyncOperation<CL, R> op) throws DynoException;

    /**
     * Shut down the connection pool and terminate all existing connections
     */
    public void shutdown();

    /**
     * Setup the connection pool and start any maintenance threads
     */
    public Future<Boolean> start();
}