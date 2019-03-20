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
import java.util.concurrent.TimeUnit;

import com.netflix.dyno.connectionpool.exception.DynoException;

/**
 * Interface for a pool of {@link Connection}(s) for a single {@link Host}
 * 
 * The interface prescribes certain key features required by clients of this class, such as 
 *      <ol>
 *      <li> Basic connection pool life cycle management such as prime connections (init) and shutdown </li> <br/>
 *      
 *      <li> Basic {@link Connection} life cycle management such as borrow / return / close / markAsDown </li> <br/>
 *      
 *      <li> Tracking the {@link Host} associated with the connection pool. </li> <br/>
 *      
 *      <li> Visibility into the status of the connection pool and it's connections.
 *         <ol>
 *         <li>  Tracking status of pool -  isConnecting / isActive  / isShutdown  </li>
 *         <li>  Tracking basic counters for connections - active / pending / blocked / idle / busy / closed etc </li>
 *         <li>  Tracking latency scores for connections to this host.  </li>
 *         <li>  Tracking failures for connections to this host. </li>
 *         </ol> 
 *     </ol>
 *     
 * This class is intended to be used within a collection of {@link HostConnectionPool} tracked by a
 * {@link ConnectionPool} for all the {@link Host}(s) within a Dynomite cluster.
 * 
 * @see {@link ConnectionPool} for references to this class.
 *  
 * @author poberai
 * 
 * @param <CL>
 */
public interface HostConnectionPool<CL> {
	
    /**
     * Borrow a connection from the host. May create a new connection if one is
     * not available.
     * 
     * @param duration The amount of time to wait for a connection to become available
     * @param unit Specifies the unit of time corresponding to the duration (i.e. seconds vs milliseconds)
     * @return A borrowed connection.  Connection must be returned either by calling returnConnection 
     *  or closeConnection.
     * @throws DynoException
     */
    Connection<CL> borrowConnection(int duration, TimeUnit unit) throws DynoException;

    /**
     * Return a connection to the host's pool. May close the connection if the
     * pool is down or the last exception on the connection is determined to be
     * fatal.
     * 
     * @param connection
     * @return True if connection was closed
     */
    boolean returnConnection(Connection<CL> connection);

    /**
     * Close this connection and update internal state
     * 
     * @param connection
     */
    boolean closeConnection(Connection<CL> connection);

    /**
     * Recycle a connection by closing this and adding a new connection to the pool
     *
     * @param connection
     */
    void recycleConnection(Connection<CL> connection);

    /**
     * Shut down the host so no more connections may be created when
     * borrowConnections is called and connections will be terminated when
     * returnConnection is called.
     */
    void markAsDown(DynoException reason);

    /**
     * Recycle all connections in the connection pool. 
     * Note that the impl should either be able to handle active requests when re-cycling connections 
     * or should be NOT active when re-cycling connections, so that calling clients can failover to other pools/hosts.
     */
    void reconnect();

    /**
     * Completely shut down this connection pool as part of a client shutdown
     */
    void shutdown();

    /**
     * Create new connections and add them to the pool. Consult ConnectionPoolConfiguration.getMaxConnsPerHost()
     * 
     * @throws DynoException
     * @returns Actual number of connections created
     */
    int primeConnections() throws DynoException;

    /**
     * @return Get the host to which this pool is associated
     */
    Host getHost();

    /**
     * @return Return true if the pool is active.
     */
    boolean isActive();
    
    /**
     * @return Return true if the has been shut down and is no longer accepting traffic.
     */
    boolean isShutdown();
    
    /**
     * Get all connections for the managed pool underneath. 
     * USE with EXTREME CAUTION since all vended connections must be returned to the pool
     * in order to avoid pool exhaustion.
     * @return Collection<Connection<CL>>
     */
    Collection<Connection<CL>> getAllConnections();

    int getConnectionTimeout();

    int getSocketTimeout();

    int size();
}