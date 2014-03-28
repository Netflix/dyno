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


public interface ConnectionPoolConfiguration {

    /**
     * @return Unique name assigned to this connection pool
     */
    public String getName();

    /**
     * @return Data port to be used when no port is specified to a list of seeds or when
     * doing a ring describe since the ring describe does not include a host
     */
    public int getPort();

    /**
     * @return Maximum number of connections to allocate for a single host's pool
     */
    public int getMaxConnsPerHost();

    /**
     * @return Initial number of connections created when a connection pool is started
     */
    public int getInitConnsPerHost();

    /**
     * @return Maximum amount of time to wait for a connection to free up when a
     * connection pool is exhausted.
     * 
     * @return
     */
    public int getMaxTimeoutWhenExhausted();

    /**
     * @return Get the max number of failover attempts
     */
    public int getMaxFailoverCount();

    /**
     * @return Socket read/write timeout
     */
    public int getSocketTimeout();

    /**
     * @return Socket connect timeout
     */
    int getConnectTimeout();

//    /**
//     * @return Window size for limiting the number of connection open requests
//     */
//    int getConnectionLimiterWindowSize();
//
//    /**
//     * @return Maximum number of connection attempts in a given window
//     */
//    int getConnectionLimiterMaxPendingCount();
//
//
//    /**
//     * @return Return the ratio for keeping a minimum number of hosts in the pool even if they are slow
//     * or are blocked.  For example, a ratio of 0.75 with a connection pool of 12 hosts will 
//     * ensure that no more than 4 hosts can be quaratined.
//     */
//    float getMinHostInPoolRatio();
    

    /**
     * @return Maximum number of pending connect attempts per host
     */
    public int getMaxPendingConnectionsPerHost();

    /**
     * @return Get max number of blocked clients for a host.
     */
    public int getMaxBlockedThreadsPerHost();

    /**
     * @return  Shut down a host if it times out too many time within this window
     */
    public int getTimeoutWindow();

    /**
     * @return Number of allowed timeouts within getTimeoutWindow() milliseconds
     */
    public int getMaxTimeoutCount();
}
