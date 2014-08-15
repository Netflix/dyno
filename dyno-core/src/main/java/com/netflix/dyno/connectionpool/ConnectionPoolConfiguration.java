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

import com.netflix.dyno.connectionpool.RetryPolicy.RetryPolicyFactory;
import com.netflix.dyno.connectionpool.impl.health.ErrorMonitor.ErrorMonitorFactory;


public interface ConnectionPoolConfiguration {
	
	public static enum LoadBalancingStrategy {
		RoundRobin, TokenAware;
	}

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
     * @return LoadBalancingStrategy
     */
    public LoadBalancingStrategy getLoadBalancingStrategy();
    
    /**
     * @return Socket connect timeout
     */
    public int getConnectTimeout();
    
    /**
     * 
     * @return
     */
    public int getPoolShutdownDelay();
    
    /**
     * 
     * @return
     */
    public boolean localDcAffinity(); 
    
    /**
     * 
     * @return
     */
    public ErrorMonitorFactory getErrorMonitorFactory();
    
    /**
     * 
     * @return
     */
    public RetryPolicyFactory getRetryPolicyFactory();
    
    /**
     * 
     * @return
     */
    public HostSupplier getHostSupplier();
    
    /**
     * 
     * @return
     */
    public TokenMapSupplier getTokenSupplier();
    
    /**
     * 
     * @return
     */
    public int getPingFrequencySeconds();
}