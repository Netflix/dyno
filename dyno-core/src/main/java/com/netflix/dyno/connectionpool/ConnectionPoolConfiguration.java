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
	
	enum LoadBalancingStrategy {
		RoundRobin, TokenAware;
	}

    /**
     * @return Unique name assigned to this connection pool
     */
    String getName();

    /**
     * @return Data port to be used when no port is specified to a list of seeds or when
     * doing a ring describe since the ring describe does not include a host
     */
    int getPort();

    /**
     * @return Maximum number of connections to allocate for a single host's pool
     */
    int getMaxConnsPerHost();

    /**
     * @return Maximum amount of time to wait for a connection to free up when a
     * connection pool is exhausted.
     * 
     * @return
     */
    int getMaxTimeoutWhenExhausted();

    /**
     * @return Get the max number of failover attempts
     */
    int getMaxFailoverCount();

    /**
     * @return Socket read/write timeout
     */
    int getSocketTimeout();

    /**
     * @return LoadBalancingStrategy
     */
    LoadBalancingStrategy getLoadBalancingStrategy();
    
    /**
     * @return Socket connect timeout
     */
    int getConnectTimeout();
    
    /**
     * 
     * @return
     */
    int getPoolShutdownDelay();
    
    /**
     * 
     * @return
     */
    boolean localDcAffinity();
    
    /**
     * 
     * @return
     */
    ErrorMonitorFactory getErrorMonitorFactory();
    
    /**
     * 
     * @return
     */
    RetryPolicyFactory getRetryPolicyFactory();
    
    /**
     * 
     * @return
     */
    HostSupplier getHostSupplier();
    
    /**
     * 
     * @return
     */
    TokenMapSupplier getTokenSupplier();
    
    /**
     * 
     * @return
     */
    int getPingFrequencySeconds();
    
    /**
     * 
     * @return
     */
    String getLocalDC();

    /**
     * Returns the amount of time the histogram accumulates data before it is cleared, in seconds.
     * <p>
     * A histogram is used to record timing metrics. This provides more accurate timings to telemetry systems that
     * are polling at a fixed interval that spans hundreds or thousands of requests, i.e. 1 minute. Since the history
     * off all requests are preserved this is not ideal for scenarios where we don't want the history, for example
     * after a network latency spike the average latency will be affected for hours or days.
     * <p>
     * Note that 0 is the default and effectively disables this setting meaning all history is preserved.
     *
     * @return a positive integer that specifies the duration of the frequency to accumulate timing data or 0
     */
    int getTimingCountersResetFrequencySeconds();

    /**
     * Returns info about a system that will consume configuration data from dyno. This is used to
     * log configuration settings to a central system such as elastic search or cassandra.
     *
     * @return todo
     */
     String getConfigurationPublisherConfig();

    /**
     * If there are no hosts marked as 'Up' in the {@link HostSupplier} when starting the connection pool
     * a {@link com.netflix.dyno.connectionpool.exception.NoAvailableHostsException} will be thrown
     * if this is set to true. By default this is false.
     * <p>
     * When this does occur and this property is set to false, a warning will be logged and the connection pool
     * will go into an idle state, polling once per minute in the background for available hosts to connect to.
     * The connection pool can idle indefinitely. In the event that hosts do become available, the connection
     * pool will start.
     * </p>
     *
     * @return boolean to control the startup behavior specified in the description.
     */
    boolean getFailOnStartupIfNoHosts();

}