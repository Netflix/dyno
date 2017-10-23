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

/**
 * Specifies configuration settings for an instance of a dyno connection pool.
 */
public interface ConnectionPoolConfiguration {
    
	enum LoadBalancingStrategy {
		RoundRobin, TokenAware;
	}

    enum CompressionStrategy {
        /** Disables compression */
        NONE,

        /** Compresses values that exceed {@link #getValueCompressionThreshold()} */
        THRESHOLD
    }

    /**
     * Returns the unique name assigned to this connection pool.
     */
    String getName();

    /**
     * Returns the maximum number of connections to allocate to each Dynomite host. Note that at startup exactly this
     * number of connections will be established prior to serving requests.
     */
    int getMaxConnsPerHost();

    /**
     * Returns the maximum amount of time to wait for a connection to become available from the pool.
     *
     * <p>Retrieving a connection from the pool is very fast unless all connections are busy (in which case the
     * connection pool is exhausted)</p>
     */
    int getMaxTimeoutWhenExhausted();

    /**
     * Returns the maximum number of failover attempts.
     */
    int getMaxFailoverCount();

    /**
     * Returns the socket read/write timeout
     */
    int getSocketTimeout();

    /**
     * Returns the {@link LoadBalancingStrategy} for this connection pool.
     */
    LoadBalancingStrategy getLoadBalancingStrategy();
    
    /**
     * Specifies the socket connection timeout
     */
    int getConnectTimeout();

    /**
     * Returns true if the connection pool respects zone affinity, false otherwise.
     *
     * <p>
     * By default this is true. This is only false if it has been explicitly set to false OR if the connection pool
     * cannot determine it's local zone at startup.
     * </p>
     */
    boolean localZoneAffinity();
    
    /**
     * Returns the {@link ErrorMonitorFactory} to use for this connection pool.
     */
    ErrorMonitorFactory getErrorMonitorFactory();
    
    /**
     * Returns the {@link RetryPolicyFactory} to use for this connection pool.
     */
    RetryPolicyFactory getRetryPolicyFactory();
    
    /**
     * Returns the {@link HostSupplier} to use for this connection pool.
     */
    HostSupplier getHostSupplier();
    
    /**
     * Returns the {@link TokenMapSupplier} to use for this connection pool.
     */
    TokenMapSupplier getTokenSupplier();
    
    /**
     * Returns the interval for which pings are issued on connections.
     *
     * <p>
     * Pings are used to keep persistent connections warm.
     * </p>
     */
    int getPingFrequencySeconds();
    
    /**
     * Returns the local rack name for this connection pool. In AWS terminology this is a zone, e.g. us-east-1c.
     */
    String getLocalRack();

    /**
     * Returns the local data center name for this connection pool. In AWS terminology this is a region, e.g. us-east-1.
     */
    String getLocalDataCenter();

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
     * This is an experimental setting and as such is unstable and subject to change or be removed.
     * <p>
     * Returns info about a system that will consume configuration data from dyno. This is used to
     * log configuration settings to a central system such as elastic search or cassandra.
     * </p>
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

    /**
     * This works in conjunction with {@link #getCompressionStrategy()}. The compression strategy must be set to
     * {@link CompressionStrategy#THRESHOLD} for this to have any effect.
     * <p>
     * The value for this configuration setting is specified in <strong>bytes</strong>
     *
     * @return The final value to be used as a threshold in bytes.
     */
    int getValueCompressionThreshold();

    /**
     * Determines if values should be compressed prior to sending them across the wire to Dynomite. Note that this
     * feature <strong>is disabled by default</strong>.
     * <p>
     * Note that if compression is not enabled, no attempt to compress or decompress any data is made. If compression
     * has been enabled and needs to be disabled, rather than disabling compression it is recommended to set the
     * threshold to a large number so that effectively nothing will be compressed however data retrieved will still
     * be decompressed.
     *
     * @return the configured compression strategy value
     */
    CompressionStrategy getCompressionStrategy();

    /**
     * Returns true if dual-write functionality is enabled, false otherwise.
     */
    boolean isDualWriteEnabled();

    /**
     * Returns the name of the designated dual-write cluster, or null.
     */
    String getDualWriteClusterName();

    /**
     * Returns the percentage of traffic designated to be sent to the dual-write cluster.
     */
    int getDualWritePercentage();

    String getHashtag();

}
