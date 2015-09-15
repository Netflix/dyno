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

import java.util.ArrayList;
import java.util.List;

import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ErrorRateMonitorConfig;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.RetryPolicy;
import com.netflix.dyno.connectionpool.RetryPolicy.RetryPolicyFactory;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.health.ErrorMonitor.ErrorMonitorFactory;
import com.netflix.dyno.connectionpool.impl.health.SimpleErrorMonitorImpl.SimpleErrorMonitorFactory;
import com.netflix.dyno.connectionpool.impl.utils.ConfigUtils;

public class ConnectionPoolConfigurationImpl implements ConnectionPoolConfiguration {
	
	// DEFAULTS 
	private static final int DEFAULT_PORT = 8102; 
	private static final int DEFAULT_MAX_CONNS_PER_HOST = 1; 
	private static final int DEFAULT_MAX_TIMEOUT_WHEN_EXHAUSTED = 2000; 
	private static final int DEFAULT_MAX_FAILOVER_COUNT = 3; 
	private static final int DEFAULT_CONNECT_TIMEOUT = 3000; 
	private static final int DEFAULT_SOCKET_TIMEOUT = 12000; 
	private static final int DEFAULT_POOL_SHUTDOWN_DELAY = 60000; 
	private static final int DEFAULT_PING_FREQ_SECONDS = 1;
	private static final int DEFAULT_FLUSH_TIMINGS_FREQ_SECONDS = 300;
	private static final boolean DEFAULT_LOCAL_DC_AFFINITY = true; 
	private static final LoadBalancingStrategy DEFAULT_LB_STRATEGY = LoadBalancingStrategy.TokenAware;
    private static final String DEFAULT_CONFIG_PUBLISHER_ADDRESS = null;
    private static final boolean DEFAULT_FAIL_ON_STARTUP_IFNOHOSTS = false;
    private static final int DEFAULT_FAIL_ON_STARTUP_IFNOHOSTS_SECONDS = 60;

	private HostSupplier hostSupplier;
	private TokenMapSupplier tokenSupplier;

	private final String name;
	private int port = DEFAULT_PORT; 
	private int maxConnsPerHost = DEFAULT_MAX_CONNS_PER_HOST; 
	private int maxTimeoutWhenExhausted = DEFAULT_MAX_TIMEOUT_WHEN_EXHAUSTED; 
	private int maxFailoverCount = DEFAULT_MAX_FAILOVER_COUNT; 
	private int connectTimeout = DEFAULT_CONNECT_TIMEOUT; 
	private int socketTimeout = DEFAULT_SOCKET_TIMEOUT; 
	private int poolShutdownDelay = DEFAULT_POOL_SHUTDOWN_DELAY; 
	private int pingFrequencySeconds = DEFAULT_PING_FREQ_SECONDS;
	private int flushTimingsFrequencySeconds = DEFAULT_FLUSH_TIMINGS_FREQ_SECONDS;
	private boolean localDcAffinity = DEFAULT_LOCAL_DC_AFFINITY;
	private LoadBalancingStrategy lbStrategy = DEFAULT_LB_STRATEGY; 
	private String localDC;
    private String configPublisherAddress = DEFAULT_CONFIG_PUBLISHER_ADDRESS;
    private boolean failOnStartupIfNoHosts = DEFAULT_FAIL_ON_STARTUP_IFNOHOSTS;
    private int failOnStarupIfNoHostsSeconds = DEFAULT_FAIL_ON_STARTUP_IFNOHOSTS_SECONDS;

	private RetryPolicyFactory retryFactory = new RetryPolicyFactory() {

		@Override
		public RetryPolicy getRetryPolicy() {
			return new RunOnce();
		}
	};
	
	private ErrorMonitorFactory errorMonitorFactory = new SimpleErrorMonitorFactory();
	
	public ConnectionPoolConfigurationImpl(String name) {
		this.name = name;
		this.localDC = ConfigUtils.getLocalZone();
	}
	
	@Override
	public String getName() {
		return name;
	}

	@Override
	public int getPort() {
		return port;
	}

	@Override
	public int getMaxConnsPerHost() {
		return maxConnsPerHost;
	}

	@Override
	public int getMaxTimeoutWhenExhausted() {
		return maxTimeoutWhenExhausted;
	}

	@Override
	public int getMaxFailoverCount() {
		return maxFailoverCount;
	}

	public ConnectionPoolConfigurationImpl setPort(int port) {
		this.port = port;
		return this;
	}

	@Override
	public int getConnectTimeout() {
		return connectTimeout;
	}

	@Override
	public int getSocketTimeout() {
		return socketTimeout;
	}

	@Override
	public RetryPolicyFactory getRetryPolicyFactory() {
		return retryFactory;
	}
	
	@Override
	public int getPoolShutdownDelay() {
		return poolShutdownDelay;
	}

	@Override
	public boolean localDcAffinity() {
		return localDcAffinity;
	}

	@Override
	public ErrorMonitorFactory getErrorMonitorFactory() {
		return errorMonitorFactory;
	}

	@Override
	public LoadBalancingStrategy getLoadBalancingStrategy() {
		return lbStrategy;
	}
	
	@Override
	public int getPingFrequencySeconds() {
		return pingFrequencySeconds;
	}

    @Override
    public String getLocalDC() {
        return localDC;
    }

    @Override
    public int getTimingCountersResetFrequencySeconds() {
        return flushTimingsFrequencySeconds;
    }

    @Override
    public String getConfigurationPublisherConfig() {
        return null;
    }

    @Override
    public boolean getFailOnStartupIfNoHosts() {
        return failOnStartupIfNoHosts;
    }

    public int getDefaultFailOnStartupIfNoHostsSeconds() {
        return failOnStarupIfNoHostsSeconds;
    }
	
	// ALL SETTERS
	public ConnectionPoolConfigurationImpl setMaxConnsPerHost(int maxConnsPerHost) {
		this.maxConnsPerHost = maxConnsPerHost;
		return this;
	}

	public ConnectionPoolConfigurationImpl setMaxTimeoutWhenExhausted(int maxTimeoutWhenExhausted) {
		this.maxTimeoutWhenExhausted = maxTimeoutWhenExhausted;
		return this;
	}

	public ConnectionPoolConfigurationImpl setMaxFailoverCount(int maxFailoverCount) {
		this.maxFailoverCount = maxFailoverCount;
		return this;
	}

	public ConnectionPoolConfigurationImpl setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
		return this;
	}

	public ConnectionPoolConfigurationImpl setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
		return this;
	}

	public ConnectionPoolConfigurationImpl setLoadBalancingStrategy(LoadBalancingStrategy strategy) {
		this.lbStrategy = strategy;
		return this;
	}

	public ConnectionPoolConfigurationImpl setRetryPolicyFactory(RetryPolicyFactory factory) {
		this.retryFactory = factory;
		return this;
	}

	public ConnectionPoolConfigurationImpl setPoolShutdownDelay(int shutdownDelaySeconds) {
		poolShutdownDelay = shutdownDelaySeconds;
		return this;
	}
	
	public ConnectionPoolConfigurationImpl setPingFrequencySeconds(int seconds) {
		pingFrequencySeconds = seconds;
		return this;
	}

	public ConnectionPoolConfigurationImpl setLocalDcAffinity(boolean condition) {
		localDcAffinity = condition;
		return this;
	}

    public ConnectionPoolConfigurationImpl setFailOnStartupIfNoHosts(boolean condition) {
        this.failOnStartupIfNoHosts = condition;
        return this;
    }

    public ConnectionPoolConfigurationImpl setFailOnStartupIfNoHostsSeconds(int seconds) {
        this.failOnStarupIfNoHostsSeconds = seconds;
        return null;
    }

	public HostSupplier getHostSupplier() {
		return hostSupplier;
	}

	public ConnectionPoolConfigurationImpl withHostSupplier(HostSupplier hSupplier) {
		hostSupplier = hSupplier;
		return this;
	}

	public TokenMapSupplier getTokenSupplier() {
		return tokenSupplier;
	}

	public ConnectionPoolConfigurationImpl withTokenSupplier(TokenMapSupplier tSupplier) {
		tokenSupplier = tSupplier;
		return this;
	}

	public ConnectionPoolConfigurationImpl withErrorMonitorFactory(ErrorMonitorFactory factory) {
		errorMonitorFactory = factory;
		return this;
	}
	
	public static class ErrorRateMonitorConfigImpl implements ErrorRateMonitorConfig {

		int window = 20; 
		int checkFrequency = 1;
		int suppressWindow = 90; 
		
		private List<ErrorThreshold> thresholds = new ArrayList<ErrorThreshold>();

		public ErrorRateMonitorConfigImpl() {
			this.addThreshold(10, 10, 80);
		}
		
		public ErrorRateMonitorConfigImpl(int w, int f, int s) {
			this.window = w;
			this.checkFrequency = f;
			this.suppressWindow = s;
		}
		
		@Override
		public int getWindowSizeSeconds() {
			return window;
		}

		@Override
		public int getCheckFrequencySeconds() {
			return checkFrequency;
		}

		@Override
		public int getCheckSuppressWindowSeconds() {
			return suppressWindow;
		}

		@Override
		public List<ErrorThreshold> getThresholds() {
			return thresholds;
		}

		public void addThreshold(final int bucketThreshold, final int bucketWindow, final int bucketCoverage) {
			thresholds.add(new ErrorThreshold() {

				@Override
				public int getThresholdPerSecond() {
					return bucketThreshold;
				}

				@Override
				public int getWindowSeconds() {
					return bucketWindow;
				}

				@Override
				public int getWindowCoveragePercentage() {
					return bucketCoverage;
				}
				
			});
		}
	}

    public ConnectionPoolConfigurationImpl setLocalDC(String dc) {
		this.localDC = dc;
		return this;
	}

}
