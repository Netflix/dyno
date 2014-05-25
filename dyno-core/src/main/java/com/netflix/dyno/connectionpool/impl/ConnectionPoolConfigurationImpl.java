package com.netflix.dyno.connectionpool.impl;

import java.util.ArrayList;
import java.util.List;

import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ErrorRateMonitorConfig;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.RetryPolicy;
import com.netflix.dyno.connectionpool.RetryPolicy.RetryPolicyFactory;
import com.netflix.dyno.connectionpool.TokenMapSupplier;

public class ConnectionPoolConfigurationImpl implements ConnectionPoolConfiguration {
	
	// DEFAULTS 
	private static final int DEFAULT_PORT = 8102; 
	private static final int DEFAULT_MAX_CONNS_PER_HOST = 1; 
	private static final int DEFAULT_MAX_TIMEOUT_WHEN_EXHAUSTED = 2000; 
	private static final int DEFAULT_MAX_FAILOVER_COUNT = 3; 
	private static final int DEFAULT_CONNECT_TIMEOUT = 3000; 
	private static final int DEFAULT_SOCKET_TIMEOUT = 12000; 
	private static final int DEFAULT_POOL_SHUTDOWN_DELAY = 60000; 
	private static final boolean DEFAULT_LOCAL_DC_AFFINITY = true; 
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
	private boolean localDcAffinity = DEFAULT_LOCAL_DC_AFFINITY; 
	
	private RetryPolicyFactory retryFactory = new RetryPolicyFactory() {

		@Override
		public RetryPolicy getRetryPolicy() {
			return new RunOnce();
		}
	};
	
	private ErrorRateMonitorConfigImpl errorRateConfig = new ErrorRateMonitorConfigImpl();
	
	public ConnectionPoolConfigurationImpl(String name) {
		this.name = name;
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
	public ErrorRateMonitorConfig getErrorCheckConfig() {
		return errorRateConfig;
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


	public ConnectionPoolConfigurationImpl setRetryPolicyFactory(RetryPolicyFactory factory) {
		this.retryFactory = factory;
		return this;
	}


	public ConnectionPoolConfigurationImpl setPoolShutdownDelay(int shutdownDelaySeconds) {
		poolShutdownDelay = shutdownDelaySeconds;
		return this;
	}
	
	public ConnectionPoolConfigurationImpl setLocalDcAffinity(boolean condition) {
		localDcAffinity = condition;
		return this;
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

	public static class ErrorRateMonitorConfigImpl implements ErrorRateMonitorConfig {

		int window = 60; 
		int suppressWindow = 90; 
		int checkFrequency = 1;
		
		public ErrorRateMonitorConfigImpl() {
			
		}
		
		public ErrorRateMonitorConfigImpl(int w, int f, int s) {
			this.window = w;
			this.checkFrequency = f;
			this.suppressWindow = s;
		}
		
		private List<ErrorThreshold> thresholds = new ArrayList<ErrorThreshold>();
		
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
}
