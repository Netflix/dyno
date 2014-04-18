package com.netflix.dyno.connectionpool.impl;

import java.util.ArrayList;
import java.util.List;

import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ErrorRateMonitorConfig;
import com.netflix.dyno.connectionpool.RetryPolicy;
import com.netflix.dyno.connectionpool.RetryPolicy.RetryPolicyFactory;

public class ConnectionPoolConfigurationImpl implements ConnectionPoolConfiguration {
	
	// DEFAULTS 
	private static final int DEFAULT_PORT = 11211; 
	private static final int DEFAULT_MAX_CONNS_PER_HOST = 3; 
	private static final int DEFAULT_MAX_TIMEOUT_WHEN_EXHAUSTED = 2000; 
	private static final int DEFAULT_MAX_FAILOVER_COUNT = 3; 
	private static final int DEFAULT_CONNECT_TIMEOUT = 3000; 
	private static final int DEFAULT_SOCKET_TIMEOUT = 12000; 

	private final String name;
	private int port = DEFAULT_PORT; 
	private int maxConnsPerHost = DEFAULT_MAX_CONNS_PER_HOST; 
	private int maxTimeoutWhenExhausted = DEFAULT_MAX_TIMEOUT_WHEN_EXHAUSTED; 
	private int maxFailoverCount = DEFAULT_MAX_FAILOVER_COUNT; 
	private int connectTimeout = DEFAULT_CONNECT_TIMEOUT; 
	private int socketTimeout = DEFAULT_SOCKET_TIMEOUT; 
	
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

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public int getConnectTimeout() {
		return connectTimeout;
	}

	@Override
	public int getSocketTimeout() {
		return socketTimeout;
	}

	// ALL SETTERS
	public void setMaxConnsPerHost(int maxConnsPerHost) {
		this.maxConnsPerHost = maxConnsPerHost;
	}

	public void setMaxTimeoutWhenExhausted(int maxTimeoutWhenExhausted) {
		this.maxTimeoutWhenExhausted = maxTimeoutWhenExhausted;
	}

	public void setMaxFailoverCount(int maxFailoverCount) {
		this.maxFailoverCount = maxFailoverCount;
	}

	public void setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	@Override
	public ErrorRateMonitorConfig getErrorCheckConfig() {
		return errorRateConfig;
	}
	
	public static class ErrorRateMonitorConfigImpl implements ErrorRateMonitorConfig {

		int window = 60; 
		int suppressWindow = 90; 
		int checkFrequency = 1;
		
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

	@Override
	public RetryPolicyFactory getRetryPolicyFactory() {
		return retryFactory;
	}
	
	public ConnectionPoolConfigurationImpl setRetryPolicyFactory(RetryPolicyFactory factory) {
		this.retryFactory = factory;
		return this;
	}

}
