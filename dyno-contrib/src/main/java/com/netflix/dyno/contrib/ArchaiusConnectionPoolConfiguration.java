/*******************************************************************************
 * Copyright 2015 Netflix
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
package com.netflix.dyno.contrib;

import com.netflix.config.DynamicStringProperty;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.dyno.connectionpool.ErrorRateMonitorConfig;
import com.netflix.dyno.connectionpool.RetryPolicy.RetryPolicyFactory;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.RetryNTimes;
import com.netflix.dyno.connectionpool.impl.RunOnce;

public class ArchaiusConnectionPoolConfiguration extends ConnectionPoolConfigurationImpl {

    private static final Logger Logger = LoggerFactory.getLogger(ArchaiusConnectionPoolConfiguration.class);

    private static final String DynoPrefix = "dyno.";

    //private final DynamicIntProperty port;
    private final DynamicIntProperty maxConnsPerHost;
    private final DynamicIntProperty maxTimeoutWhenExhausted;
    private final DynamicIntProperty maxFailoverCount;
    private final DynamicIntProperty connectTimeout;
    private final DynamicIntProperty socketTimeout;
    private final DynamicBooleanProperty localZoneAffinity;
    private final DynamicIntProperty resetTimingsFrequency;
    private final DynamicStringProperty configPublisherConfig;
    private final DynamicIntProperty compressionThreshold;

    private final LoadBalancingStrategy loadBalanceStrategy;
    private final CompressionStrategy compressionStrategy;
    private final ErrorRateMonitorConfig errorRateConfig;
    private final RetryPolicyFactory retryPolicyFactory;
    private final DynamicBooleanProperty failOnStartupIfNoHosts;

    private DynamicBooleanProperty isDualWriteEnabled;
    private DynamicStringProperty dualWriteClusterName;
    private DynamicIntProperty dualWritePercentage;

    public ArchaiusConnectionPoolConfiguration(String name) {
        super(name);

        String propertyPrefix = DynoPrefix + name;

        maxConnsPerHost = DynamicPropertyFactory.getInstance().getIntProperty(propertyPrefix + ".connection.maxConnsPerHost", super.getMaxConnsPerHost());
        maxTimeoutWhenExhausted = DynamicPropertyFactory.getInstance().getIntProperty(propertyPrefix + ".connection.maxTimeoutWhenExhausted", super.getMaxTimeoutWhenExhausted());
        maxFailoverCount = DynamicPropertyFactory.getInstance().getIntProperty(propertyPrefix + ".connection.maxFailoverCount", super.getMaxFailoverCount());
        connectTimeout = DynamicPropertyFactory.getInstance().getIntProperty(propertyPrefix + ".connection.connectTimeout", super.getConnectTimeout());
        socketTimeout = DynamicPropertyFactory.getInstance().getIntProperty(propertyPrefix + ".connection.socketTimeout", super.getSocketTimeout());
        localZoneAffinity = DynamicPropertyFactory.getInstance().getBooleanProperty(propertyPrefix + ".connection.localZoneAffinity", super.localZoneAffinity());
        resetTimingsFrequency = DynamicPropertyFactory.getInstance().getIntProperty(propertyPrefix + ".connection.metrics.resetFrequencySeconds", super.getTimingCountersResetFrequencySeconds());
        configPublisherConfig = DynamicPropertyFactory.getInstance().getStringProperty(propertyPrefix + ".config.publisher.address", super.getConfigurationPublisherConfig());
        failOnStartupIfNoHosts = DynamicPropertyFactory.getInstance().getBooleanProperty(propertyPrefix + ".config.startup.failIfNoHosts", super.getFailOnStartupIfNoHosts());
        compressionThreshold = DynamicPropertyFactory.getInstance().getIntProperty(propertyPrefix + ".config.compressionThreshold", super.getValueCompressionThreshold());

        loadBalanceStrategy = parseLBStrategy(propertyPrefix);
        errorRateConfig = parseErrorRateMonitorConfig(propertyPrefix);
        retryPolicyFactory = parseRetryPolicyFactory(propertyPrefix);
        compressionStrategy = parseCompressionStrategy(propertyPrefix);

        isDualWriteEnabled = DynamicPropertyFactory.getInstance().getBooleanProperty(propertyPrefix + ".dualwrite.enabled", super.isDualWriteEnabled());
        dualWriteClusterName = DynamicPropertyFactory.getInstance().getStringProperty(propertyPrefix + ".dualwrite.cluster", super.getDualWriteClusterName());
        dualWritePercentage = DynamicPropertyFactory.getInstance().getIntProperty(propertyPrefix + ".dualwrite.percentage", super.getDualWritePercentage());
    }


    @Override
    public String getName() {
        return super.getName();
    }


    @Override
    public int getMaxConnsPerHost() {
        return maxConnsPerHost.get();
    }

    @Override
    public int getMaxTimeoutWhenExhausted() {
        return maxTimeoutWhenExhausted.get();
    }

    @Override
    public int getMaxFailoverCount() {
        return maxFailoverCount.get();
    }


    @Override
    public int getConnectTimeout() {
        return connectTimeout.get();
    }

    @Override
    public int getSocketTimeout() {
        return socketTimeout.get();
    }

    @Override
    public RetryPolicyFactory getRetryPolicyFactory() {
        return retryPolicyFactory;
    }

    @Override
    public boolean localZoneAffinity() {
        return localZoneAffinity.get();
    }

    @Override
    public LoadBalancingStrategy getLoadBalancingStrategy() {
        return loadBalanceStrategy;
    }

    @Override
    public CompressionStrategy getCompressionStrategy() {
        return compressionStrategy;
    }

    @Override
    public int getValueCompressionThreshold() {
        return compressionThreshold.get();
    }

    @Override
    public int getTimingCountersResetFrequencySeconds() {
        return resetTimingsFrequency.get();
    }

    @Override
    public String getConfigurationPublisherConfig() {
        return configPublisherConfig.get();
    }

    @Override
    public boolean getFailOnStartupIfNoHosts() {
        return failOnStartupIfNoHosts.get();
    }

    @Override
    public boolean isDualWriteEnabled() {
        return isDualWriteEnabled.get();
    }

    @Override
    public String getDualWriteClusterName() {
        return dualWriteClusterName.get();
    }

    @Override
    public int getDualWritePercentage() {
        return dualWritePercentage.get();
    }

    public void setIsDualWriteEnabled(DynamicBooleanProperty booleanProperty) {
        this.isDualWriteEnabled = booleanProperty;
    }

    public void setDualWriteClusterName(DynamicStringProperty stringProperty) {
        this.dualWriteClusterName = stringProperty;
    }

    public void setDualWritePercentage(DynamicIntProperty intProperty) {
        this.dualWritePercentage = intProperty;
    }

    @Override
    public String toString() {
        return "ArchaiusConnectionPoolConfiguration{" +
                "name=" + getName() +
                ", maxConnsPerHost=" + maxConnsPerHost +
                ", maxTimeoutWhenExhausted=" + maxTimeoutWhenExhausted +
                ", maxFailoverCount=" + maxFailoverCount +
                ", connectTimeout=" + connectTimeout +
                ", socketTimeout=" + socketTimeout +
                ", localZoneAffinity=" + localZoneAffinity +
                ", resetTimingsFrequency=" + resetTimingsFrequency +
                ", configPublisherConfig=" + configPublisherConfig +
                ", compressionThreshold=" + compressionThreshold +
                ", loadBalanceStrategy=" + loadBalanceStrategy +
                ", compressionStrategy=" + compressionStrategy +
                ", errorRateConfig=" + errorRateConfig +
                ", retryPolicyFactory=" + retryPolicyFactory +
                ", failOnStartupIfNoHosts=" + failOnStartupIfNoHosts +
                ", isDualWriteEnabled=" + isDualWriteEnabled +
                ", dualWriteClusterName=" + dualWriteClusterName +
                ", dualWritePercentage=" + dualWritePercentage +
                '}';
    }

    private LoadBalancingStrategy parseLBStrategy(String propertyPrefix) {

        LoadBalancingStrategy defaultConfig = super.getLoadBalancingStrategy();

        String cfg =
                DynamicPropertyFactory.getInstance().getStringProperty(propertyPrefix + ".lbStrategy", defaultConfig.name()).get();

        LoadBalancingStrategy lb = null;
        try {
            lb = LoadBalancingStrategy.valueOf(cfg);
        } catch (Exception e) {
            Logger.warn("Unable to parse LoadBalancingStrategy: " + cfg + ", switching to default: " + defaultConfig.name());
            lb = defaultConfig;
        }

        return lb;
    }

    private CompressionStrategy parseCompressionStrategy(String propertyPrefix) {

        CompressionStrategy defaultCompStrategy = super.getCompressionStrategy();

        String cfg = DynamicPropertyFactory
                .getInstance()
                .getStringProperty(propertyPrefix + ".compressionStrategy", defaultCompStrategy.name()).get();

        CompressionStrategy cs = null;
        try {
            cs = CompressionStrategy.valueOf(cfg);
            Logger.info("Dyno configuration: CompressionStrategy = " + cs.name());
        } catch (IllegalArgumentException ex) {
            Logger.warn("Unable to parse CompressionStrategy: " + cfg + ", switching to default: " + defaultCompStrategy.name());
            cs = defaultCompStrategy;
        }

        return cs;

    }

    private ErrorRateMonitorConfig parseErrorRateMonitorConfig(String propertyPrefix) {
        String errorRateConfig = DynamicPropertyFactory.getInstance().getStringProperty(propertyPrefix + ".errorRateConfig", null).get();
        try {
            if (errorRateConfig == null) {
                return null;
            }

            // Classic format that is supported is json.

            JSONObject json = new JSONObject(errorRateConfig);

            int window = json.getInt("window");
            int frequency = json.getInt("frequency");
            int suppress = json.getInt("suppress");

            ErrorRateMonitorConfigImpl configImpl = new ErrorRateMonitorConfigImpl(window, frequency, suppress);

            JSONArray thresholds = json.getJSONArray("thresholds");
            for (int i = 0; i < thresholds.length(); i++) {

                JSONObject tConfig = thresholds.getJSONObject(i);

                int rps = tConfig.getInt("rps");
                int seconds = tConfig.getInt("seconds");
                int coverage = tConfig.getInt("coverage");

                configImpl.addThreshold(rps, seconds, coverage);
            }

            return configImpl;

        } catch (Exception e) {
            Logger.warn("Failed to parse error rate config: " + errorRateConfig, e);
        }

        return new ErrorRateMonitorConfigImpl();
    }

    private RetryPolicyFactory parseRetryPolicyFactory(String propertyPrefix) {

        String retryPolicy = DynamicPropertyFactory.getInstance().getStringProperty(propertyPrefix + ".retryPolicy", "RunOnce").get();

        if (retryPolicy.equals("RunOnce")) {
            return new RunOnce.RetryFactory();
        }

        if (retryPolicy.startsWith("RetryNTimes")) {

            String[] parts = retryPolicy.split(":");

            if (parts.length < 2) {
                return new RunOnce.RetryFactory();
            }

            try {

                int n = Integer.parseInt(parts[1]);
                boolean allowFallback = false;
                if (parts.length == 3) {
                    allowFallback = Boolean.parseBoolean(parts[2]);
                }
                return new RetryNTimes.RetryFactory(n, allowFallback);

            } catch (Exception e) {
                return new RunOnce.RetryFactory();
            }
        }

        return new RunOnce.RetryFactory();
    }


}
