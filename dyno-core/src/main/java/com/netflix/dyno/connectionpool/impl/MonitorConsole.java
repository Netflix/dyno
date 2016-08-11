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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.netflix.dyno.connectionpool.*;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;

/**
 * Console that gives the admin insight into the current status of the Dyno {@link ConnectionPool}
 * 
 * @author poberai
 *
 */
public class MonitorConsole implements MonitorConsoleMBean {

	private static final MonitorConsole Instance = new MonitorConsole();

    /*package*/ static final String OBJECT_NAME = "com.netflix.dyno.connectionpool.impl:type=MonitorConsole";

    public static MonitorConsole getInstance() {
		return Instance;
	}

	private final ConcurrentHashMap<String, ConnectionPoolMonitor> cpMonitors = new ConcurrentHashMap<String, ConnectionPoolMonitor>();
	private final ConcurrentHashMap<String, ConnectionPoolImpl<?>> connectionPools = new ConcurrentHashMap<String, ConnectionPoolImpl<?>>();
	
	private MonitorConsole() {
		
	}

    @Override
	public String getMonitorNames() {
		return cpMonitors.keySet().toString();
	}
	
	public void addMonitorConsole(String name, ConnectionPoolMonitor monitor) {
		cpMonitors.put(name, monitor);
	}
	
	public void registerConnectionPool(ConnectionPoolImpl<?> cp) {
        ConnectionPoolImpl<?> cpImpl = connectionPools.putIfAbsent(cp.getName(), cp);

        if (cpImpl != null) {
            // Need a unique id, so append a timestamp
            String name = cp.getName() + System.currentTimeMillis();
            connectionPools.put(name, cp);
            addMonitorConsole(name, cp.getMonitor());
        } else {
            addMonitorConsole(cp.getName(), cp.getMonitor());
        }

	}

    @Override
	public String getMonitorStats(String name) {
		
		ConnectionPoolMonitor cpMonitor = cpMonitors.get(name);
		if (cpMonitor == null) {
			return name + " NOT FOUND";
		}
		
		StringBuilder sb = new StringBuilder();
		
		 sb
		 .append("ConnectionPoolMonitor(")
         .append("\nConnections[" )
             .append("   created: " ).append(cpMonitor.getConnectionCreatedCount())
             .append(",  closed: "  ).append(cpMonitor.getConnectionClosedCount())
             .append(",  createFailed: "  ).append(cpMonitor.getConnectionCreateFailedCount())
             .append(",  borrowed: ").append(cpMonitor.getConnectionBorrowedCount())
             .append(",  returned: ").append(cpMonitor.getConnectionReturnedCount())
             .append(",  borrowedLatMean: ").append(cpMonitor.getConnectionBorrowedLatMean())
             .append(",  borrowedLatP99: ").append(cpMonitor.getConnectionBorrowedLatP99())
             
         .append("]\nOperations[")
             .append("   success=" ).append(cpMonitor.getOperationSuccessCount())
             .append(",  failure=" ).append(cpMonitor.getOperationFailureCount())
             .append(",  failover=").append(cpMonitor.getFailoverCount())
         .append("]\nHosts[")
             .append("   add="        ).append(cpMonitor.getHostUpCount())
             .append(",  down="       ).append(cpMonitor.getHostDownCount())
         .append("])");
		 
		 Map<Host, HostConnectionStats> hostStats = cpMonitor.getHostStats();
		 for (Host host : hostStats.keySet()) {
			 
			 if (host.getHostAddress().contains("AllHosts")) {
				 continue;
			 }
			 
			 HostConnectionStats hStats = hostStats.get(host);
			 sb.append("\nHost: " + host.getHostAddress() + ":" + host.getPort() + ":" + host.getRack() + "\t");
			 sb.append(" borrowed: " + hStats.getConnectionsBorrowed());
			 sb.append(" returned: " + hStats.getConnectionsReturned());
			 sb.append(" created: " + hStats.getConnectionsCreated());
			 sb.append(" closed: " + hStats.getConnectionsClosed());
			 sb.append(" createFailed: " + hStats.getConnectionsCreateFailed());
			 sb.append(" errors: " + hStats.getOperationErrorCount());
			 sb.append(" success: " + hStats.getOperationSuccessCount());
		 }
		 sb.append("\n");
		 
		 return sb.toString();
	}

	public TokenPoolTopology getTopology(String cpName) {
		ConnectionPoolImpl<?> pool = connectionPools.get(cpName);
		return (pool != null) ? pool.getTopology() : null;
	}

    @Override
    public Map<String, Map<String, List<String>>> getTopologySnapshot(String cpName) {
        Map<String, Map<String, List<String>>> snapshot =
                new HashMap<String, Map<String, List<String>>>();

        TokenPoolTopology topology = getTopology(cpName);

        if (topology == null) {
            return snapshot;
        }

        Map<String, List<TokenPoolTopology.TokenStatus>> map = topology.getAllTokens();

        for (String rack : map.keySet()) {
            List<TokenPoolTopology.TokenStatus> tokens = map.get(rack);
            snapshot.put(rack, getTokenStatusMap(tokens));
        }

        return snapshot;
    }

    @Override
    public Map<String, String> getRuntimeConfiguration(String cpName) {
        ConnectionPoolImpl<?> pool = connectionPools.get(cpName);

        if (pool != null && pool.getConfiguration() != null) {
            final ConnectionPoolConfiguration cpConfig = pool.getConfiguration();

            // Retain order for easy diffing across requests/nodes
            final Map<String, String> config = new LinkedHashMap<>();

            // Rather than use reflection to iterate and find getters, simply provide the base configuration
            config.put("localRack", cpConfig.getLocalRack());
            config.put("compressionStrategy", cpConfig.getCompressionStrategy().name());
            config.put("compressionThreshold", String.valueOf(cpConfig.getValueCompressionThreshold()));
            config.put("connectTimeout", String.valueOf(cpConfig.getConnectTimeout()));
            config.put("failOnStartupIfNoHosts", String.valueOf(cpConfig.getFailOnStartupIfNoHosts()));
            config.put("hostSupplier", cpConfig.getHostSupplier().toString());
            config.put("loadBalancingStrategy", cpConfig.getLoadBalancingStrategy().name());
            config.put("maxConnsPerHost", String.valueOf(cpConfig.getMaxConnsPerHost()));
            config.put("port", String.valueOf(cpConfig.getPort()));
            config.put("socketTimeout", String.valueOf(cpConfig.getSocketTimeout()));
            config.put("timingCountersResetFrequencyInSecs",
                    String.valueOf(cpConfig.getTimingCountersResetFrequencySeconds()));
            config.put("replicationFactor", String.valueOf(pool.getTopology().getReplicationFactor()));
            config.put("retryPolicy", pool.getConfiguration().getRetryPolicyFactory().getRetryPolicy().toString());
            config.put("localRackAffinity", String.valueOf(pool.getConfiguration().localZoneAffinity()));

            return Collections.unmodifiableMap(config);
        }

        return null;
    }

    private Map<String, List<String>> getTokenStatusMap(List<TokenPoolTopology.TokenStatus> tokens) {
        Map<String, List<String>> map = new HashMap<String, List<String>>();
        for (TokenPoolTopology.TokenStatus tokenStatus : tokens) {
            String token = tokenStatus.getToken().toString();
            HostConnectionPool<?> hostPool = tokenStatus.getHostPool();

            List<String> meta = CollectionUtils.newArrayList(
                    hostPool.getHost().getHostAddress(),
                    hostPool.isActive() ? "UP" : "DOWN"
            );

            map.put(token, meta);
        }
        return map;
    }

}
