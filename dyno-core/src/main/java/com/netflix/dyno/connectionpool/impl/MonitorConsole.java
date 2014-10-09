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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionStats;

/**
 * Console that gives the admin insight into the current status of the Dyno {@link ConnectionPool}
 * 
 * @author poberai
 *
 */
public class MonitorConsole {

	private static final MonitorConsole Instance = new MonitorConsole();

	public static MonitorConsole getInstance() {
		return Instance;
	}

	private final ConcurrentHashMap<String, ConnectionPoolMonitor> cpMonitorConsole = new ConcurrentHashMap<String, ConnectionPoolMonitor>();
	
	private MonitorConsole() {
		
	}
	
	public String getMonitorNames() {
		return cpMonitorConsole.keySet().toString();
	}
	
	public void addMonitorConsole(String name, ConnectionPoolMonitor monitor) {
		cpMonitorConsole.put(name, monitor);
	}
	
	public String getMonitorStats(String name) {
		
		ConnectionPoolMonitor cpMonitor = cpMonitorConsole.get(name);
		if (cpMonitor == null) {
			return name + " NOT FOUND";
		}
		
		StringBuilder sb = new StringBuilder();
		
		 sb
		 .append("ConnectionPoolMonitor(")
         .append("\nConnections[" )
             .append("   created: " ).append(cpMonitor.getConnectionCreatedCount())
             .append(",  closed: "  ).append(cpMonitor.getConnectionClosedCount())
             .append(",  failed: "  ).append(cpMonitor.getConnectionCreateFailedCount())
             .append(",  borrowed: ").append(cpMonitor.getConnectionBorrowedCount())
             .append(",  returned: ").append(cpMonitor.getConnectionReturnedCount())
             
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
			 
			 if (host.getHostName().contains("AllHosts")) {
				 continue;
			 }
			 
			 HostConnectionStats hStats = hostStats.get(host);
			 sb.append("\nHost: " + host.getHostName() + ":" + host.getPort() + ":" + host.getRack() + "\t");
			 sb.append(" borrowed: " + hStats.getConnectionsBorrowed());
			 sb.append(" returned: " + hStats.getConnectionsReturned());
			 sb.append(" created: " + hStats.getConnectionsCreated());
			 sb.append(" closed: " + hStats.getConnectionsClosed());
			 sb.append(" failed: " + hStats.getConnectionsCreateFailed());
		 }
		 sb.append("\n");
		 
		 return sb.toString();
	}
}
