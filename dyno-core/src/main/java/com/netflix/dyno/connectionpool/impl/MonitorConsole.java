package com.netflix.dyno.connectionpool.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionStats;

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
			 sb.append("\nHost: " + host.getHostName() + ":" + host.getPort() + ":" + host.getDC() + "\t");
			 sb.append(" borrowed: " + hStats.getConnectionsBorrowed());
			 sb.append(" returned: " + hStats.getConnectionsReturned());
			 sb.append(" created: " + hStats.getConnectionsCreated());
			 sb.append(" closed: " + hStats.getConnectionsClosed());
			 sb.append(" failed: " + hStats.getConnectionsCreateFailed());
		 }
		 
		 return sb.toString();
	}
}
