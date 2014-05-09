package com.netflix.dyno.connectionpool.impl;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/dyno/console/monitor")
public class MonitorConsoleResource {

	private static final Logger Logger = LoggerFactory.getLogger(MonitorConsoleResource.class);

	public MonitorConsoleResource() {
		Logger.info("LOADED MonitorConsoleResource");
	}

	@Path("/names")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String getMonitorNames() {

		return MonitorConsole.getInstance().getMonitorNames();
	}
	
	 @Path("/stats/{monitorName}")
     @GET
	 @Consumes(MediaType.TEXT_PLAIN)
	 @Produces(MediaType.TEXT_PLAIN)
	 public String getMonitorStats(@PathParam("monitorName") String monitorName) {
		 
		 return MonitorConsole.getInstance().getMonitorStats(monitorName);
	 }
	
}
