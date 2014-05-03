package com.netflix.dyno.demo;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/dyno/demo")
public class DynoDemoResource {

	private static final Logger Logger = LoggerFactory.getLogger(DynoDemoResource.class);

	public DynoDemoResource() {
	}

	@Path("/dataFill/start")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String startDataFill() throws Exception {

		Logger.info("Starting dyno data fill"); 
		try {
			new DynoMCacheBackfill().backfill();
			return "data fill done!" + "\n";
		} catch (Exception e) {
			Logger.error("Error starting datafill", e);
			return "dataFill failed!";
		}
	}
	
	
	// ALL DYNO RESOURCES
	@Path("/start")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String dynoStart() throws Exception {

		Logger.info("Starting dyno demo test"); 
		try {
			DynoDemo.getInstance().getDriver().start();
			return "Dyno test started!" + "\n";
		} catch (Exception e) {
			Logger.error("Error starting dyno test", e);
			return "dyno start failed! " + e.getMessage();
		}
	}
		
	@Path("/dyno/startReads")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String dynoStartReads() throws Exception {

		Logger.info("Starting dyno demo test"); 
		try {
			DynoDemo.getInstance().getDriver().startReads();
			return "Dyno test started!" + "\n";
		} catch (Exception e) {
			Logger.error("Error starting dyno test", e);
			return "dyno start failed! " + e.getMessage();
		}
	}
		
	@Path("/dyno/startWrites")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String dynoStartWrites() throws Exception {

		Logger.info("Starting dyno demo test"); 
		try {
			DynoDemo.getInstance().getDriver().startWrites();
			return "Dyno test started!" + "\n";
		} catch (Exception e) {
			Logger.error("Error starting dyno test", e);
			return "dyno start failed! " + e.getMessage();
		}
	}
		
	@Path("/dyno/stop")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String dynoStop() throws Exception {

		Logger.info("Stopping dyno demo test"); 
		try {
			DynoDemo.getInstance().getDriver().stop();
			return "Dyno test stopped!" + "\n";
		} catch (Exception e) {
			Logger.error("Error stopping dyno test", e);
			return "dyno stop failed! " + e.getMessage();
		}
	}

	@Path("/dyno/status")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String dynoStatus() throws Exception {

		Logger.info("Stating dyno data fill"); 
		try {
			return DynoDemo.getInstance().getDriver().getStatus();
		} catch (Exception e) {
			Logger.error("Error getting dyno status", e);
			return "dyno status failed! " + e.getMessage();
		}
	}
}
