package com.netflix.dyno.demo.redis;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.demo.DynoDataBackfill;

@Path("/dyno/demo/redis")
public class DynoRedisDemoResource {

	private static final Logger Logger = LoggerFactory.getLogger(DynoRedisDemoResource.class);

	public DynoRedisDemoResource() {
	}

	@Path("/dataFill/start")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String startDataFill() throws Exception {

		Logger.info("Starting dyno data fill"); 
		try {
			DynoDataBackfill.Instance.backfill();
			return "data fill done!" + "\n";
		} catch (Exception e) {
			Logger.error("Error starting datafill", e);
			return "dataFill failed!";
		}
	}

	@Path("/cDataFill/start")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String conditionalBackfill() throws Exception {

		Logger.info("Starting dyno data fill"); 
		try {
			DynoDataBackfill.Instance.conditionalBackfill();
			return "data fill done!" + "\n";
		} catch (Exception e) {
			Logger.error("Error starting datafill", e);
			return "dataFill failed!";
		}
	}
	
	@Path("/dataFill/stop")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String stopDataFill() throws Exception {

		Logger.info("stop dyno data fill"); 
		try {
			DynoDataBackfill.Instance.stopBackfill();
			return "data fill stop!" + "\n";
		} catch (Exception e) {
			Logger.error("Error stop datafill", e);
			return "dataFill failed!";
		}
	}
	
	@Path("/keyDistribution")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String keyDistribution() throws Exception {

		Logger.info("key distribution"); 
		try {
			String s = DynoRedisDriver.getInstance().keyDistribution();
			return "key distribution" + "\n" + s;
		} catch (Exception e) {
			Logger.error("Error ", e);
			return " failed!";
		}
	}
	
	@Path("/keyHash")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String keyHash() throws Exception {

		Logger.info("key distribution"); 
		try {
			String s = DynoRedisDriver.getInstance().keyHash();
			return "key distribution" + "\n" + s;
		} catch (Exception e) {
			Logger.error("Error ", e);
			return " failed!";
		}
	}

	@Path("/dataFill/randomWrites")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String startRandomWrites() throws Exception {

		Logger.info("stop dyno data fill"); 
		try {
			DynoDataBackfill.Instance.randomWrites();
			return "data fill stop!" + "\n";
		} catch (Exception e) {
			Logger.error("Error stop datafill", e);
			return "dataFill failed!";
		}
	}
	
	@Path("/dataFill/shutdown")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String shutdownDataFill() throws Exception {

		Logger.info("shutdown dyno data fill"); 
		try {
			DynoDataBackfill.Instance.shutdown();
			return "data fill stop!" + "\n";
		} catch (Exception e) {
			Logger.error("Error shutdown datafill", e);
			return "dataFill failed!";
		}
	}

	// ALL DYNO RESOURCES

	// ALL DYNO RESOURCES
	@Path("/init")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String dynoInit() throws Exception {

		DynoRedisDriver.getInstance().init();
		return "Dyno client inited!" + "\n";
	}

	@Path("/start")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String dynoStart() throws Exception {

		Logger.info("Starting dyno demo test"); 
		try {
			DynoRedisDriver.getInstance().start();
			return "Dyno test started!" + "\n";
		} catch (Exception e) {
			Logger.error("Error starting dyno test", e);
			return "dyno start failed! " + e.getMessage();
		}
	}

	@Path("/startReads")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String dynoStartReads() throws Exception {

		Logger.info("Starting dyno demo test"); 
		try {
			DynoRedisDriver.getInstance().startReads();
			return "Dyno test started!" + "\n";
		} catch (Exception e) {
			Logger.error("Error starting dyno test", e);
			return "dyno start failed! " + e.getMessage();
		}
	}

	@Path("/startWrites")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String dynoStartWrites() throws Exception {

		Logger.info("Starting dyno demo test"); 
		try {
			DynoRedisDriver.getInstance().startWrites();
			return "Dyno test started!" + "\n";
		} catch (Exception e) {
			Logger.error("Error starting dyno test", e);
			return "dyno start failed! " + e.getMessage();
		}
	}

	@Path("/stop")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String dynoStop() throws Exception {

		Logger.info("Stopping dyno demo test"); 
		try {
			DynoRedisDriver.getInstance().stop();
			return "Dyno test stopped!" + "\n";
		} catch (Exception e) {
			Logger.error("Error stopping dyno test", e);
			return "dyno stop failed! " + e.getMessage();
		}
	}


	@Path("/readSingle/{key}")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String readSingle(@PathParam("key") String key) throws Exception {

		try {
			return "\n" + DynoRedisDriver.getInstance().readSingle(key) + "\n";
		} catch (Exception e) {
			//Logger.error("ERROR: " +  e.getMessage());
			if (!(e instanceof DynoException)) {
				e.printStackTrace();
			}
			return "\ndyno readSingle failed! " + e.getMessage() + "\n";
		}
	}

	@Path("/writeSingle/{key}/{val}")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String writeSingle(@PathParam("key") String key, @PathParam("val") String value) throws Exception {

		try {
			return "\n" + DynoRedisDriver.getInstance().writeSingle(key, value) + "\n";
		} catch (Exception e) {
			Logger.error("ERROR: " +  e.getMessage());
			return "dyno writeSingle failed! " + e.getMessage();
		}
	}

	@Path("/status")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String dynoStatus() throws Exception {

		try {
			return DynoRedisDriver.getInstance().getStatus();
		} catch (Exception e) {
			Logger.error("Error getting dyno status", e);
			return "dyno status failed! " + e.getMessage();
		}
	}

	@Path("/removeOneHost")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String removeOneHost() throws Exception {

		return "Not Implemented!!" + "\n";
	}

}
