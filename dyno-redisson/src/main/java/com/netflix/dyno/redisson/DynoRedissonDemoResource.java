package com.netflix.dyno.redisson;

import java.util.concurrent.atomic.AtomicReference;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Path("/dyno/demo/redisson")
public class DynoRedissonDemoResource {

	private static final Logger Logger = LoggerFactory.getLogger(DynoRedissonDemoResource.class);

	private static final AtomicReference<RedissonDemo> demo = new AtomicReference<RedissonDemo>(null);
	
	public DynoRedissonDemoResource() {
	}

	@Path("/start/{threads}/{loop}")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String startRedisson(@PathParam("threads") int nThreads, @PathParam("loop") int loop) throws Exception {

		Logger.info("Starting redisson demo"); 
		try {
			demo.set(new RedissonDemo(nThreads, loop));
			demo.get().run();
			return "redisson demo!" + "\n";
		} catch (Exception e) {
			Logger.error("Error starting datafill", e);
			return "redisson demo failed";
		}
	}

	@Path("/stop")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String stopRedisson() throws Exception {

		Logger.info("stopping redisson demo"); 
		try {
			demo.get().stop();
			return "stop!" + "\n";
		} catch (Exception e) {
			Logger.error("Error starting datafill", e);
			return "redisson demo failed";
		}
	}
}
