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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.TokenPoolTopology;
import com.netflix.dyno.connectionpool.TokenPoolTopology.TokenStatus;

@Path("/dyno/console")
public class MonitorConsoleResource {

	private static final Logger Logger = LoggerFactory.getLogger(MonitorConsoleResource.class);

	public MonitorConsoleResource() {
		Logger.info("LOADED MonitorConsoleResource");
	}

	@Path("/monitors")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String getMonitorNames() {
		return MonitorConsole.getInstance().getMonitorNames();
	}

	@Path("/monitor/{monitorName}")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String getMonitorStats(@PathParam("monitorName") String monitorName) {

		return MonitorConsole.getInstance().getMonitorStats(monitorName);
	}

	@Path("/topologies")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.TEXT_PLAIN)
	public String getConnectionPoolNames() {
		return MonitorConsole.getInstance().getConnectionPoolNames().toString();
	}

	@SuppressWarnings("unchecked")
	@Path("/topology/{cpName}")
	@GET
	@Consumes(MediaType.TEXT_PLAIN)
	@Produces(MediaType.APPLICATION_JSON)
	public String getConnectionPoolToplogy(@PathParam("cpName") String cpName) {

		TokenPoolTopology topology = MonitorConsole.getInstance().getTopology(cpName);
		if (topology == null) {
			return "Not Found: " + cpName;
		}

		ConcurrentHashMap<String, List<TokenStatus>> map = topology.getAllTokens();

		JSONObject json = new JSONObject();

		for (String rack : map.keySet()) {
			List<TokenStatus> tokens = map.get(rack);
			json.put(rack, getTokenStatusMap(tokens));
		}
		return json.toJSONString();
	}

	private Map<String, String> getTokenStatusMap(List<TokenStatus> tokens) {

		Map<String, String> map = new HashMap<String, String>();
		for (TokenStatus tokenStatus : tokens) {
			String token = tokenStatus.getToken().toString();
			HostConnectionPool<?> hostPool = tokenStatus.getHostPool();
			String poolStatus = hostPool.getHost().getHostName() + "__" + (hostPool.isActive() ? "UP" : "DOWN");
			map.put(token, poolStatus);
		}
		return map;
	}

}
