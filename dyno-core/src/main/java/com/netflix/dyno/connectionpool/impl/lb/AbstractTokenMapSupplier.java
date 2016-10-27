/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.connectionpool.impl.lb;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.netflix.dyno.connectionpool.exception.TimeoutException;
import com.netflix.dyno.connectionpool.impl.utils.ConfigUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Predicate;

/**
 * An Example of the JSON payload that we get from a dynomite server [
 * {"token":"3051939411","hostname":"ec2-54-237-143-4.compute-1.amazonaws.com"
 * ,"dc":"florida","ip":"54.237.143.4", "zone":"us-east-1d",
 * "location":"us-east-1"}, {"token":"188627880","
 * hostname":"ec2-50-17-65-2.compute-1.amazonaws.com"
 * ,"dc":"florida","ip":"50.17.65.2", "zone":"us-east-1d",
 * "location":"us-east-1"},
 * {"token":"2019187467","hostname":"ec2-54-83-87-174.compute-1.amazonaws.com"
 * ,"dc":"florida-v001","ip":"54.83.87.174", "zone":"us-east-1c",
 * "location":"us-east-1"},
 * {"token":"3450843231","hostname":"ec2-54-81-138-73.compute-1.amazonaws.com"
 * ,"dc":"florida-v001","ip":"54.81.138.73", "zone":"us-east-1c",
 * "location":"us-east-1"}, {"token":"587531700","
 * hostname":"ec2-54-82-176-215.compute-1.amazonaws.com","dc":"florida-v001","ip":"54.82.176.215","zone":"us-east-1c",
 * "location":"us-east-1"},
 * {"token":"3101134286","hostname":"ec2-54-82-83-115.compute-1.amazonaws.com"
 * ,"dc":"florida-v000","ip":"54.82.83.115", "zone":"us-east-1e",
 * "location":"us-east-1"}, {"token":"237822755","
 * hostname":"ec2-54-211-220-55.compute-1.amazonaws.com","dc":"florida-v000","ip":"54.211.220.55","zone":"us-east-1e",
 * "location":"us-east-1"},
 * {"token":"1669478519","hostname":"ec2-54-80-65-203.compute-1.amazonaws.com"
 * ,"dc":"florida-v000","ip":"54.80.65.203", "zone":"us-east-1e",
 * "location":"us-east-1"} ]
 * 
 * @author poberai
 *
 */
public abstract class AbstractTokenMapSupplier implements TokenMapSupplier {

    private static final Logger Logger = LoggerFactory.getLogger(AbstractTokenMapSupplier.class);
    private final String localZone;
    private final String localDatacenter;
    private int unsuppliedPort = -1;

    public AbstractTokenMapSupplier(String localRack) {
        this.localZone = localRack;
        localDatacenter = ConfigUtils.getDataCenter();
    }
    public AbstractTokenMapSupplier(String localRack, int port){
        this.localZone = localRack;
        localDatacenter = ConfigUtils.getDataCenter();
        unsuppliedPort = port;
    }

    public AbstractTokenMapSupplier() {
        localZone = ConfigUtils.getLocalZone();
        localDatacenter = ConfigUtils.getDataCenter();
    }
    public AbstractTokenMapSupplier(int port) {
        localZone = ConfigUtils.getLocalZone();
        localDatacenter = ConfigUtils.getDataCenter();
        unsuppliedPort = port;
    }

    public abstract String getTopologyJsonPayload(Set<Host> activeHosts);

    public abstract String getTopologyJsonPayload(String hostname);

    @Override
    public List<HostToken> getTokens(Set<Host> activeHosts) {

	// Doing this since not all tokens are received from an individual call
	// to a dynomite server
	// hence trying them all
	Set<HostToken> allTokens = new HashSet<HostToken>();

	for (Host host : activeHosts) {
	    try {
		List<HostToken> hostTokens = parseTokenListFromJson(getTopologyJsonPayload((host.getHostAddress())));
		for (HostToken hToken : hostTokens) {
		    allTokens.add(hToken);
		}
	    } catch (Exception e) {
		Logger.warn("Could not get json response for token topology [" + e.getMessage() + "]");
	    }
	}
	return new ArrayList<HostToken>(allTokens);
    }

    @Override
    public HostToken getTokenForHost(final Host host, final Set<Host> activeHosts) {
	String jsonPayload;
	if (activeHosts.size() == 0) {
	    jsonPayload = getTopologyJsonPayload(host.getHostAddress());
	} else {
	    try {
		jsonPayload = getTopologyJsonPayload(activeHosts);
	    } catch (TimeoutException ex) {
		// Try using the host we just primed connections to. If that
		// fails,
		// let the exception bubble up to ConnectionPoolImpl which will
		// remove
		// the host from the host-mapping
		jsonPayload = getTopologyJsonPayload(host.getHostAddress());
	    }
	}
	List<HostToken> hostTokens = parseTokenListFromJson(jsonPayload);

	return CollectionUtils.find(hostTokens, new Predicate<HostToken>() {

	    @Override
	    public boolean apply(HostToken x) {
		return x.getHost().getHostAddress().equals(host.getHostName());
	    }
	});
    }

    private boolean isLocalZoneHost(Host host) {
	if (localZone == null || localZone.isEmpty()) {
	    Logger.warn("Local rack was not defined");
	    return true; // consider everything
	}
	return localZone.equalsIgnoreCase(host.getRack());
    }

    private boolean isLocalDatacenterHost(Host host) {

	if (localDatacenter == null || localDatacenter.isEmpty()) {
	    Logger.warn("Local Datacenter was not defined");
	    return true;
	}

	return localDatacenter.equalsIgnoreCase(host.getDatacenter());
    }

    // package-private for Test
    List<HostToken> parseTokenListFromJson(String json) {

	List<HostToken> hostTokens = new ArrayList<HostToken>();

	JSONParser parser = new JSONParser();
	try {
	    JSONArray arr = (JSONArray) parser.parse(json);

	    Iterator<?> iter = arr.iterator();
	    while (iter.hasNext()) {

		Object item = iter.next();
		if (!(item instanceof JSONObject)) {
		    continue;
		}
		JSONObject jItem = (JSONObject) item;

		Long token = Long.parseLong((String) jItem.get("token"));
		String hostname = (String) jItem.get("hostname");
		String zone = (String) jItem.get("zone");
		String portStr = (String) jItem.get("port");
                int port = unsuppliedPort;
                if(portStr != null){
                    port = Integer.valueOf(portStr);
                }

		Host host = new Host(hostname, port, zone,  Status.Up);

		if (isLocalDatacenterHost(host)) {
		    HostToken hostToken = new HostToken(token, host);
		    hostTokens.add(hostToken);
		}
	    }

	} catch (ParseException e) {
	    Logger.error("Failed to parse json response: " + json, e);
	    throw new RuntimeException(e);
	}

	return hostTokens;
    }
}
