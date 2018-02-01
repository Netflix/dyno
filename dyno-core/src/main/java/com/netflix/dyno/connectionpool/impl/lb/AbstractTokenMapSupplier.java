/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
 * An Example of the JSON payload that we get from dynomite-manager (this will eventually be changed so that the call
 * is made directly to Dynomite)
 * <pre>
 * [
 *  {
 *      "dc": "eu-west-1",
 *      "hostname": "ec2-52-208-92-24.eu-west-1.compute.amazonaws.com",
 *      "ip": "52.208.92.24",
 *      "rack": "dyno_sandbox--euwest1c",
 *      "token": "1383429731",
 *      "zone": "eu-west-1c",
 *      "hashtag" : "{}"
 *  },
 *  {
 *      "dc": "us-east-1",
 *      "hostname": "ec2-52-90-147-135.compute-1.amazonaws.com",
 *      "ip": "52.90.147.135",
 *      "rack": "dyno_sandbox--useast1c",
 *      "token": "1383429731",
 *      "zone": "us-east-1c",
 *      "hashtag" : "{}"
 *  },
 *  {
 *      "dc": "us-east-1",
 *      "hostname": "ec2-52-23-207-227.compute-1.amazonaws.com",
 *      "ip": "52.23.207.227",
 *      "rack": "dyno_sandbox--useast1e",
 *      "token": "1383429731",
 *      "zone": "us-east-1e",
 *      "hashtag" : "{}"      
 *  },
 *  {
 *      "dc": "eu-west-1",
 *      "hostname": "ec2-52-209-165-110.eu-west-1.compute.amazonaws.com",
 *      "ip": "52.209.165.110",
 *      "rack": "dyno_sandbox--euwest1a",
 *      "token": "1383429731",
 *      "zone": "eu-west-1a",
 *      "hashtag" : "{}"
 *  },
 *  {
 *      "dc": "eu-west-1",
 *      "hostname": "ec2-52-16-89-77.eu-west-1.compute.amazonaws.com",
 *      "ip": "52.16.89.77",
 *      "rack": "dyno_sandbox--euwest1b",
 *      "token": "1383429731",
 *      "zone": "eu-west-1b",
 *      "hashtag" : "{}"
      
 *  },
 *  {
 *      "dc": "us-east-1",
 *      "hostname": "ec2-54-208-235-30.compute-1.amazonaws.com",
 *      "ip": "54.208.235.30",
 *      "rack": "dyno_sandbox--useast1d",
 *      "token": "1383429731",
 *      "zone": "us-east-1d",
 *      "hashtag" : "{}"

 *  }
 *]
 * </pre>
 *
 * @author poberai
 * @author ipapapa
 *
 */
public abstract class AbstractTokenMapSupplier implements TokenMapSupplier {

    private static final Logger Logger = LoggerFactory.getLogger(AbstractTokenMapSupplier.class);
    private final String localZone;
    private final String localDatacenter;
    private final int dynomitePort;

    public AbstractTokenMapSupplier(String localRack, int dynomitePort) {
        this(localRack, ConfigUtils.getDataCenter(), dynomitePort);
    }

    public AbstractTokenMapSupplier(int dynomitePort) {
        this(ConfigUtils.getLocalZone(), ConfigUtils.getDataCenter(), dynomitePort);
    }

    public AbstractTokenMapSupplier(String localRack, String localDatacenter, int dynomitePort) {
        this.localZone = localRack;
        this.localDatacenter = localDatacenter;
        this.dynomitePort = dynomitePort;
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
        return new ArrayList<>(allTokens);
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
                return x.getHost().compareTo(host) == 0;
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
                String ipAddress = (String) jItem.get("ip");
                String zone = (String) jItem.get("zone");
                String datacenter = (String) jItem.get("dc");
                String portStr = (String) jItem.get("port");
                String hashtag = (String) jItem.get("hashtag");
                int port = dynomitePort;
                if (portStr != null) {
                    port = Integer.valueOf(portStr);
                }

                Host host = new Host(hostname, ipAddress, port, zone, datacenter, Status.Up, hashtag);

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
