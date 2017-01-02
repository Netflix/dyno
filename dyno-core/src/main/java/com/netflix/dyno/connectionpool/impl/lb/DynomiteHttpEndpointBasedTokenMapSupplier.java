package com.netflix.dyno.connectionpool.impl.lb;

import java.util.Iterator;
import java.util.Set;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.impl.lb.HttpEndpointBasedTokenMapSupplier;

/**
 * 
 * Class to dynomite api REST
 * 
 */
public class DynomiteHttpEndpointBasedTokenMapSupplier extends HttpEndpointBasedTokenMapSupplier {

    private static final Logger Logger = LoggerFactory.getLogger(DynomiteHttpEndpointBasedTokenMapSupplier.class);
    
    private static final String DEFAULT_URL = "http://{hostname}:{port}/cluster_describe";
    private static final Integer DEFAULT_PORT = 22222;

    public DynomiteHttpEndpointBasedTokenMapSupplier() {
        super(DEFAULT_URL,DEFAULT_PORT);
    }
    
    public DynomiteHttpEndpointBasedTokenMapSupplier(String localDatacenter, String localRack) {
        super(DEFAULT_URL,DEFAULT_PORT,localDatacenter,localRack);
    }

    public DynomiteHttpEndpointBasedTokenMapSupplier(int port) {
        super(port);
    }
    
    public DynomiteHttpEndpointBasedTokenMapSupplier(String url, int port) {
        super(url, port);
    }

    public DynomiteHttpEndpointBasedTokenMapSupplier(String url, int port, String localDatacenter, String localRack) {
        super(url, port,localDatacenter,localRack);
    }

    @Override
    public String getTopologyJsonPayload(String hostname) {
        return dynomiteTrans(super.getTopologyJsonPayload(hostname));
    }

    @Override
    public String getTopologyJsonPayload(Set<Host> activeHosts) {
        return dynomiteTrans(super.getTopologyJsonPayload(activeHosts));
    }

    @SuppressWarnings("unchecked")
    public String dynomiteTrans(String json) {
        JSONArray ja = new JSONArray();

        JSONParser parser = new JSONParser();
        try {
            JSONObject jitem = (JSONObject) parser.parse(json);
            JSONArray arr = (JSONArray) jitem.get("dcs");
            Iterator<?> iter = arr.iterator();
            while (iter.hasNext()) {
                Object item = iter.next();
                if (!(item instanceof JSONObject)) {
                    continue;
                }
                JSONObject jItem = (JSONObject) item;
                String datacenter = (String) jItem.get("name");
                JSONArray racks = (JSONArray) jItem.get("racks");

                Iterator<?> racksI = racks.iterator();
                while (racksI.hasNext()) {
                    Object rackItem = racksI.next();
                    if (!(rackItem instanceof JSONObject)) {
                        continue;
                    }
                    JSONObject jRackItem = (JSONObject) rackItem;
                    String rackName = (String) jRackItem.get("name");
                    JSONArray servers = (JSONArray) jRackItem.get("servers");

                    Iterator<?> serversI = servers.iterator();
                    while (serversI.hasNext()) {
                        Object serverItem = serversI.next();
                        if (!(serverItem instanceof JSONObject)) {
                            continue;
                        }
                        JSONObject JServerItem = (JSONObject) serverItem;
                        String serverName = (String) JServerItem.get("name");
                        String serverHost = (String) JServerItem.get("host");
                        String serverPort = String.valueOf((Long) JServerItem.get("port"));
                        String serverToken = String.valueOf((Long) JServerItem.get("token"));

                        JSONObject jnode = new JSONObject();
                        jnode.put("token", serverToken);
                        jnode.put("serverName", serverName);
                        jnode.put("serverPort", serverPort);
                        jnode.put("hostname", serverHost);
                        jnode.put("rack", rackName);
                        jnode.put("ip", serverHost);
                        jnode.put("zone", rackName);
                        jnode.put("dc", datacenter);
                        ja.add(jnode);
                    }

                }

            }
        } catch (Exception lastEx) {
            Logger.error("Process error on dynomite rest json = [ " + json + " ]", lastEx);
            throw new DynoException(lastEx);
        }
        return ja.toJSONString();
    }

}