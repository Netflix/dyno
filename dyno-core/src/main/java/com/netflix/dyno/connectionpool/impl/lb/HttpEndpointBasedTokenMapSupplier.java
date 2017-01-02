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

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.TimeoutException;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Predicate;
import com.netflix.dyno.connectionpool.impl.utils.IOUtilities;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.params.HttpConnectionParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class HttpEndpointBasedTokenMapSupplier extends AbstractTokenMapSupplier {


    private static final Logger Logger = LoggerFactory.getLogger(HttpEndpointBasedTokenMapSupplier.class);

    private static final String DefaultServerUrl = "http://{hostname}:{port}/REST/v1/admin/cluster_describe";
    private static final Integer NUM_RETRIES_PER_NODE = 2;
    private static final Integer NUM_RETRIER_ACROSS_NODES = 2;
    private static final Integer defaultPort = 8080;

    private final String serverUrl;

    public HttpEndpointBasedTokenMapSupplier() {
        this(DefaultServerUrl, defaultPort);
    }
    
    public HttpEndpointBasedTokenMapSupplier(int port) {
	this(DefaultServerUrl, port);
    }
    
    public HttpEndpointBasedTokenMapSupplier(String url, int port,String localDatacenter,String localRack) {
    super(port,localDatacenter,localRack);
    serverUrl = transformUrl(url,port);
    }

    public HttpEndpointBasedTokenMapSupplier(String url, int port) {
	super(port);
	serverUrl = transformUrl(url,port);
    }
    
    /**
     * If no port is passed means -1 then we will substitute to defaultPort
     * else the passed one.
    */
    private static String transformUrl(String url,int port){
     url = url.replace("{port}", (port > -1) ? Integer.toString(port) : Integer.toString(defaultPort));
     return url;
    }

    @Override
    public String getTopologyJsonPayload(String hostname) {
	try {
	    return getResponseViaHttp(hostname);
	} catch (Exception e) {
	    throw new RuntimeException(e);
	}
    }
    
    /**
     * Tries to get topology information by randomly trying across nodes.
     */
    @Override
    public String getTopologyJsonPayload(Set<Host> activeHosts) {
	int count = NUM_RETRIER_ACROSS_NODES;
	String response;
	Exception lastEx = null;

	do {
	    try {
		response = getTopologyWithNodeRetry(activeHosts);
		if (response != null) {
		    return response;
		}
	    } catch (Exception e) {
		lastEx = e;
	    } finally {
		count--;
	    }
	} while ((count > 0));

	if (lastEx != null) {
	    if (lastEx instanceof ConnectTimeoutException) {
		throw new TimeoutException("Unable to obtain topology", lastEx);
	    }
	    throw new DynoException(lastEx);
	} else {
	    throw new DynoException("Could not contact dynomite for token map");
	}

    }

    private String getResponseViaHttp(String hostname) throws Exception {

	String url = serverUrl;
	url = url.replace("{hostname}", hostname);

	if (Logger.isDebugEnabled()) {
	    Logger.debug("Making http call to url: " + url);
	}

	DefaultHttpClient client = new DefaultHttpClient();
	client.getParams().setParameter(HttpConnectionParams.CONNECTION_TIMEOUT, 2000);
	client.getParams().setParameter(HttpConnectionParams.SO_TIMEOUT, 5000);

	DefaultHttpRequestRetryHandler retryhandler = new DefaultHttpRequestRetryHandler(NUM_RETRIER_ACROSS_NODES,
		true);
	client.setHttpRequestRetryHandler(retryhandler);

	HttpGet get = new HttpGet(url);

	HttpResponse response = client.execute(get);
	int statusCode = response.getStatusLine().getStatusCode();
	if (!(statusCode == 200)) {
	    Logger.error("Got non 200 status code from " + url);
	    return null;
	}

	InputStream in = null;
	try {
	    in = response.getEntity().getContent();
	    return IOUtilities.toString(in);
	} finally {
	    if (in != null) {
		in.close();
	    }
	}
    }

    /**
     * Finds a random host from the set of active hosts to perform
     * cluster_describe
     * 
     * @param activeHosts
     * @return a random host
     */
    private String getRandomHost(Set<Host> activeHosts) {
	Random random = new Random();

	List<Host> hostsUp = new ArrayList<Host>(CollectionUtils.filter(activeHosts, new Predicate<Host>() {

	    @Override
	    public boolean apply(Host x) {
		return x.isUp();
	    }
	}));

	return hostsUp.get(random.nextInt(hostsUp.size())).getHostAddress();
    }

    /**
     * Tries multiple nodes, and it only bubbles up the last node's exception.
     * We want to bubble up the exception in order for the last node to be
     * removed from the connection pool.
     * 
     * @param activeHosts
     * @return the topology from cluster_describe
     */
    private String getTopologyWithNodeRetry(Set<Host> activeHosts) {
	int count = NUM_RETRIES_PER_NODE;
	String nodeResponse;
	Exception lastEx;
	final String randomHost = getRandomHost(activeHosts);
	do {
	    try {
		lastEx = null;
		nodeResponse = getResponseViaHttp(randomHost);
		if (nodeResponse != null) {

		    Logger.info("Received topology from " + randomHost);
		    return nodeResponse;
		}
	    } catch (Exception e) {
		Logger.info("cannot get topology from : " + randomHost);
		lastEx = e;
	    } finally {
		count--;
	    }

	} while ((count > 0));

	if (lastEx != null) {
	    if (lastEx instanceof ConnectTimeoutException) {
		throw new TimeoutException("Unable to obtain topology", lastEx);
	    }
	    throw new DynoException(lastEx);
	} else {
	    throw new DynoException("Could not contact dynomite for token map");
	}

    }

}