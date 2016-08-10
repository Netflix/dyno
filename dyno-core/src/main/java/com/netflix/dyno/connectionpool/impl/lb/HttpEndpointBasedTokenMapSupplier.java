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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.TimeoutException;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.params.HttpConnectionParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Predicate;
import com.netflix.dyno.connectionpool.impl.utils.IOUtilities;

public class HttpEndpointBasedTokenMapSupplier extends AbstractTokenMapSupplier {

	private static final Logger Logger = LoggerFactory.getLogger(HttpEndpointBasedTokenMapSupplier.class);
	
	private static final String DefaultServerUrl = "http://{hostname}:8080/REST/v1/admin/cluster_describe";
	private final String serverUrl;
	private static final Integer NumRetries = 2;

	public HttpEndpointBasedTokenMapSupplier(int port) {
		this(DefaultServerUrl, port);
	}

	public HttpEndpointBasedTokenMapSupplier(String url, int port) {
        super(port);
		serverUrl = url;
	}

	@Override
	public String getTopologyJsonPayload(Set<Host> activeHosts) {
		
		int count = NumRetries;
		Exception lastEx = null;

		String response;
        final String randomHost = getRandomHost(activeHosts);
		do {
			try {
                response = getResponseViaHttp(randomHost);
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
            Logger.warn("Unable to obtain topology for Host " + randomHost + ", error = " + lastEx.getMessage());
            if (lastEx instanceof ConnectTimeoutException) {
                throw new TimeoutException("Unable to obtain topology", lastEx);
            }
			throw new DynoException(lastEx);
		} else {
			throw new DynoException("Could not contact dynomite for token map");
		}
	}

	@Override
	public String getTopologyJsonPayload(String hostname) {
		try { 
			return getResponseViaHttp(hostname);
		} catch (Exception e) {
			throw new RuntimeException(e);
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
		
		DefaultHttpRequestRetryHandler retryhandler = new DefaultHttpRequestRetryHandler(NumRetries, true);
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
	
	private String getRandomHost(Set<Host> activeHosts) {
		Random random = new Random();
		
		List<Host> hostsUp = new ArrayList<Host>(CollectionUtils.filter(activeHosts, new Predicate<Host>() {

			@Override
			public boolean apply(Host x) {
				return x.isUp();
			}
		}));
		
		return hostsUp.get(random.nextInt(hostsUp.size())).getHostName();
	}
}
