package com.netflix.dyno.connectionpool.impl.lb;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
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

	public HttpEndpointBasedTokenMapSupplier() {
		this(DefaultServerUrl);
	}

	public HttpEndpointBasedTokenMapSupplier(String url) {
		serverUrl = url;
	}

	@Override
	public String getTopologyJsonPayload() {
		
		int count = NumRetries;
		Exception lastEx = null;
		
		String response = null;
		do {
			try {
				response = getResponseViaHttp(getRandomHost());
				if (response != null) {
					return response;
				}
			} catch (Exception e) {
				lastEx = e;
			} finally {
				count--;
			}
		} while ((count > 0) && (response == null));
		
		if (lastEx == null) {
			throw new RuntimeException(lastEx);
		} else {
			throw new RuntimeException("Could not contact dynomite for token map");
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
	
	private String getRandomHost() {
		Random random = new Random();
		
		List<Host> hostsUp = new ArrayList<Host>(CollectionUtils.filter(getHosts(), new Predicate<Host>() {

			@Override
			public boolean apply(Host x) {
				return x.isUp();
			}
		}));
		
		return hostsUp.get(random.nextInt(hostsUp.size())).getHostName();
	}
}
