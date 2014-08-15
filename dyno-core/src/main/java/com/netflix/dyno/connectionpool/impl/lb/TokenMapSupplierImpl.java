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
package com.netflix.dyno.connectionpool.impl.lb;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Predicate;
import com.netflix.dyno.connectionpool.impl.utils.IOUtilities;

/**
 * An Example of the JSON payload that we get from a dynomite server
 * [
 *   {"token":"3051939411","hostname":"ec2-54-237-143-4.compute-1.amazonaws.com" ,"dc":"florida","ip":"54.237.143.4",      "zone":"us-east-1d", "location":"us-east-1"},
 *   {"token":"188627880"," hostname":"ec2-50-17-65-2.compute-1.amazonaws.com"   ,"dc":"florida","ip":"50.17.65.2",        "zone":"us-east-1d", "location":"us-east-1"},
 *   {"token":"2019187467","hostname":"ec2-54-83-87-174.compute-1.amazonaws.com" ,"dc":"florida-v001","ip":"54.83.87.174", "zone":"us-east-1c", "location":"us-east-1"},
 *   {"token":"3450843231","hostname":"ec2-54-81-138-73.compute-1.amazonaws.com" ,"dc":"florida-v001","ip":"54.81.138.73", "zone":"us-east-1c", "location":"us-east-1"},
 *   {"token":"587531700"," hostname":"ec2-54-82-176-215.compute-1.amazonaws.com","dc":"florida-v001","ip":"54.82.176.215","zone":"us-east-1c", "location":"us-east-1"},
 *   {"token":"3101134286","hostname":"ec2-54-82-83-115.compute-1.amazonaws.com" ,"dc":"florida-v000","ip":"54.82.83.115", "zone":"us-east-1e", "location":"us-east-1"},
 *   {"token":"237822755"," hostname":"ec2-54-211-220-55.compute-1.amazonaws.com","dc":"florida-v000","ip":"54.211.220.55","zone":"us-east-1e", "location":"us-east-1"},
 *   {"token":"1669478519","hostname":"ec2-54-80-65-203.compute-1.amazonaws.com" ,"dc":"florida-v000","ip":"54.80.65.203", "zone":"us-east-1e", "location":"us-east-1"}
 * ]
 * 
 * @author poberai
 *
 */
public class TokenMapSupplierImpl implements TokenMapSupplier {

	private static final Logger Logger = LoggerFactory.getLogger(TokenMapSupplierImpl.class);
	
	private static final String ServerUrl = "http://{hostname}:8080/REST/v1/admin/cluster_describe";
	private static final Integer NumRetries = 2;

	private final String localZone;
	private final List<Host> hosts = new ArrayList<Host>();
	private int port; 
	
	public TokenMapSupplierImpl() {
		localZone = System.getenv("EC2_AVAILABILITY_ZONE");
		port = -1;
	}

	@Override
	public void initWithHosts(Collection<Host> hostList) {
		
		port = hostList.iterator().next().getPort();
		
		hosts.addAll(CollectionUtils.filter(hostList, new Predicate<Host>() {

			@Override
			public boolean apply(Host host) {
				return isLocalZoneHost(host);
			}
		}));
	}
	
	@Override
	public List<HostToken> getTokens() {

		// Doing this since not all tokens are received from an individual call to a dynomite server
		// hence trying them all
		Set<HostToken> allTokens = new HashSet<HostToken>();
		
		for (Host host : hosts) {
			try {
				List<HostToken> hostTokens = parseTokenListFromJson(getResponseViaHttp(host.getHostName()));
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
	public HostToken getTokenForHost(final Host host) {
		this.hosts.add(host);
		String jsonPayload = getHttpResponseWithRetries();
		List<HostToken> hostTokens = parseTokenListFromJson(jsonPayload);
		
		return CollectionUtils.find(hostTokens, new Predicate<HostToken>() {

			@Override
			public boolean apply(HostToken x) {
				return x.getHost().getHostName().equals(host.getHostName());
			}
		});
	}
	
	private String getHttpResponseWithRetries() {

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
		
	private String getResponseViaHttp(String hostname) throws Exception {
		
		String url = ServerUrl;
		url = url.replace("{hostname}", hostname);

		if (Logger.isDebugEnabled()) {
			Logger.debug("Making http call to url: " + url);
		}
		
		HttpClient client = new DefaultHttpClient();
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
		
		List<Host> hostsUp = new ArrayList<Host>(CollectionUtils.filter(hosts, new Predicate<Host>() {

			@Override
			public boolean apply(Host x) {
				return x.isUp();
			}
		}));
		
		return hostsUp.get(random.nextInt(hostsUp.size())).getHostName();
	}
	
	private boolean isLocalZoneHost(Host host) {
		if (localZone == null || localZone.isEmpty()) {
			return true; // consider everything
		}
		return localZone.equalsIgnoreCase(host.getDC());
	}


	private List<HostToken> parseTokenListFromJson(String json) {
		
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
				JSONObject jItem = (JSONObject)item;
				
				Long token = Long.parseLong((String)jItem.get("token"));
				String hostname = (String)jItem.get("hostname");
				String zone = (String)jItem.get("zone");
				
				Host host = new Host(hostname, port, Status.Up).setDC(zone);
				HostToken hostToken = new HostToken(token, host);
				hostTokens.add(hostToken);
			}
			
		} catch (ParseException e) {
			Logger.error("Failed to parse json response: " + json, e);
			throw new RuntimeException(e);
		}

		return hostTokens;
	}
	
	public static class UnitTest {

		@Test
		public void testParseJson() throws Exception {


			String json = "[{\"token\":\"3051939411\",\"hostname\":\"ec2-54-237-143-4.compute-1.amazonaws.com\",\"dc\":\"florida\",\"ip\":\"54.237.143.4\",\"zone\":\"us-east-1d\",\"location\":\"us-east-1\"}\"," +
					"\"{\"token\":\"188627880\",\"hostname\":\"ec2-50-17-65-2.compute-1.amazonaws.com\",\"dc\":\"florida\",\"ip\":\"50.17.65.2\",\"zone\":\"us-east-1d\",\"location\":\"us-east-1\"},\"" +
					"\"{\"token\":\"2019187467\",\"hostname\":\"ec2-54-83-87-174.compute-1.amazonaws.com\",\"dc\":\"florida-v001\",\"ip\":\"54.83.87.174\",\"zone\":\"us-east-1c\",\"location\":\"us-east-1\"},\"" +
					"\"{\"token\":\"3450843231\",\"hostname\":\"ec2-54-81-138-73.compute-1.amazonaws.com\",\"dc\":\"florida-v001\",\"ip\":\"54.81.138.73\",\"zone\":\"us-east-1c\",\"location\":\"us-east-1\"},\""+
					"\"{\"token\":\"587531700\",\"hostname\":\"ec2-54-82-176-215.compute-1.amazonaws.com\",\"dc\":\"florida-v001\",\"ip\":\"54.82.176.215\",\"zone\":\"us-east-1c\",\"location\":\"us-east-1\"},\"" +
					"\"{\"token\":\"3101134286\",\"hostname\":\"ec2-54-82-83-115.compute-1.amazonaws.com\",\"dc\":\"florida-v000\",\"ip\":\"54.82.83.115\",\"zone\":\"us-east-1e\",\"location\":\"us-east-1\"},\"" +
					"\"{\"token\":\"237822755\",\"hostname\":\"ec2-54-211-220-55.compute-1.amazonaws.com\",\"dc\":\"florida-v000\",\"ip\":\"54.211.220.55\",\"zone\":\"us-east-1e\",\"location\":\"us-east-1\"},\"" +
					"\"{\"token\":\"1669478519\",\"hostname\":\"ec2-54-80-65-203.compute-1.amazonaws.com\",\"dc\":\"florida-v000\",\"ip\":\"54.80.65.203\",\"zone\":\"us-east-1e\",\"location\":\"us-east-1\"}]\"";

			List<Host> hostList = new ArrayList<Host>();
			
			hostList.add(new Host("ec2-54-237-143-4.compute-1.amazonaws.com", 11211, Status.Up));
			hostList.add(new Host("ec2-50-17-65-2.compute-1.amazonaws.com", 11211, Status.Up));
			hostList.add(new Host("ec2-54-83-87-174.compute-1.amazonaws.com", 11211, Status.Up));
			hostList.add(new Host("ec2-54-81-138-73.compute-1.amazonaws.com", 11211, Status.Up));
			hostList.add(new Host("ec2-54-82-176-215.compute-1.amazonaws.com", 11211, Status.Up));
			hostList.add(new Host("ec2-54-82-83-115.compute-1.amazonaws.com", 11211, Status.Up));
			hostList.add(new Host("ec2-54-211-220-55.compute-1.amazonaws.com", 11211, Status.Up));
			hostList.add(new Host("ec2-54-80-65-203.compute-1.amazonaws.com", 11211, Status.Up));
			
			TokenMapSupplierImpl tokenSupplier = new TokenMapSupplierImpl();
			tokenSupplier.initWithHosts(hostList);
			
			List<HostToken> hTokens = tokenSupplier.parseTokenListFromJson(json);
			
			Assert.assertTrue(hTokens.get(0).getToken().equals(3051939411L));
			Assert.assertTrue(hTokens.get(0).getHost().getHostName().equals("ec2-54-237-143-4.compute-1.amazonaws.com"));
			Assert.assertTrue(hTokens.get(1).getToken().equals(188627880L));
			Assert.assertTrue(hTokens.get(1).getHost().getHostName().equals("ec2-50-17-65-2.compute-1.amazonaws.com"));
			Assert.assertTrue(hTokens.get(2).getToken().equals(2019187467L));
			Assert.assertTrue(hTokens.get(2).getHost().getHostName().equals("ec2-54-83-87-174.compute-1.amazonaws.com"));
			Assert.assertTrue(hTokens.get(3).getToken().equals(3450843231L));
			Assert.assertTrue(hTokens.get(3).getHost().getHostName().equals("ec2-54-81-138-73.compute-1.amazonaws.com"));
			Assert.assertTrue(hTokens.get(4).getToken().equals(587531700L));
			Assert.assertTrue(hTokens.get(4).getHost().getHostName().equals("ec2-54-82-176-215.compute-1.amazonaws.com"));
			Assert.assertTrue(hTokens.get(5).getToken().equals(3101134286L));
			Assert.assertTrue(hTokens.get(5).getHost().getHostName().equals("ec2-54-82-83-115.compute-1.amazonaws.com"));
			Assert.assertTrue(hTokens.get(6).getToken().equals(237822755L));
			Assert.assertTrue(hTokens.get(6).getHost().getHostName().equals("ec2-54-211-220-55.compute-1.amazonaws.com"));
			Assert.assertTrue(hTokens.get(7).getToken().equals(1669478519L));
			Assert.assertTrue(hTokens.get(7).getHost().getHostName().equals("ec2-54-80-65-203.compute-1.amazonaws.com"));
		}
	}
}
