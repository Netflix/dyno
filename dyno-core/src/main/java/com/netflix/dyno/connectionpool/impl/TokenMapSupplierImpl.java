package com.netflix.dyno.connectionpool.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

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
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.HostToken;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
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

	private final ConcurrentHashMap<String, Host> hostMap = new ConcurrentHashMap<String, Host>();
	private final ConcurrentHashMap<String, HostToken> hostTokenMap = new ConcurrentHashMap<String, HostToken>();
	
	private final String localZone;
	
	public TokenMapSupplierImpl(HostSupplier hSupplier) {
		
		for (Host host : hSupplier.getHosts()) {
			hostMap.put(host.getHostName(), host);
		}
		localZone = System.getenv("EC2_AVAILABILITY_ZONE");
	}
	

	@Override
	public List<HostToken> getTokens() {

		if (hostTokenMap.size() == 0) {
			String jsonPayload = getHttpResponseWithRetries();
			parseTokenListFromJson(jsonPayload);
		}
		
		return new ArrayList<HostToken>(hostTokenMap.values());
	}
	
	public HostToken getTokenForHost(Host host) {
		HostToken hostToken = hostTokenMap.get(host.getHostName());
		if (hostToken == null) {
			// we haven't seen this host before, add to map
			hostMap.put(host.getHostName(), host);
			// refresh token map from backend hosts
			String jsonPayload = getHttpResponseWithRetries();
			parseTokenListFromJson(jsonPayload);
		}
		return hostTokenMap.get(host.getHostName());
	}
	
	private String getHttpResponseWithRetries() {

		int count = NumRetries;
		Exception lastEx = null;
		
		String response = null;
		do {
			try {
				response = getResponseViaHttp();
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
		
	private String getResponseViaHttp() throws Exception {
		
		String url = ServerUrl;
		url = url.replace("{hostname}", getRandomHost());
		
		Logger.info("Making http call to url: " + url);
		
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
		
		List<String> list = new ArrayList<String>();
		for (Host host : hostMap.values()) {
			
			if (host.isUp() && isLocalZoneHost(host)) {
				list.add(host.getHostName());
			}
		}
		return list.get(random.nextInt(list.size()));
	}
	
	private boolean isLocalZoneHost(Host host) {
		if (localZone == null || localZone.isEmpty()) {
			return true; // consider everything
		}
		return localZone.equalsIgnoreCase(host.getDC());
	}


	private void parseTokenListFromJson(String json) {
		
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
				
				Host host = hostMap.get(hostname);
				if (host == null) {
					Logger.warn("Found a token that I do not recognize .. ignoring token: " + jItem.toString());
					continue;  
				}
				
				HostToken hostToken = new HostToken(token, host);
				hostTokenMap.put(hostname, hostToken);
			}
			
		} catch (ParseException e) {
			Logger.error("Failed to parse json response: " + json, e);
			throw new RuntimeException(e);
		}

		return;
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
			
			HostSupplier mockSupplier = mock(HostSupplier.class);
			when(mockSupplier.getHosts()).thenReturn(hostList);
			
			TokenMapSupplierImpl tokenSupplier = new TokenMapSupplierImpl(mockSupplier);
			tokenSupplier.parseTokenListFromJson(json);
			List<HostToken> hTokens = new ArrayList<HostToken>(tokenSupplier.hostTokenMap.values());
			
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
