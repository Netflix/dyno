package com.netflix.dyno.connectionpool.impl.lb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
public abstract class AbstractTokenMapSupplier implements TokenMapSupplier {

	private static final Logger Logger = LoggerFactory.getLogger(AbstractTokenMapSupplier.class);
	
	private final String localZone;
	private final List<Host> hosts = new ArrayList<Host>();
	private int port; 

	public AbstractTokenMapSupplier() {
		localZone = System.getenv("EC2_AVAILABILITY_ZONE");
		port = -1;
	}

	public abstract String getTopologyJsonPayload();
	
	public abstract String getTopologyJsonPayload(String hostname);

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
	
	protected List<Host> getHosts() {
		return hosts;
	}
	
	@Override
	public List<HostToken> getTokens() {

		// Doing this since not all tokens are received from an individual call to a dynomite server
		// hence trying them all
		Set<HostToken> allTokens = new HashSet<HostToken>();
		
		for (Host host : hosts) {
			try {
				List<HostToken> hostTokens = parseTokenListFromJson(getTopologyJsonPayload((host.getHostName())));
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
		String jsonPayload = getTopologyJsonPayload();
		List<HostToken> hostTokens = parseTokenListFromJson(jsonPayload);
		
		return CollectionUtils.find(hostTokens, new Predicate<HostToken>() {

			@Override
			public boolean apply(HostToken x) {
				return x.getHost().getHostName().equals(host.getHostName());
			}
		});
	}
	
	private boolean isLocalZoneHost(Host host) {
		if (localZone == null || localZone.isEmpty()) {
			return true; // consider everything
		}
		return localZone.equalsIgnoreCase(host.getRack());
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
				
				Host host = new Host(hostname, port, Status.Up).setRack(zone);
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

		final String json = "[{\"token\":\"3051939411\",\"hostname\":\"ec2-54-237-143-4.compute-1.amazonaws.com\",\"dc\":\"florida\",\"ip\":\"54.237.143.4\",\"zone\":\"us-east-1d\",\"location\":\"us-east-1\"}\"," +
				"\"{\"token\":\"188627880\",\"hostname\":\"ec2-50-17-65-2.compute-1.amazonaws.com\",\"dc\":\"florida\",\"ip\":\"50.17.65.2\",\"zone\":\"us-east-1d\",\"location\":\"us-east-1\"},\"" +
				"\"{\"token\":\"2019187467\",\"hostname\":\"ec2-54-83-87-174.compute-1.amazonaws.com\",\"dc\":\"florida-v001\",\"ip\":\"54.83.87.174\",\"zone\":\"us-east-1c\",\"location\":\"us-east-1\"},\"" +
				"\"{\"token\":\"3450843231\",\"hostname\":\"ec2-54-81-138-73.compute-1.amazonaws.com\",\"dc\":\"florida-v001\",\"ip\":\"54.81.138.73\",\"zone\":\"us-east-1c\",\"location\":\"us-east-1\"},\""+
				"\"{\"token\":\"587531700\",\"hostname\":\"ec2-54-82-176-215.compute-1.amazonaws.com\",\"dc\":\"florida-v001\",\"ip\":\"54.82.176.215\",\"zone\":\"us-east-1c\",\"location\":\"us-east-1\"},\"" +
				"\"{\"token\":\"3101134286\",\"hostname\":\"ec2-54-82-83-115.compute-1.amazonaws.com\",\"dc\":\"florida-v000\",\"ip\":\"54.82.83.115\",\"zone\":\"us-east-1e\",\"location\":\"us-east-1\"},\"" +
				"\"{\"token\":\"237822755\",\"hostname\":\"ec2-54-211-220-55.compute-1.amazonaws.com\",\"dc\":\"florida-v000\",\"ip\":\"54.211.220.55\",\"zone\":\"us-east-1e\",\"location\":\"us-east-1\"},\"" +
				"\"{\"token\":\"1669478519\",\"hostname\":\"ec2-54-80-65-203.compute-1.amazonaws.com\",\"dc\":\"florida-v000\",\"ip\":\"54.80.65.203\",\"zone\":\"us-east-1e\",\"location\":\"us-east-1\"}]\"";

		private TokenMapSupplier testTokenMapSupplier = new AbstractTokenMapSupplier() {

			@Override
			public String getTopologyJsonPayload() {
				return json;
			}

			@Override
			public String getTopologyJsonPayload(String hostname) {
				return json;
			}
		};
		
		@Test
		public void testParseJson() throws Exception {

			List<Host> hostList = new ArrayList<Host>();
			
			hostList.add(new Host("ec2-54-237-143-4.compute-1.amazonaws.com", 11211, Status.Up));
			hostList.add(new Host("ec2-50-17-65-2.compute-1.amazonaws.com", 11211, Status.Up));
			hostList.add(new Host("ec2-54-83-87-174.compute-1.amazonaws.com", 11211, Status.Up));
			hostList.add(new Host("ec2-54-81-138-73.compute-1.amazonaws.com", 11211, Status.Up));
			hostList.add(new Host("ec2-54-82-176-215.compute-1.amazonaws.com", 11211, Status.Up));
			hostList.add(new Host("ec2-54-82-83-115.compute-1.amazonaws.com", 11211, Status.Up));
			hostList.add(new Host("ec2-54-211-220-55.compute-1.amazonaws.com", 11211, Status.Up));
			hostList.add(new Host("ec2-54-80-65-203.compute-1.amazonaws.com", 11211, Status.Up));
			
			testTokenMapSupplier.initWithHosts(hostList);
			
			List<HostToken> hTokens = testTokenMapSupplier.getTokens();
			Collections.sort(hTokens, new Comparator<HostToken>() {
				@Override
				public int compare(HostToken o1, HostToken o2) {
					return o1.getToken().compareTo(o2.getToken());
				}
			});
			
			Assert.assertTrue(hTokens.get(0).getToken().equals(188627880L));
			Assert.assertTrue(hTokens.get(0).getHost().getHostName().equals("ec2-50-17-65-2.compute-1.amazonaws.com"));
			Assert.assertTrue(hTokens.get(1).getToken().equals(237822755L));
			Assert.assertTrue(hTokens.get(1).getHost().getHostName().equals("ec2-54-211-220-55.compute-1.amazonaws.com"));
			Assert.assertTrue(hTokens.get(2).getToken().equals(587531700L));
			Assert.assertTrue(hTokens.get(2).getHost().getHostName().equals("ec2-54-82-176-215.compute-1.amazonaws.com"));
			Assert.assertTrue(hTokens.get(3).getToken().equals(1669478519L));
			Assert.assertTrue(hTokens.get(3).getHost().getHostName().equals("ec2-54-80-65-203.compute-1.amazonaws.com"));
			Assert.assertTrue(hTokens.get(4).getToken().equals(2019187467L));
			Assert.assertTrue(hTokens.get(4).getHost().getHostName().equals("ec2-54-83-87-174.compute-1.amazonaws.com"));
			Assert.assertTrue(hTokens.get(5).getToken().equals(3051939411L));
			Assert.assertTrue(hTokens.get(5).getHost().getHostName().equals("ec2-54-237-143-4.compute-1.amazonaws.com"));
			Assert.assertTrue(hTokens.get(6).getToken().equals(3101134286L));
			Assert.assertTrue(hTokens.get(6).getHost().getHostName().equals("ec2-54-82-83-115.compute-1.amazonaws.com"));
			Assert.assertTrue(hTokens.get(7).getToken().equals(3450843231L));
			Assert.assertTrue(hTokens.get(7).getHost().getHostName().equals("ec2-54-81-138-73.compute-1.amazonaws.com"));
		}
	}
}
