package com.netflix.dyno.connectionpool.impl.lb;

import java.util.*;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.Host.Status;

public class AbstractTokenMapSupplierTest {

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
        public String getTopologyJsonPayload(Set<Host> activeHosts) {
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

		List<HostToken> hTokens = testTokenMapSupplier.getTokens(new HashSet<Host>(hostList));
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
