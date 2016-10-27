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
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;

public class TokenMapSupplierTest {

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

		hostList.add(new Host("ec2-54-237-143-4.compute-1.amazonaws.com", 11211, "us-east-1d", Status.Up));
		hostList.add(new Host("ec2-50-17-65-2.compute-1.amazonaws.com", 11211, "us-east-1d", Status.Up));
		hostList.add(new Host("ec2-54-83-87-174.compute-1.amazonaws.com", 11211, "us-east-1c", Status.Up));
		hostList.add(new Host("ec2-54-81-138-73.compute-1.amazonaws.com", 11211, "us-east-1c", Status.Up));
		hostList.add(new Host("ec2-54-82-176-215.compute-1.amazonaws.com", 11211, "us-east-1c", Status.Up));
		hostList.add(new Host("ec2-54-82-83-115.compute-1.amazonaws.com", 11211, "us-east-1e", Status.Up));
		hostList.add(new Host("ec2-54-211-220-55.compute-1.amazonaws.com", 11211, "us-east-1e", Status.Up));
		hostList.add(new Host("ec2-54-80-65-203.compute-1.amazonaws.com", 11211, "us-east-1e", Status.Up));

        HttpEndpointBasedTokenMapSupplier tokenSupplier = new HttpEndpointBasedTokenMapSupplier("us-east-1d", 11211);

		List<HostToken> hTokens = tokenSupplier.parseTokenListFromJson(json);

		Assert.assertTrue(hTokens.get(0).getToken().equals(3051939411L));
		Assert.assertTrue(hTokens.get(0).getHost().getHostAddress().equals("ec2-54-237-143-4.compute-1.amazonaws.com"));
		Assert.assertTrue(hTokens.get(1).getToken().equals(188627880L));
		Assert.assertTrue(hTokens.get(1).getHost().getHostAddress().equals("ec2-50-17-65-2.compute-1.amazonaws.com"));
		Assert.assertTrue(hTokens.get(2).getToken().equals(2019187467L));
		Assert.assertTrue(hTokens.get(2).getHost().getHostAddress().equals("ec2-54-83-87-174.compute-1.amazonaws.com"));
		Assert.assertTrue(hTokens.get(3).getToken().equals(3450843231L));
		Assert.assertTrue(hTokens.get(3).getHost().getHostAddress().equals("ec2-54-81-138-73.compute-1.amazonaws.com"));
		Assert.assertTrue(hTokens.get(4).getToken().equals(587531700L));
		Assert.assertTrue(hTokens.get(4).getHost().getHostAddress().equals("ec2-54-82-176-215.compute-1.amazonaws.com"));
		Assert.assertTrue(hTokens.get(5).getToken().equals(3101134286L));
		Assert.assertTrue(hTokens.get(5).getHost().getHostAddress().equals("ec2-54-82-83-115.compute-1.amazonaws.com"));
		Assert.assertTrue(hTokens.get(6).getToken().equals(237822755L));
		Assert.assertTrue(hTokens.get(6).getHost().getHostAddress().equals("ec2-54-211-220-55.compute-1.amazonaws.com"));
		Assert.assertTrue(hTokens.get(7).getToken().equals(1669478519L));
		Assert.assertTrue(hTokens.get(7).getHost().getHostAddress().equals("ec2-54-80-65-203.compute-1.amazonaws.com"));
	}
	@Test
	public void testParseJsonWithPorts() throws Exception {


		String json = "[{\"token\":\"3051939411\",\"hostname\":\"ec2-54-237-143-4.compute-1.amazonaws.com\",\"port\":\"11211\",\"dc\":\"florida\",\"ip\":\"54.237.143.4\",\"zone\":\"us-east-1d\",\"location\":\"us-east-1\"}\"," +
				"\"{\"token\":\"188627880\",\"hostname\":\"ec2-54-237-143-4.compute-1.amazonaws.com\",\"port\":\"11212\",\"dc\":\"florida\",\"ip\":\"50.17.65.2\",\"zone\":\"us-east-1d\",\"location\":\"us-east-1\"},\"" +
				"\"{\"token\":\"2019187467\",\"hostname\":\"ec2-54-237-143-4.compute-1.amazonaws.com\",\"port\":\"11213\",\"dc\":\"florida-v001\",\"ip\":\"54.83.87.174\",\"zone\":\"us-east-1c\",\"location\":\"us-east-1\"},\"" +
				"\"{\"token\":\"3450843231\",\"hostname\":\"ec2-54-237-143-4.compute-1.amazonaws.com\",\"port\":\"11214\",\"dc\":\"florida-v001\",\"ip\":\"54.81.138.73\",\"zone\":\"us-east-1c\",\"location\":\"us-east-1\"},\""+
				"\"{\"token\":\"587531700\",\"hostname\":\"ec2-54-82-176-215.compute-1.amazonaws.com\",\"port\":\"11215\",\"dc\":\"florida-v001\",\"ip\":\"54.82.176.215\",\"zone\":\"us-east-1c\",\"location\":\"us-east-1\"},\"" +
				"\"{\"token\":\"3101134286\",\"hostname\":\"ec2-54-82-83-115.compute-1.amazonaws.com\",\"port\":\"11216\",\"dc\":\"florida-v000\",\"ip\":\"54.82.83.115\",\"zone\":\"us-east-1e\",\"location\":\"us-east-1\"},\"" +
				"\"{\"token\":\"237822755\",\"hostname\":\"ec2-54-211-220-55.compute-1.amazonaws.com\",\"port\":\"11217\",\"dc\":\"florida-v000\",\"ip\":\"54.211.220.55\",\"zone\":\"us-east-1e\",\"location\":\"us-east-1\"},\"" +
				"\"{\"token\":\"1669478519\",\"hostname\":\"ec2-54-80-65-203.compute-1.amazonaws.com\",\"port\":\"11218\",\"dc\":\"florida-v000\",\"ip\":\"54.80.65.203\",\"zone\":\"us-east-1e\",\"location\":\"us-east-1\"}]\"";

		List<Host> hostList = new ArrayList<Host>();

		hostList.add(new Host("ec2-54-237-143-4.compute-1.amazonaws.com", 11211, "us-east-1d", Status.Up));
		hostList.add(new Host("ec2-54-237-143-4.compute-1.amazonaws.com", 11212, "us-east-1d", Status.Up));
		hostList.add(new Host("ec2-54-237-143-4.compute-1.amazonaws.com", 11213, "us-east-1c", Status.Up));
		hostList.add(new Host("ec2-54-237-143-4.compute-1.amazonaws.com", 11214, "us-east-1c", Status.Up));
		hostList.add(new Host("ec2-54-82-176-215.compute-1.amazonaws.com", 11215, "us-east-1c", Status.Up));
		hostList.add(new Host("ec2-54-82-83-115.compute-1.amazonaws.com", 11216, "us-east-1e", Status.Up));
		hostList.add(new Host("ec2-54-211-220-55.compute-1.amazonaws.com", 11217, "us-east-1e", Status.Up));
		hostList.add(new Host("ec2-54-80-65-203.compute-1.amazonaws.com", 11218, "us-east-1e", Status.Up));

        HttpEndpointBasedTokenMapSupplier tokenSupplier = new HttpEndpointBasedTokenMapSupplier("us-east-1d", 11211);

		List<HostToken> hTokens = tokenSupplier.parseTokenListFromJson(json);

		Assert.assertTrue(hTokens.get(0).getToken().equals(3051939411L));
		Assert.assertTrue(hTokens.get(0).getHost().getHostAddress().equals("ec2-54-237-143-4.compute-1.amazonaws.com"));
		Assert.assertEquals(hTokens.get(0).getHost().getPort(), 11211);
		
		Assert.assertTrue(hTokens.get(1).getToken().equals(188627880L));
		Assert.assertTrue(hTokens.get(1).getHost().getHostAddress().equals("ec2-54-237-143-4.compute-1.amazonaws.com"));
		Assert.assertEquals(hTokens.get(1).getHost().getPort(), 11212);
		
		Assert.assertTrue(hTokens.get(2).getToken().equals(2019187467L));
		Assert.assertTrue(hTokens.get(2).getHost().getHostAddress().equals("ec2-54-237-143-4.compute-1.amazonaws.com"));
		Assert.assertEquals(hTokens.get(2).getHost().getPort(), 11213);
		
		Assert.assertTrue(hTokens.get(3).getToken().equals(3450843231L));
		Assert.assertTrue(hTokens.get(3).getHost().getHostAddress().equals("ec2-54-237-143-4.compute-1.amazonaws.com"));
		Assert.assertEquals(hTokens.get(3).getHost().getPort(), 11214);
		
		Assert.assertTrue(hTokens.get(4).getToken().equals(587531700L));
		Assert.assertTrue(hTokens.get(4).getHost().getHostAddress().equals("ec2-54-82-176-215.compute-1.amazonaws.com"));
		Assert.assertEquals(hTokens.get(4).getHost().getPort(), 11215);
		
		Assert.assertTrue(hTokens.get(5).getToken().equals(3101134286L));
		Assert.assertTrue(hTokens.get(5).getHost().getHostAddress().equals("ec2-54-82-83-115.compute-1.amazonaws.com"));
		Assert.assertEquals(hTokens.get(5).getHost().getPort(), 11216);
		
		Assert.assertTrue(hTokens.get(6).getToken().equals(237822755L));
		Assert.assertTrue(hTokens.get(6).getHost().getHostAddress().equals("ec2-54-211-220-55.compute-1.amazonaws.com"));
		Assert.assertEquals(hTokens.get(6).getHost().getPort(), 11217);
		
		Assert.assertTrue(hTokens.get(7).getToken().equals(1669478519L));
		Assert.assertTrue(hTokens.get(7).getHost().getHostAddress().equals("ec2-54-80-65-203.compute-1.amazonaws.com"));
		Assert.assertEquals(hTokens.get(7).getHost().getPort(), 11218);
	}
}
