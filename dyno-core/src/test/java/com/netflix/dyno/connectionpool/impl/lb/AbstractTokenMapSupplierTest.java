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

import java.util.*;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.Host.Status;

public class AbstractTokenMapSupplierTest {

	// For some negative test cases, let the json payload have some nodes with wrong port
	final String json = "[{\"token\":\"3051939411\",\"hostname\":\"ec2-54-237-143-4.compute-1.amazonaws.com\",\"port\":\"22123\",\"dc\":\"us-east-1\",\"ip\":\"54.237.143.4\",\"zone\":\"us-east-1d\"}\"," +
			"\"{\"token\":\"188627880\",\"hostname\":\"ec2-50-17-65-2.compute-1.amazonaws.com\",\"port\":\"22122\",\"dc\":\"us-east-1\",\"ip\":\"50.17.65.2\",\"zone\":\"us-east-1d\"},\"" +
			"\"{\"token\":\"2019187467\",\"hostname\":\"ec2-54-83-87-174.compute-1.amazonaws.com\",\"port\":\"22122\",\"dc\":\"us-east-1\",\"ip\":\"54.83.87.174\",\"zone\":\"us-east-1c\" },\"" +
			// TEST WRONG RACK here.
			"\"{\"token\":\"3450843231\",\"hostname\":\"ec2-54-81-138-73.compute-1.amazonaws.com\",\"port\":\"22122\",\"dc\":\"us-east-1\",\"ip\":\"54.81.138.73\",\"zone\":\"us-east-1d\"},\""+
			"\"{\"token\":\"587531700\",\"hostname\":\"ec2-54-82-176-215.compute-1.amazonaws.com\",\"port\":\"22122\",\"dc\":\"us-east-1\",\"ip\":\"54.82.176.215\",\"zone\":\"us-east-1c\"},\"" +
			"\"{\"token\":\"3101134286\",\"hostname\":\"ec2-54-82-83-115.compute-1.amazonaws.com\",\"port\":\"22123\",\"dc\":\"us-east-1\",\"ip\":\"54.82.83.115\",\"zone\":\"us-east-1e\"},\"" +
			"\"{\"token\":\"237822755\",\"hostname\":\"ec2-54-211-220-55.compute-1.amazonaws.com\",\"port\":\"22122\",\"dc\":\"us-east-1\",\"ip\":\"54.211.220.55\",\"zone\":\"us-east-1e\"},\"" +
			"\"{\"token\":\"1669478519\",\"hostname\":\"ec2-54-80-65-203.compute-1.amazonaws.com\",\"port\":\"22122\",\"dc\":\"us-east-1\",\"ip\":\"54.80.65.203\",\"zone\":\"us-east-1e\"}]\"";

	private TokenMapSupplier testTokenMapSupplier = new AbstractTokenMapSupplier(22122) {

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

		// Create a dummy host list that one would get from teh host supplier.
		Map<String, Host> hosts = Collections.synchronizedMap(new HashMap());

		hosts.put("ec2-54-237-143-4.compute-1.amazonaws.com", new Host("ec2-54-237-143-4.compute-1.amazonaws.com", 22122,"us-east-1d",  Status.Up));
		hosts.put("ec2-50-17-65-2.compute-1.amazonaws.com", new Host("ec2-50-17-65-2.compute-1.amazonaws.com", 22122,"us-east-1d" , Status.Up));
		hosts.put("ec2-54-83-87-174.compute-1.amazonaws.com", new Host("ec2-54-83-87-174.compute-1.amazonaws.com", 22122, "us-east-1c", Status.Up));
		hosts.put("ec2-54-81-138-73.compute-1.amazonaws.com", new Host("ec2-54-81-138-73.compute-1.amazonaws.com", 22122,"us-east-1c", Status.Up));
		hosts.put("ec2-54-82-176-215.compute-1.amazonaws.com", new Host("ec2-54-82-176-215.compute-1.amazonaws.com", 22122,"us-east-1c", Status.Up));
		hosts.put("ec2-54-82-83-115.compute-1.amazonaws.com", new Host("ec2-54-82-83-115.compute-1.amazonaws.com", 22122, "us-east-1e", Status.Up));
		hosts.put("ec2-54-211-220-55.compute-1.amazonaws.com", new Host("ec2-54-211-220-55.compute-1.amazonaws.com", 22122,"us-east-1e", Status.Up));
		hosts.put("ec2-54-80-65-203.compute-1.amazonaws.com", new Host("ec2-54-80-65-203.compute-1.amazonaws.com", 22122,"us-east-1e", Status.Up));

		// Get the list of token to host map that one would get from the TokenMapSupplier.
		List<HostToken> hTokens = testTokenMapSupplier.getTokens(new HashSet<>(hosts.values()));

		Collections.sort(hTokens, new Comparator<HostToken>() {
			@Override
			public int compare(HostToken o1, HostToken o2) {
				return o1.getToken().compareTo(o2.getToken());
			}
		});


		// Assert that the host object from host supplier and from the token map supplier match.
        Assert.assertTrue(validateHostToken(hTokens.get(0), 188627880L, hosts.get(hTokens.get(0).getHost().getHostName())));
        Assert.assertTrue(validateHostToken(hTokens.get(1), 237822755L, hosts.get(hTokens.get(1).getHost().getHostName())));
        Assert.assertTrue(validateHostToken(hTokens.get(2), 587531700L, hosts.get(hTokens.get(2).getHost().getHostName())));
        Assert.assertTrue(validateHostToken(hTokens.get(3), 1669478519L, hosts.get(hTokens.get(3).getHost().getHostName())));
        Assert.assertTrue(validateHostToken(hTokens.get(4), 2019187467L, hosts.get(hTokens.get(4).getHost().getHostName())));
		// Assert that the host object from host supplier and from the token map supplier DO NOT match.
        Assert.assertFalse(validateHostToken(hTokens.get(5), 3051939411L, hosts.get(hTokens.get(5).getHost().getHostName())));
        Assert.assertFalse(validateHostToken(hTokens.get(6), 3101134286L, hosts.get(hTokens.get(6).getHost().getHostName())));
        Assert.assertFalse(validateHostToken(hTokens.get(7), 3450843231L, hosts.get(hTokens.get(7).getHost().getHostName())));
    }

    private boolean validateHostToken(HostToken hostToken, Long token, Host hostFromHostSupplier) {
        return Objects.equals(hostToken.getToken(), token) &&
                hostToken.getHost().equals(hostFromHostSupplier);
    }
}
