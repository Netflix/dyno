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

	final String json = "[{\"token\":\"3051939411\",\"hostname\":\"ec2-54-237-143-4.compute-1.amazonaws.com\",\"port\":\"8102\",\"dc\":\"us-east-1\",\"ip\":\"54.237.143.4\",\"zone\":\"us-east-1d\"}\"," +
			"\"{\"token\":\"188627880\",\"hostname\":\"ec2-50-17-65-2.compute-1.amazonaws.com\",\"port\":\"8102\",\"dc\":\"us-east-1\",\"ip\":\"50.17.65.2\",\"zone\":\"us-east-1d\"},\"" +
			"\"{\"token\":\"2019187467\",\"hostname\":\"ec2-54-83-87-174.compute-1.amazonaws.com\",\"port\":\"8102\",\"dc\":\"us-east-1\",\"ip\":\"54.83.87.174\",\"zone\":\"us-east-1c\" },\"" +
			"\"{\"token\":\"3450843231\",\"hostname\":\"ec2-54-81-138-73.compute-1.amazonaws.com\",\"port\":\"8102\",\"dc\":\"us-east-1\",\"ip\":\"54.81.138.73\",\"zone\":\"us-east-1c\"},\""+
			"\"{\"token\":\"587531700\",\"hostname\":\"ec2-54-82-176-215.compute-1.amazonaws.com\",\"port\":\"8102\",\"dc\":\"us-east-1\",\"ip\":\"54.82.176.215\",\"zone\":\"us-east-1c\"},\"" +
			"\"{\"token\":\"3101134286\",\"hostname\":\"ec2-54-82-83-115.compute-1.amazonaws.com\",\"port\":\"8102\",\"dc\":\"us-east-1\",\"ip\":\"54.82.83.115\",\"zone\":\"us-east-1e\"},\"" +
			"\"{\"token\":\"237822755\",\"hostname\":\"ec2-54-211-220-55.compute-1.amazonaws.com\",\"port\":\"8102\",\"dc\":\"us-east-1\",\"ip\":\"54.211.220.55\",\"zone\":\"us-east-1e\"},\"" +
			"\"{\"token\":\"1669478519\",\"hostname\":\"ec2-54-80-65-203.compute-1.amazonaws.com\",\"port\":\"8102\",\"dc\":\"us-east-1\",\"ip\":\"54.80.65.203\",\"zone\":\"us-east-1e\"}]\"";

	private TokenMapSupplier testTokenMapSupplier = new AbstractTokenMapSupplier(8102) {

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

		List<Host> hostList = new ArrayList<>();

		hostList.add(new Host("ec2-54-237-143-4.compute-1.amazonaws.com", 8102,"rack",  Status.Up));
		hostList.add(new Host("ec2-50-17-65-2.compute-1.amazonaws.com", 8102,"rack" , Status.Up));
		hostList.add(new Host("ec2-54-83-87-174.compute-1.amazonaws.com", 8102, "rack", Status.Up));
		hostList.add(new Host("ec2-54-81-138-73.compute-1.amazonaws.com", 8102,"rack", Status.Up));
		hostList.add(new Host("ec2-54-82-176-215.compute-1.amazonaws.com", 8102,"rack", Status.Up));
		hostList.add(new Host("ec2-54-82-83-115.compute-1.amazonaws.com", 8102, "rack", Status.Up));
		hostList.add(new Host("ec2-54-211-220-55.compute-1.amazonaws.com", 8102,"rack", Status.Up));
		hostList.add(new Host("ec2-54-80-65-203.compute-1.amazonaws.com", 8102,"rack", Status.Up));

		List<HostToken> hTokens = testTokenMapSupplier.getTokens(new HashSet<>(hostList));
		Collections.sort(hTokens, new Comparator<HostToken>() {
			@Override
			public int compare(HostToken o1, HostToken o2) {
				return o1.getToken().compareTo(o2.getToken());
			}
		});

        Assert.assertTrue(validateHostToken(hTokens.get(0), 188627880L, "ec2-50-17-65-2.compute-1.amazonaws.com", "50.17.65.2", 8102, "us-east-1d", "us-east-1"));
        Assert.assertTrue(validateHostToken(hTokens.get(1), 237822755L, "ec2-54-211-220-55.compute-1.amazonaws.com", "54.211.220.55", 8102, "us-east-1e", "us-east-1"));
        Assert.assertTrue(validateHostToken(hTokens.get(2), 587531700L, "ec2-54-82-176-215.compute-1.amazonaws.com", "54.82.176.215", 8102, "us-east-1c", "us-east-1"));
        Assert.assertTrue(validateHostToken(hTokens.get(3), 1669478519L, "ec2-54-80-65-203.compute-1.amazonaws.com", "54.80.65.203", 8102, "us-east-1e", "us-east-1"));
        Assert.assertTrue(validateHostToken(hTokens.get(4), 2019187467L, "ec2-54-83-87-174.compute-1.amazonaws.com", "54.83.87.174", 8102, "us-east-1c", "us-east-1"));
        Assert.assertTrue(validateHostToken(hTokens.get(5), 3051939411L, "ec2-54-237-143-4.compute-1.amazonaws.com", "54.237.143.4", 8102, "us-east-1d", "us-east-1"));
        Assert.assertTrue(validateHostToken(hTokens.get(6), 3101134286L, "ec2-54-82-83-115.compute-1.amazonaws.com", "54.82.83.115", 8102, "us-east-1e", "us-east-1"));
        Assert.assertTrue(validateHostToken(hTokens.get(7), 3450843231L, "ec2-54-81-138-73.compute-1.amazonaws.com", "54.81.138.73", 8102, "us-east-1c", "us-east-1"));
    }

    private boolean validateHostToken(HostToken hostToken, Long token, String hostname, String ipAddress, int port, String rack, String datacenter) {
        return Objects.equals(hostToken.getToken(), token) &&
                hostToken.getHost().getHostName().equals(hostname) &&
                hostToken.getHost().getHostAddress().equals(ipAddress) &&
                hostToken.getHost().getPort() == port &&
                hostToken.getHost().getRack().equals(rack) &&
                hostToken.getHost().getDatacenter().equals(datacenter);
    }
}
