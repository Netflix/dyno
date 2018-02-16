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
package com.netflix.dyno.contrib;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.agent.model.NewCheck;
import com.ecwid.consul.v1.agent.model.NewService;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.contrib.consul.ConsulHostsSupplier;
import com.pszymczyk.consul.ConsulProcess;
import com.pszymczyk.consul.ConsulStarterBuilder;
import org.codehaus.jettison.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ConsulHostsSupplierTest {

    private ConsulProcess consulServer;
    private ConsulClient consulClient;
    private static String APPLICATION_NAME = "testApp";

    @Before
    public void beforeEach() {
        String config = "{" +
            "\"datacenter\": \"test-dc\"," +
            "\"log_level\": \"INFO\"," +
            "\"node_name\": \"foobar\"" +
            "}";
        consulServer = ConsulStarterBuilder.consulStarter().withCustomConfig(config).build().start();
        consulClient = new ConsulClient("127.0.0.1", consulServer.getHttpPort());
    }

    @After
    public void afterEach() throws Exception {
        consulServer.close();
    }

    @Test
    public void testAwsHosts() throws JSONException {
        List<String> tags = new ArrayList<>();
        tags.add("cloud=aws");
        tags.add("availability-zone=us-east-1b");
        tags.add("datacenter=us-east-1");

        NewService service = new NewService();
        service.setName(APPLICATION_NAME);
        service.setTags(tags);

        consulClient.agentServiceRegister(service);

        NewCheck check = new NewCheck();
        check.setName(APPLICATION_NAME);
        check.setScript("true");
        check.setTtl("1m");
        check.setInterval("30s");

        consulClient.agentCheckRegister(check);

        consulClient.agentCheckPass(APPLICATION_NAME);

        ConsulHostsSupplier hostSupplier = new ConsulHostsSupplier(APPLICATION_NAME, consulClient);

        assertEquals(hostSupplier.getApplicationName(), APPLICATION_NAME);

        List<Host> hosts = hostSupplier.getHosts();

        assertEquals(hosts.size(), 1);

        Host host = hosts.get(0);

        assertEquals(host.getRack(), "us-east-1b");
        assertEquals(host.getDatacenter(), "us-east-1");
        assertEquals(host.getHostName(), "127.0.0.1");
        assertEquals(host.getIpAddress(), "127.0.0.1");
        assertTrue(host.isUp());
    }

    @Test
    public void testOtherCloudProviderHosts() throws JSONException {
        List<String> tags = new ArrayList<>();
        tags.add("cloud=kubernetes");
        tags.add("rack=rack1");
        tags.add("datacenter=dc1");

        NewService service = new NewService();
        service.setName(APPLICATION_NAME);
        service.setTags(tags);

        consulClient.agentServiceRegister(service);

        NewCheck check = new NewCheck();
        check.setName(APPLICATION_NAME);
        check.setScript("true");
        check.setTtl("1m");
        check.setInterval("30s");

        consulClient.agentCheckRegister(check);

        consulClient.agentCheckPass(APPLICATION_NAME);

        ConsulHostsSupplier hostSupplier = new ConsulHostsSupplier(APPLICATION_NAME, consulClient);

        assertEquals(hostSupplier.getApplicationName(), APPLICATION_NAME);

        List<Host> hosts = hostSupplier.getHosts();

        assertEquals(hosts.size(), 1);

        Host host = hosts.get(0);

        assertEquals(host.getRack(), "rack1");
        assertEquals(host.getDatacenter(), "dc1");
        assertEquals(host.getHostName(), "127.0.0.1");
        assertEquals(host.getIpAddress(), "127.0.0.1");
        assertTrue(host.isUp());
    }
}
