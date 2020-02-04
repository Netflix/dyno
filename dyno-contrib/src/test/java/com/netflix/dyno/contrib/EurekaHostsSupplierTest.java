/**
 * Copyright 2020 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.contrib;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.netflix.appinfo.AmazonInfo;
import com.netflix.appinfo.AmazonInfo.MetaDataKey;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.shared.Application;
import com.netflix.dyno.connectionpool.Host;
import java.util.List;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;

public class EurekaHostsSupplierTest {
    private static final String APPLICATION_NAME = "DYNOTEST";

    private EurekaClient eurekaClient;
    private EurekaHostsSupplier hostsSupplier;

    @Before
    public void setUp() {
        eurekaClient = mock(EurekaClient.class);
        hostsSupplier = new EurekaHostsSupplier(APPLICATION_NAME, eurekaClient);
    }

    @Test
    public void testGetHosts() {
        String zone = "us-east-1c";
        String hostname = "1.2.3.4";
        Application app = new Application();
        AmazonInfo amazonInfo = AmazonInfo.Builder.newBuilder()
                .addMetadata(MetaDataKey.availabilityZone, zone).build();
        InstanceInfo instance = InstanceInfo.Builder.newBuilder()
                .setInstanceId(UUID.randomUUID().toString())
                .setDataCenterInfo(amazonInfo)
                .setHostName(hostname)
                .setAppName(APPLICATION_NAME)
                .build();
        app.addInstance(instance);
        when(eurekaClient.getApplication(APPLICATION_NAME)).thenReturn(app);
        List<Host> hosts = hostsSupplier.getHosts();
        assertFalse(hosts.isEmpty());
        assertEquals(hostname, hosts.get(0).getHostName());
        assertEquals(zone, hosts.get(0).getRack());
    }
}
