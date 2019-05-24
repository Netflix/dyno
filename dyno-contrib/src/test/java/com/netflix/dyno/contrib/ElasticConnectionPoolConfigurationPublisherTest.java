/**
 * Copyright 2016 Netflix, Inc.
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

import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;

public class ElasticConnectionPoolConfigurationPublisherTest {

    private ConnectionPoolConfiguration config;

    @Before
    public void before() {
        config = new ArchaiusConnectionPoolConfiguration("UnitTest")
                .setLoadBalancingStrategy(ConnectionPoolConfiguration.LoadBalancingStrategy.RoundRobin)
                .setMaxConnsPerHost(100);
    }

    @Test
    public void testLibraryVersion() {
        ElasticConnectionPoolConfigurationPublisher publisher =
                new ElasticConnectionPoolConfigurationPublisher("ClientApp", "dyno_cluster_1", "unit-test-vip", config);

        Map<String, String> versions = publisher.getLibraryVersion(this.getClass(), "dyno-core");
        assertTrue(versions.size() == 1 || versions.size() == 0); // dyno-contrib depends on dyno-core
    }

    @Test
    public void testFindVersionFindIndex() {
        ElasticConnectionPoolConfigurationPublisher publisher =
                new ElasticConnectionPoolConfigurationPublisher("ClientApp", "dyno_cluster_1", "unit-test-vip", config);

        assertTrue(publisher.findVersionStartIndex(null) < 0);
        assertTrue(publisher.findVersionStartIndex("foo") < 0);
        assertTrue(publisher.findVersionStartIndex("foo-bar.jar") < 0);
        assertTrue(publisher.findVersionStartIndex("foo-bar-2.3") == 8);
        assertTrue(publisher.findVersionStartIndex("foo-bar-2.3.1-SNAPSHOT") == 8);
        assertTrue(publisher.findVersionStartIndex("foo-bar-baz-2.3") == 12);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateJsonFromConfigNullConfig() throws JSONException {
        ElasticConnectionPoolConfigurationPublisher publisher =
                new ElasticConnectionPoolConfigurationPublisher("ClientApp", "dyno_cluster_1", "unit-test-vip", config);

        publisher.createJsonFromConfig(null);
    }

    @Test
    public void testCreateJsonFromConfig() throws JSONException {
        ElasticConnectionPoolConfigurationPublisher publisher =
                new ElasticConnectionPoolConfigurationPublisher("ClientApp", "dyno_cluster_1", "unit-test-vip", config);

        JSONObject json = publisher.createJsonFromConfig(config);

        assertTrue(json.has("ApplicationName"));
        assertTrue(json.has("DynomiteClusterName"));
        assertTrue(json.has("MaxConnsPerHost"));

    }
}
