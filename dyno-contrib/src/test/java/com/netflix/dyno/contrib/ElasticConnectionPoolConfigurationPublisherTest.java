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

        Map<String, String> versions =  publisher.getLibraryVersion(this.getClass(), "dyno-core");
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
        assertTrue(json.has("Port"));

    }
}
