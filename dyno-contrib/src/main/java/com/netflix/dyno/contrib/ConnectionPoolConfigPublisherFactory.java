package com.netflix.dyno.contrib;

import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolConfigurationPublisher;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;

import java.util.Locale;

/**
 * @author jcacciatore
 */
public class ConnectionPoolConfigPublisherFactory {

    private static final org.slf4j.Logger Logger = org.slf4j.LoggerFactory.getLogger(ConnectionPoolConfigPublisherFactory.class);

    public ConnectionPoolConfigurationPublisher createPublisher(String applicationName, String clusterName, ConnectionPoolConfiguration config) {
        if (config.getConfigurationPublisherConfig() != null) {
            try {
                JSONObject json = new JSONObject(config.getConfigurationPublisherConfig());
                String vip = json.getString("vip");
                if (vip != null) {
                    String type = json.getString("type");
                    if (type != null) {
                        ConnectionPoolConfigurationPublisher.PublisherType publisherType =
                                ConnectionPoolConfigurationPublisher.PublisherType.valueOf(type.toUpperCase(Locale.ENGLISH));

                        if (ConnectionPoolConfigurationPublisher.PublisherType.ELASTIC == publisherType) {
                            return new ElasticConnectionPoolConfigurationPublisher(applicationName, clusterName, vip, config);
                        }
                    }
                }
            } catch (JSONException e) {
                Logger.warn("Invalid json specified for config publisher: " + e.getMessage());
            }
        }

        return null;
    }

}
