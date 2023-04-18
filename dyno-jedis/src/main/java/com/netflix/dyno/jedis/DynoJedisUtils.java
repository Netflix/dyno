package com.netflix.dyno.jedis;

import com.netflix.discovery.EurekaClient;
import com.netflix.dyno.connectionpool.ConnectionFactory;
import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.connectionpool.impl.lb.HttpEndpointBasedTokenMapSupplier;
import com.netflix.dyno.contrib.DynoOPMonitor;
import com.netflix.dyno.contrib.EurekaHostsSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import javax.inject.Singleton;
import javax.net.ssl.SSLSocketFactory;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

@Singleton
public class DynoJedisUtils {


    private static final Logger logger = LoggerFactory.getLogger(DynoJedisClient.class);

    public static void updateConnectionPoolConfig(ConnectionPoolConfigurationImpl cpConfig,
                                                  HostSupplier hostSupplier, TokenMapSupplier tokenMapSupplier,
                                                  EurekaClient discoveryClient, String clusterName) {
        if (hostSupplier == null) {
            if (discoveryClient == null) {
                throw new DynoConnectException("HostSupplier not provided. Cannot initialize EurekaHostsSupplier "
                        + "which requires a DiscoveryClient");
            } else {
                hostSupplier = new EurekaHostsSupplier(clusterName, discoveryClient);
            }
        }
        cpConfig.withHostSupplier(hostSupplier);
        if (tokenMapSupplier != null)
            cpConfig.withTokenSupplier(tokenMapSupplier);
        setLoadBalancingStrategy(cpConfig);
        setHashtagConnectionPool(hostSupplier, cpConfig);
    }

    public static ConnectionPool<Jedis> createConnectionPool(String appName, DynoOPMonitor opMonitor,
                                                             ConnectionPoolMonitor cpMonitor, ConnectionPoolConfiguration cpConfig,
                                                             SSLSocketFactory sslSocketFactory) {
        JedisConnectionFactory connFactory = new JedisConnectionFactory(opMonitor, sslSocketFactory);

        return startConnectionPool(appName, connFactory, cpConfig, cpMonitor);
    }

    private static ConnectionPool<Jedis> startConnectionPool(String appName, ConnectionFactory connFactory,
                                                             ConnectionPoolConfiguration cpConfig, ConnectionPoolMonitor cpMonitor) {

        final ConnectionPool<Jedis> pool = new ConnectionPoolImpl<>(connFactory, cpConfig, cpMonitor);

        try {
            logger.info("Starting connection pool for app " + appName);

            pool.start().get();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> pool.shutdown()));
        } catch (NoAvailableHostsException e) {
            if (cpConfig.getFailOnStartupIfNoHosts()) {
                throw new RuntimeException(e);
            }

            logger.warn("UNABLE TO START CONNECTION POOL -- IDLING");

            pool.idle();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return pool;
    }

    private static void setLoadBalancingStrategy(ConnectionPoolConfigurationImpl config) {
        if (ConnectionPoolConfiguration.LoadBalancingStrategy.TokenAware == config.getLoadBalancingStrategy()) {
            if (config.getTokenSupplier() == null) {
                logger.warn(
                        "TOKEN AWARE selected and no token supplier found, using default HttpEndpointBasedTokenMapSupplier()");
                config.withTokenSupplier(new HttpEndpointBasedTokenMapSupplier());
            }

            if (config.getLocalRack() == null && config.localZoneAffinity()) {
                String warningMessage = "DynoJedisClient for app=[" + config.getName()
                        + "] is configured for local rack affinity "
                        + "but cannot determine the local rack! DISABLING rack affinity for this instance. "
                        + "To make the client aware of the local rack either use "
                        + "ConnectionPoolConfigurationImpl.setLocalRack() when constructing the client "
                        + "instance or ensure EC2_AVAILABILTY_ZONE is set as an environment variable, e.g. "
                        + "run with -DLOCAL_RACK=us-east-1c";
                config.setLocalZoneAffinity(false);
                logger.warn(warningMessage);
            }
        }
    }

    /**
     * Set the hash to the connection pool if is provided by Dynomite
     *
     * @param hostSupplier
     * @param config
     */
    private static void setHashtagConnectionPool(HostSupplier hostSupplier, ConnectionPoolConfigurationImpl config) {
        // Find the hosts from host supplier
        List<Host> hosts = (List<Host>) hostSupplier.getHosts();
        Collections.sort(hosts);

        logger.info("[DynoConnectDebug] Got number of hosts = " + hosts.size());
        // Take the token map supplier (aka the token topology from
        // Dynomite)
        TokenMapSupplier tokenMapSupplier = config.getTokenSupplier();

        // Create a list of host/Tokens
        List<HostToken> hostTokens;
        if (tokenMapSupplier != null) {
            Set<Host> hostSet = new HashSet<Host>(hosts);
            hostTokens = tokenMapSupplier.getTokens(hostSet);
            /* Dyno cannot reach the TokenMapSupplier endpoint,
             * therefore no nodes can be retrieved.
             */
            if (hostTokens.isEmpty()) {
                throw new DynoConnectException("No hosts in the TokenMapSupplier");
            }
        } else {
            throw new DynoConnectException("TokenMapSupplier not provided");
        }

        String hashtag = hostTokens.get(0).getHost().getHashtag();
        Stream<String> htStream = hostTokens.stream().map(hostToken -> hostToken.getHost().getHashtag());

        if (hashtag == null) {
            htStream.filter(ht -> ht != null).findAny().ifPresent(ignore -> {
                logger.error("Hashtag mismatch across hosts");
                throw new RuntimeException("Hashtags are different across hosts");
            });
        } else {
            /**
             * Checking hashtag consistency from all Dynomite hosts. If
             * hashtags are not consistent, we need to throw an exception.
             */
            htStream.filter(ht -> !hashtag.equals(ht)).findAny().ifPresent(ignore -> {
                logger.error("Hashtag mismatch across hosts");
                throw new RuntimeException("Hashtags are different across hosts");
            });
        }

        if (hashtag != null) {
            config.withHashtag(hashtag);
        }
    }
}
