package com.netflix.dyno.demo.redis;

import java.util.concurrent.atomic.AtomicReference;

import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration.LoadBalancingStrategy;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.contrib.EurekaHostsSupplier;
import com.netflix.dyno.demo.DynoDriver;
import com.netflix.dyno.jedis.DynoJedisClient;

public class DynoRedisDriver extends DynoDriver {


	private static final DynoDriver Instance = new DynoRedisDriver();

	private final AtomicReference<DynoJedisClient> client = new AtomicReference<DynoJedisClient>(null);

	public static DynoDriver getInstance() {
		return Instance;
	}

	private DynoRedisDriver() {
		super();
	}

	public DynoClient dynoClientWrapper = new DynoClient () {

		
		
		@Override
		public void init() {

			if (client.get() != null) {
				return;
			}

			System.out.println("Initing dyno redis client");
			
			DynamicIntProperty Port = DynamicPropertyFactory.getInstance().getIntProperty("dyno.driver.port", 22122);
			DynamicIntProperty MaxConns = DynamicPropertyFactory.getInstance().getIntProperty("dyno.driver.conns", 60);
			DynamicStringProperty ClusterName = DynamicPropertyFactory.getInstance().getStringProperty("dyno.driver.cluster", "dynomite_redis_puneet");
			
			String cluster = ClusterName.get();
			int port = Port.get();
			int conns = MaxConns.get();
			
			System.out.println("Cluster: " + cluster + ", port: " + port + ", conns: " + conns);

			client.set(DynoJedisClient.Builder.withName("Demo")
						.withDynomiteClusterName(cluster)
						.withCPConfig(new ConnectionPoolConfigurationImpl(cluster)
									.setPort(port)
									.setMaxTimeoutWhenExhausted(1000)
									.setMaxConnsPerHost(conns)
									.withHostSupplier(new EurekaHostsSupplier(cluster, port))
									.setLoadBalancingStrategy(LoadBalancingStrategy.TokenAware))
						.build());
		}

		@Override
		public String get(String key) throws Exception {
			return client.get().get(key);
		}

		@Override
		public void set(String key, String value) {
			client.get().set(key, value);
		}
	};

	public DynoClient getDynoClient() {
		return dynoClientWrapper;
	}
}