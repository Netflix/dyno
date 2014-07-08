package com.netflix.dyno.demo.redis;

import java.util.concurrent.atomic.AtomicReference;

import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration.LoadBalancingStrategy;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.RetryNTimes;
import com.netflix.dyno.connectionpool.impl.health.SimpleErrorMonitorImpl;
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
	
	public static volatile DynoJedisClient dClientInstance = null;

	public DynoClient dynoClientWrapper = new DynoClient () {
		
		@Override
		public void init() {

			if (client.get() != null) {
				return;
			}

			System.out.println("Initing dyno redis client");
			
			DynamicStringProperty ClusterName = DynamicPropertyFactory.getInstance().getStringProperty("dyno.driver.cluster", "dynomite_redis_puneet");
			String cluster = ClusterName.get();
			System.out.println("Cluster: " + cluster);
			
			DynoJedisClient jClient = new DynoJedisClient.Builder()
					.withApplicationName("Demo")
					.withDynomiteClusterName(cluster)
					.build();
			
			dClientInstance = jClient;
			
			client.set(jClient);	
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