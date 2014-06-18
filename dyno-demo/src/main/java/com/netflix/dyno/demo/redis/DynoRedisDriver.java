package com.netflix.dyno.demo.redis;

import java.util.concurrent.atomic.AtomicReference;

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
			
			client.set(DynoJedisClient.Builder.withName("Demo")
						.withDynomiteClusterName("dynomite_redis_puneet")
						.withCPConfig(new ConnectionPoolConfigurationImpl("dynomite_redis_puneet")
									//.setPort(22122)
									.setPort(8102)
									.setMaxTimeoutWhenExhausted(1000)
									.setMaxConnsPerHost(60)
									//.setRetryPolicyFactory(new RetryNTimes.RetryFactory(1, true))
									//.setMaxConnsPerHost(3)
									//.withHostSupplier(new EurekaHostsSupplier("dynomite_redis_puneet", 22122))
									.withHostSupplier(new EurekaHostsSupplier("dynomite_redis_puneet", 8102))
									.setLoadBalancingStrategy(LoadBalancingStrategy.TokenAware))
						.build());
		}

		@Override
		public String get(String key) throws Exception {
			return client.get().get(key).getResult();
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