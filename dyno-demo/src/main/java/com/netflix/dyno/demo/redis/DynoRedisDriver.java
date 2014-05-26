package com.netflix.dyno.demo.redis;

import java.util.concurrent.atomic.AtomicReference;

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
			client.set(DynoJedisClient.Builder.withName("Demo")
						.withDynomiteClusterName("dynomite_redis_puneet")
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