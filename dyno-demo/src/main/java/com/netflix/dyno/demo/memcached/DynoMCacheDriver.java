package com.netflix.dyno.demo.memcached;

import java.util.concurrent.atomic.AtomicReference;

import com.netflix.dyno.demo.DynoDriver;
import com.netflix.dyno.demo.DynoDriver.DynoClient;
import com.netflix.dyno.memcache.DynoMCacheClient;

public class DynoMCacheDriver extends DynoDriver {

	private static final DynoDriver Instance = new DynoMCacheDriver();
	
	private AtomicReference<DynoMCacheClient> client = new AtomicReference<DynoMCacheClient>(null);

	public static DynoDriver getInstance() {
		return Instance;
	}
	
	private DynoMCacheDriver() {
		super();
	}
	
	public DynoClient dynoClientWrapper = new DynoClient () {

		@Override
		public void init() {
			 client.set(DynoMCacheClient.Builder.withName("Demo")
						.withDynomiteClusterName("dynomite_memcached_puneet")
						.build());
						

		}

		@Override
		public String get(String key) throws Exception {
			return client.get().<String>get(key).getResult();
		}

		@Override
		public void set(String key, String value) {
			client.get().<String>set(key, value);
		}
	};
	
	public DynoClient getDynoClient() {
		return dynoClientWrapper;
	}
}
