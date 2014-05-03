package com.netflix.dyno.demo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.MemcachedClient;

import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.OperationMonitor;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.impl.LastOperationMonitor;
import com.netflix.dyno.memcache.DynoMCacheClient;
import com.netflix.dyno.memcache.MemcachedConnectionFactory;
import com.netflix.dyno.memcache.RollingMemcachedConnectionPoolImpl;

public class DynoClientHolder {

	private static final DynoClientHolder Instance = new DynoClientHolder();
	
	public static DynoClientHolder getInstance() {
		return Instance;
	}
	
	private final AtomicReference<DynoMCacheClient> ref = new AtomicReference<DynoMCacheClient>(null);
	
	private DynoClientHolder() {
		try {
			ref.set(init());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public DynoMCacheClient get() {
		return ref.get();
	}
	
	private DynoMCacheClient init() throws Exception {
		
		String appName = "dynomite";
		
		List<Host> hosts = new ArrayList<Host>();
		hosts.add(new Host("ec2-54-237-47-72.compute-1.amazonaws.com",  11211).setDC("us-east-1c").setStatus(Status.Up));
//		hosts.add(new Host("ec2-54-198-49-149.compute-1.amazonaws.com", 11211).setDC("us-east-1c").setStatus(Status.Up));
//		hosts.add(new Host("ec2-54-205-213-52.compute-1.amazonaws.com", 11211).setDC("us-east-1c").setStatus(Status.Up));

		ConnectionPoolConfiguration cpConfig = new ConnectionPoolConfigurationImpl(appName).setLocalDcAffinity(false);
		CountingConnectionPoolMonitor cpMonitor = new CountingConnectionPoolMonitor();
		MemcachedConnectionFactory connFactory = new MemcachedConnectionFactory(cpConfig, cpMonitor);
		OperationMonitor opMonitor = new LastOperationMonitor();

		RollingMemcachedConnectionPoolImpl<MemcachedClient> pool = 
				new RollingMemcachedConnectionPoolImpl<MemcachedClient>("pappyDemo", connFactory, cpConfig, cpMonitor, opMonitor);
		
		pool.updateHosts(hosts, Collections.<Host> emptyList());
		
		try {
			Thread.sleep(150);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		final DynoMCacheClient client = new DynoMCacheClient("Puneet", pool);
		
		return client;
	}
}
