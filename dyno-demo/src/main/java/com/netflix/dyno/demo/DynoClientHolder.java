package com.netflix.dyno.demo;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nullable;

import net.spy.memcached.MemcachedClient;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.memcache.DynoMCacheClient;

public class DynoClientHolder {

	private static final DynoClientHolder Instance = new DynoClientHolder();
	
	public static DynoClientHolder getInstance() {
		return Instance;
	}
	
	private final AtomicReference<DynoMCacheClient> ref = new AtomicReference<DynoMCacheClient>(null);
//	private final AtomicReference<MemcachedClient> ref = new AtomicReference<MemcachedClient>(null);

	private DynoClientHolder() {
		try {
			ref.set(init2());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public DynoMCacheClient get() {
		return ref.get();
	}
	
//	public MemcachedClient get() {
//		return ref.get();
//	}

	private DynoMCacheClient init2() throws Exception {
		
		DynoMCacheClient client = 
				DynoMCacheClient.Builder.withName("Demo")
				.withDynomiteClusterName("dynomite_memcached_puneet")
				.build();
				
		try {
			Thread.sleep(150);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return client;
	}
	
	
	public MemcachedClient init() {
		
		final DynamicStringProperty HostList = DynamicPropertyFactory.getInstance().getStringProperty("dyno.demo.hosts", null);
		final DynamicIntProperty HostPort = DynamicPropertyFactory.getInstance().getIntProperty("dyno.demo.port", 8102);

		int port = HostPort.get();
		final List<Host> hosts = new ArrayList<Host>();

		String hostList = HostList.get();
		if (hostList == null || hostList.isEmpty()) {
			hosts.add(new Host("ec2-54-237-33-198.compute-1.amazonaws.com",  port).setDC("us-east-1c").setStatus(Status.Up));
			hosts.add(new Host("ec2-23-23-28-219.compute-1.amazonaws.com", port).setDC("us-east-1c").setStatus(Status.Up));
			hosts.add(new Host("ec2-54-237-223-228.compute-1.amazonaws.com", port).setDC("us-east-1c").setStatus(Status.Up));
		} else {
			String[] parts = hostList.split(",");
			for (String part : parts) {
				hosts.add(new Host(part,  port).setDC("us-east-1c").setStatus(Status.Up));
			}
		}

//		hosts.add(new Host("ec2-54-82-176-215.compute-1.amazonaws.com",  8102).setDC("us-east-1c").setStatus(Status.Up));
//		hosts.add(new Host("ec2-54-83-87-174.compute-1.amazonaws.com", 8102).setDC("us-east-1c").setStatus(Status.Up));
//		hosts.add(new Host("ec2-54-81-138-73.compute-1.amazonaws.com", 8102).setDC("us-east-1c").setStatus(Status.Up));
		
		Collection<InetSocketAddress> addrs = Collections2.transform(hosts, new Function<Host, InetSocketAddress>() {

			@Override
			@Nullable
			public InetSocketAddress apply(@Nullable Host input) {
				return input.getSocketAddress();
			}
		});
		
		System.out.println("CREATING MemcachedClient for nodes: " + addrs);
		try {
			return new MemcachedClient(new ArrayList<InetSocketAddress>(addrs));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

	}
	
	public void removeOneHost() throws Exception {
		
//		List<Host> upHosts = new ArrayList<Host>();
//		upHosts.add(new Host("ec2-54-197-69-207.compute-1.amazonaws.com",  11211).setDC("us-east-1c").setStatus(Status.Up));
//		upHosts.add(new Host("ec2-54-197-132-216.compute-1.amazonaws.com", 11211).setDC("us-east-1d").setStatus(Status.Up));
//		
//		List<Host> downHosts = new ArrayList<Host>();
//		downHosts.add(new Host("ec2-54-196-135-128.compute-1.amazonaws.com", 11211).setDC("us-east-1e").setStatus(Status.Up));
//
//		Future<Boolean> f = cpRef.get().updateHosts(upHosts, downHosts);
//		f.get();
	}
}
