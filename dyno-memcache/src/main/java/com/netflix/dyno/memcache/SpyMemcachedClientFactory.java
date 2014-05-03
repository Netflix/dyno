package com.netflix.dyno.memcache;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.Nullable;

import net.spy.memcached.MemcachedClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Host;

public class SpyMemcachedClientFactory {

	private static final Logger Logger = LoggerFactory.getLogger(SpyMemcachedClientFactory.class);
	
	private final ConnectionPoolConfiguration cpConfig; 
	private final ConnectionPoolMonitor cpMonitor; 
	
	public SpyMemcachedClientFactory(ConnectionPoolConfiguration config, ConnectionPoolMonitor monitor) {
		this.cpConfig = config;
		this.cpMonitor = monitor;
	}
	
	public MemcachedClient createMemcachedClient(List<Host> hosts) throws IOException {
		
		Collection<Host> hostsUp = Collections2.filter(hosts, new Predicate<Host>() {

			@Override
			public boolean apply(@Nullable Host input) {
				return input.isUp();
			}
		});
		
		Collection<InetSocketAddress> addrs = Collections2.transform(hostsUp, new Function<Host, InetSocketAddress>() {

			@Override
			@Nullable
			public InetSocketAddress apply(@Nullable Host input) {
				return input.getSocketAddress();
			}
		});
		
		Logger.info("CREATING MemcachedClient for nodes: " + addrs);
		return new MemcachedClient(new SpyMemcachedConnectionFactory(hosts, cpConfig, cpMonitor), new ArrayList<InetSocketAddress>(addrs));
	}
}
