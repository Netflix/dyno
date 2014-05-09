package com.netflix.dyno.memcache;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
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

/**
 * This class vends a custom {@link MemcachedClient} due to the following reasons
 * 
 * 1. We need to translate a list of {@link Host}s to a list of {@link SocketAddress}
 * 2. We need to implement a custom {@link SpyMemcachedConnectionFactory} for the client. 
 *    The custom {@link SpyMemcachedConnectionFactory} encapsulates our local zone aware round robin load balancing algo. 
 *    
 * @see {@link SpyMemcachedConnectionFactory} for more details
 * 
 * @author poberai
 *
 */
public class SpyMemcachedClientFactory {

	private static final Logger Logger = LoggerFactory.getLogger(SpyMemcachedClientFactory.class);
	
	private final ConnectionPoolConfiguration cpConfig; 
	private final ConnectionPoolMonitor cpMonitor; 
	
	/**
	 * Constructor
	 * @param config
	 * @param monitor
	 */
	public SpyMemcachedClientFactory(ConnectionPoolConfiguration config, ConnectionPoolMonitor monitor) {
		this.cpConfig = config;
		this.cpMonitor = monitor;
	}
	
	/**
	 * Method to vend a MemcachedClient
	 * @param hosts
	 * @return MemcachedClient
	 * @throws IOException
	 */
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
