package com.netflix.dyno.jedis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;

import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl;
import com.netflix.dyno.contrib.DynoCPMonitor;
import com.netflix.dyno.contrib.DynoOPMonitor;
import com.netflix.dyno.contrib.EurekaHostsSupplier;

public class DynoJedisClient {
	
	private static final Logger Logger = LoggerFactory.getLogger(DynoJedisClient.class);
	
	private final ConnectionPool<Jedis> connPool;
	
	public DynoJedisClient(String name, ConnectionPool<Jedis> pool) {
		this.connPool = pool;
	}

	private enum OpName { 
		Set, Delete, Get, GetAndTouch, GetBulk, GetAsync;
	}
	
	
	public String get(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new Operation<Jedis, String>() {

			@Override
			public String getName() {
				return OpName.Get.name();
			}

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.get(key);
			}

			@Override
			public String getKey() {
				return key;
			}
			
		}).getResult();
	}
	
	public Void set(final String key, final String value) throws DynoException {
		
		return connPool.executeWithFailover(new Operation<Jedis, Void>() {

			@Override
			public String getName() {
				return OpName.Set.name();
			}

			@Override
			public Void execute(Jedis client, ConnectionContext state) throws DynoException {
				client.set(key, value);
				return null;
			}
			
			@Override
			public String getKey() {
				return key;
			}
		}).getResult();
	}

	public static class Builder {
		
		private String appName;
		private String clusterName;
		private ConnectionPoolConfigurationImpl cpConfig;
		
		public Builder(String name) {
			appName = name;
			cpConfig = new ConnectionPoolConfigurationImpl(appName);
		}
		
		public Builder withPort(int port) {
			cpConfig.setPort(port);
			return this;
		}


		public Builder withDynomiteClusterName(String cluster) {
			clusterName = cluster;
			return this;
		}

		public DynoJedisClient build() {

			assert(appName != null);
			assert(clusterName != null);
			
			cpConfig.setPort(22122);
			cpConfig.withHostSupplier(new EurekaHostsSupplier(clusterName, cpConfig));

//			CountingConnectionPoolMonitor cpMonitor = new CountingConnectionPoolMonitor();
//			OperationMonitor opMonitor = new LastOperationMonitor();
			DynoCPMonitor cpMonitor = new DynoCPMonitor(appName);
			DynoOPMonitor opMonitor = new DynoOPMonitor(appName);
			
			JedisConnectionFactory connFactory = new JedisConnectionFactory(opMonitor);

			ConnectionPoolImpl<Jedis> pool = new ConnectionPoolImpl<Jedis>(connFactory, cpConfig, cpMonitor);
			
			try {
				pool.start().get();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
			final DynoJedisClient client = new DynoJedisClient(appName, pool);
			
			return client;
		}
		
		public static Builder withName(String name) {
			return new Builder(name);
		}
	}

}
