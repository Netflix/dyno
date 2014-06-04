package com.netflix.dyno.memcache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import javax.annotation.Nullable;

import net.spy.memcached.CASValue;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.transcoders.Transcoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.netflix.dyno.connectionpool.AsyncOperation;
import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.contrib.DynoCPMonitor;
import com.netflix.dyno.contrib.DynoOPMonitor;

/**
 * Dyno client for Memcached that uses the {@link RollingMemcachedConnectionPoolImpl} for managing connections to {@link MemcachedClient}s
 * with local zone aware based RR load balancing and fallbacks to the remote zones
 * 
 * @author poberai
 *
 */
public class DynoMCacheClient {

	private static final Logger Logger = LoggerFactory.getLogger(DynoMCacheClient.class);

	private static final String SEPARATOR = ":";
	private final String cacheName;
	private final String cacheNamePrefix;

	private final int defaultTTL = 0;

	private final ConnectionPool<MemcachedClient> connPool;

	public DynoMCacheClient(String name, ConnectionPool<MemcachedClient> pool) {
		this.cacheName = name;
		this.cacheNamePrefix = name + SEPARATOR;
		this.connPool = pool;
	}

	private enum OpName { 
		Set, Delete, Get, GetAndTouch, GetBulk, GetAsync;
	}

	public <T> Future<OperationResult<Boolean>> set(final String key, final T value) throws DynoException {
		return set(key, value, defaultTTL);
	}

	public <T> Future<OperationResult<Boolean>> set(final String key, final T value, final int exp) throws DynoException {

		return connPool.executeAsync(new AsyncOperation<MemcachedClient, Boolean>() {

			@Override
			public Future<Boolean> executeAsync(MemcachedClient client) throws DynoException {
				return client.set(getCanonicalizedKey(key), exp, value);
			}

			@Override
			public String getName() {
				return OpName.Set.name();
			}

			@Override
			public String getKey() {
				return key;
			}
		});
	}

	public <T> Future<OperationResult<Boolean>> set(final String key, final T value, final Transcoder<T> tc) throws DynoException {
		return set(key, value, tc, defaultTTL);
	}

	public <T> Future<OperationResult<Boolean>> set(final String key, final T value, final Transcoder<T> tc, final int timeToLive) throws DynoException {

		return connPool.executeAsync(new AsyncOperation<MemcachedClient, Boolean>() {

			@Override
			public Future<Boolean> executeAsync(MemcachedClient client) throws DynoException {
				return client.set(getCanonicalizedKey(key), timeToLive, value, tc);
			}

			@Override
			public String getName() {
				return OpName.Set.name();
			}

			@Override
			public String getKey() {
				return key;
			}
		});

	}

	public Future<OperationResult<Boolean>> delete(final String key) throws DynoException {

		return connPool.executeAsync(new AsyncOperation<MemcachedClient, Boolean>() {

			@Override
			public Future<Boolean> executeAsync(MemcachedClient client) throws DynoException {
				return client.delete(getCanonicalizedKey(key));
			}

			@Override
			public String getName() {
				return OpName.Delete.name();
			}

			@Override
			public String getKey() {
				return key;
			}
		});
	}

	public <T> OperationResult<T> get(final String key) {

		return connPool.executeWithFailover(new Operation<MemcachedClient, T>() {

			@Override
			public T execute(MemcachedClient client, ConnectionContext state) throws DynoException {
				try { 
					return (T) client.get(key);
				} catch(Exception e) {
					Logger.warn("Throwing dyno connect ex after receiving ex from mc client " + e.getMessage());
					throw new DynoConnectException(e);
				}
			}

			@Override
			public String getName() {
				return OpName.Get.name();
			}

			@Override
			public String getKey() {
				return key;
			}
		});
	}

	<T> OperationResult<T> getAndTouch(final String key, final int timeToLive) throws DynoException {

		return connPool.executeWithFailover(new Operation<MemcachedClient, T>() {

			@Override
			public T execute(MemcachedClient client, ConnectionContext state) throws DynoException {
				return (T) client.getAndTouch(getCanonicalizedKey(key), timeToLive).getValue();
			}

			@Override
			public String getName() {
				return OpName.GetAndTouch.name();
			}

			@Override
			public String getKey() {
				return key;
			}
		});
	}

	public <T> OperationResult<T> getAndTouch(final String key, final int timeToLive, final Transcoder<T> tc) {

		return connPool.executeWithFailover(new Operation<MemcachedClient, T>() {

			@Override
			public T execute(MemcachedClient client, ConnectionContext state) throws DynoException {

				CASValue<T> casValue = client.getAndTouch(getCanonicalizedKey(key), timeToLive, tc);
				return casValue.getValue();
			}

			@Override
			public String getName() {
				return OpName.GetAndTouch.name();
			}

			@Override
			public String getKey() {
				return key;
			}
		});
	}

	public <T> OperationResult<Map<String, T>> getBulk(final String... keys) throws DynoException {

		return connPool.executeWithFailover(new Operation<MemcachedClient, Map<String, T>>() {

			@Override
			public Map<String, T> execute(MemcachedClient client, ConnectionContext state) throws DynoException {
				return (Map<String, T>) client.getBulk(getCanonicalizedKeys(keys));
			}

			@Override
			public String getName() {
				return OpName.GetBulk.name();
			}

			@Override
			public String getKey() {
				return null;
			}
		});
	}

	public <T> OperationResult<Map<String, T>> getBulk(final Transcoder<T> tc, final String... keys) throws DynoException {

		return connPool.executeWithFailover(new Operation<MemcachedClient, Map<String, T>>() {

			@Override
			public Map<String, T> execute(MemcachedClient client, ConnectionContext state) throws DynoException {
				return client.getBulk(getCanonicalizedKeys(keys), tc);
			}

			@Override
			public String getName() {
				return OpName.GetBulk.name();
			}

			@Override
			public String getKey() {
				return null;
			}
		});
	}

	public <T> OperationResult<Map<String, T>> getBulk(final Collection<String> keys) throws DynoException {

		return connPool.executeWithFailover(new Operation<MemcachedClient, Map<String, T>>() {

			@Override
			public Map<String, T> execute(MemcachedClient client, ConnectionContext state) throws DynoException {
				return (Map<String, T>) client.getBulk(getCanonicalizedKeys(keys));
			}

			@Override
			public String getName() {
				return OpName.GetBulk.name();
			}

			@Override
			public String getKey() {
				return null;
			}
		});
	}

	public <T> OperationResult<Map<String, T>> getBulk(final Collection<String> keys, final Transcoder<T> tc) throws DynoException {

		return connPool.executeWithFailover(new Operation<MemcachedClient, Map<String, T>>() {

			@Override
			public Map<String, T> execute(MemcachedClient client, ConnectionContext state) throws DynoException {
				return client.getBulk(keys, tc);
			}

			@Override
			public String getName() {
				return OpName.GetBulk.name();
			}

			@Override
			public String getKey() {
				return null;
			}
		});
	}


	public <T> Future<OperationResult<T>> getAsync(final String key) throws DynoException {

		return connPool.executeAsync(new AsyncOperation<MemcachedClient, T>() {

			@Override
			public Future<T> executeAsync(MemcachedClient client) throws DynoException {
				return (Future<T>) client.asyncGet(getCanonicalizedKey(key));
			}

			@Override
			public String getName() {
				return OpName.GetAsync.name();
			}

			@Override
			public String getKey() {
				return key;
			}
		});
	}

	public <T> Future<OperationResult<T>> getAsync(final String key, final Transcoder<T> tc) throws DynoException {

		return connPool.executeAsync(new AsyncOperation<MemcachedClient, T>() {

			@Override
			public Future<T> executeAsync(MemcachedClient client) throws DynoException {
				return (Future<T>) client.asyncGet(getCanonicalizedKey(key), tc);
			}

			@Override
			public String getName() {
				return OpName.GetAsync.name();
			}

			@Override
			public String getKey() {
				return key;
			}
		});
	}


	protected String getCanonicalizedKey(String key) {
		//return cacheNamePrefix + key;
		return key;
	}

	protected Collection<String> getCanonicalizedKeys(final Collection<String> keys) {

		return Collections2.transform(keys, new Function<String, String>() {

			@Override
			@Nullable
			public String apply(@Nullable String input) {
				return cacheNamePrefix + input;
			}
		});
	}

	protected Collection<String> getCanonicalizedKeys(final String ... keys) {

		List<String> cKeys = new ArrayList<String>();
		for (String key : keys) {
			cKeys.add(cacheNamePrefix + key);
		}
		return cKeys;
	}

	public String toString() {
		return this.cacheName;
	}

	public static class Builder {

		private String appName;
		private String clusterName;
		private ConnectionPoolConfigurationImpl cpConfig;

		public Builder(String name) {
			appName = name;
		}

		public Builder withDynomiteClusterName(String cluster) {
			clusterName = cluster;
			return this;
		}

		public Builder withConnectionPoolConfig(ConnectionPoolConfigurationImpl config) {
			cpConfig = config;
			return this;
		}

		public DynoMCacheClient build() {

			assert(appName != null);
			assert(clusterName != null);
			assert(cpConfig != null);

			//			CountingConnectionPoolMonitor cpMonitor = new CountingConnectionPoolMonitor();
			//			OperationMonitor opMonitor = new LastOperationMonitor();
			DynoCPMonitor cpMonitor = new DynoCPMonitor(appName);
			DynoOPMonitor opMonitor = new DynoOPMonitor(appName);

			MemcachedConnectionFactory connFactory = new MemcachedConnectionFactory(cpConfig, cpMonitor);

			RollingMemcachedConnectionPoolImpl<MemcachedClient> pool = 
					new RollingMemcachedConnectionPoolImpl<MemcachedClient>(appName, connFactory, cpConfig, cpMonitor, opMonitor);

			try {
				pool.start().get();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			final DynoMCacheClient client = new DynoMCacheClient(appName, pool);

			return client;
		}

		public static Builder withName(String name) {
			return new Builder(name);
		}
	}
}