package com.netflix.dyno.memcache;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

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
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.OperationMonitor;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.impl.LastOperationMonitor;

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
		 });
	 }
	 
		public <T> OperationResult<T> get(final String key) {
			
			return connPool.executeWithFailover(new Operation<MemcachedClient, T>() {

				@Override
				public T execute(MemcachedClient client, ConnectionContext state) throws DynoException {
					return (T) client.get(getCanonicalizedKey(key));
				}

				@Override
				public String getName() {
					return OpName.Get.name();
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
	
//	public static void main(String[] args) {
//		
//		String appName = "dynomite";
//		
//		
////		EurekaHostsSupplier supplier = new EurekaHostsSupplier("dynomite");
////		List<Host> hosts = supplier.getHosts();
////
////		for (Host host : hosts) {
////			System.out.println("Host: "  + host);
////		}
//
//		List<Host> hosts = new ArrayList<Host>();
//		hosts.add(new Host("ec2-54-237-47-72.compute-1.amazonaws.com",  11211).setDC("us-east-1c").setStatus(Status.Up));
////		hosts.add(new Host("ec2-54-198-49-149.compute-1.amazonaws.com", 11211).setDC("us-east-1c").setStatus(Status.Up));
////		hosts.add(new Host("ec2-54-205-213-52.compute-1.amazonaws.com", 11211).setDC("us-east-1c").setStatus(Status.Up));
//
//		ConnectionPoolConfiguration cpConfig = new ConnectionPoolConfigurationImpl(appName).setLocalDcAffinity(false);
//		CountingConnectionPoolMonitor cpMonitor = new CountingConnectionPoolMonitor();
//		OperationMonitor opMonitor = new LastOperationMonitor();
//		MemcachedConnectionFactory connFactory = new MemcachedConnectionFactory(cpConfig, cpMonitor);
//
//		RollingMemcachedConnectionPoolImpl<MemcachedClient> pool = 
//				new RollingMemcachedConnectionPoolImpl<MemcachedClient>(connFactory, cpConfig, cpMonitor, opMonitor);
//		
//		pool.updateHosts(hosts, Collections.<Host> emptyList());
//		
//		
//		try {
//			Thread.sleep(150);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		
//		System.out.println("Getting data");
//		DynoMCacheClient client = new DynoMCacheClient("Puneet", pool);
//		
//		OperationResult<Object> result = client.get("test");
//		
//		final AtomicInteger count = new AtomicInteger(0);
//		ExecutorService thPool = Executors.newFixedThreadPool(1);
//		
//		Future<Void> f = thPool.submit(new Callable<Void>() {
//
//			@Override
//			public Void call() throws Exception {
//				
//				boolean done = false;
//				while (!done) {
//					System.out.println("Count so far " + count.get());
//					Thread.sleep(5000);
//					if (count.get() >= 1000000) {
//						done = true;
//					}
//				}
//				// TODO Auto-generated method stub
//				return null;
//			}
//			
//		});
//
//		try {
//			InetSocketAddress addr  = new InetSocketAddress("ec2-54-237-47-72.compute-1.amazonaws.com", 11211);
//			MemcachedClient mc = new MemcachedClient(addr);
//
//			for (int i=0; i<1000000; i++) {
//				try {
//					mc.set("test" + i, 360000, "Value_" + i).get();
//					count.incrementAndGet();
//					System.out.println("Count : " + count.get());
//
//				} catch (Exception e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//
//			mc.shutdown();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//
////		try {
////			for (int i=0; i<1000000; i++) {
////				client.set("test" + i, UUID.randomUUID().toString()).get();
////				count.incrementAndGet();
////				System.out.println("Count : " + count.get());
////			}
////		} catch (Exception e) {
////			// TODO Auto-generated catch block
////			e.printStackTrace();
////		}
//		
////		
////		//System.out.println("Key: test, value: " + result.getResult());
////		try {
////			f.get();
////		} catch (InterruptedException e) {
////			// TODO Auto-generated catch block
////			e.printStackTrace();
////		} catch (ExecutionException e) {
////			// TODO Auto-generated catch block
////			e.printStackTrace();
////		}
//		thPool.shutdownNow();
//		pool.shutdown();
//
////		try {
////			InetSocketAddress addr  = new InetSocketAddress("ec2-54-237-47-72.compute-1.amazonaws.com", 11211);
////			MemcachedClient mc = new MemcachedClient(addr);
////			
////			mc.set("test", 36000, "puneetV");
////			String value = (String) mc.get("test");
////			System.out.println("Value: " + value);
////			mc.shutdown();
////		} catch (IOException e) {
////			e.printStackTrace();
////		}
//	}
}