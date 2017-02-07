/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.redisson;

import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lambdaworks.redis.RedisAsyncConnection;
import com.netflix.dyno.connectionpool.AsyncOperation;
import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.DecoratingListenableFuture;
import com.netflix.dyno.connectionpool.ListenableFuture;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl;
import com.netflix.dyno.connectionpool.impl.HostConnectionPoolFactory.Type;
import com.netflix.dyno.contrib.DynoCPMonitor;
import com.netflix.dyno.contrib.DynoOPMonitor;

public class DynoRedissonClient {
	
	private static final Logger Logger = LoggerFactory.getLogger(DynoRedissonClient.class);
	
	private final ConnectionPool<RedisAsyncConnection<String, String>> connPool;
	
	public DynoRedissonClient(String name, ConnectionPool<RedisAsyncConnection<String, String>> pool) {
		this.connPool = pool;
	}

	private enum OpName { 
		Set, Delete, Get, GetBulk, GetAsync;
	}
	
	
	public Future<OperationResult<String>> get(final String key) throws DynoException {
		
		return connPool.executeAsync(new AsyncOperation<RedisAsyncConnection<String, String>, String> () {

			@Override
			public String getName() {
				return OpName.Get.name();
			}

			@Override
			public String getKey() {
				return key;
			}

			@Override
			public ListenableFuture<String> executeAsync(RedisAsyncConnection<String, String> client) throws DynoException {
				return new DecoratingListenableFuture<String>((client.get(key)));
			}

            @Override
            public byte[] getBinaryKey() {
                return null;
            }
		});
	}
	
	public Future<OperationResult<String>> get(final String key, final String value) throws DynoException {
		
		return connPool.executeAsync(new AsyncOperation<RedisAsyncConnection<String, String>, String> () {

			@Override
			public String getName() {
				return OpName.Set.name();
			}

			@Override
			public String getKey() {
				return key;
			}
				

			@Override
			public ListenableFuture<String> executeAsync(RedisAsyncConnection<String, String> client) throws DynoException {
				return new DecoratingListenableFuture<String>((client.set(key, value)));
			}

            @Override
            public byte[] getBinaryKey() {
                return null;
            }
		});
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

		public Builder withCPConfig(ConnectionPoolConfigurationImpl config) {
			cpConfig = config;
			return this;
		}

		public DynoRedissonClient build() {

			assert(appName != null);
			assert(clusterName != null);
			assert(cpConfig != null);
			
			DynoCPMonitor cpMonitor = new DynoCPMonitor(appName);
			DynoOPMonitor opMonitor = new DynoOPMonitor(appName);
			
			RedissonConnectionFactory connFactory = new RedissonConnectionFactory(new NioEventLoopGroup(4), opMonitor);

			ConnectionPoolImpl<RedisAsyncConnection<String, String>> pool = 
					new ConnectionPoolImpl<RedisAsyncConnection<String, String>>(connFactory, cpConfig, cpMonitor, Type.Async);
			
			try {
				pool.start().get();
			} catch (Exception e) {
                if (cpConfig.getFailOnStartupIfNoHosts()) {
                    throw new RuntimeException(e);
                }

				Logger.warn("UNABLE TO START CONNECTION POOL -- IDLING");
                pool.idle();
			}
			
			final DynoRedissonClient client = new DynoRedissonClient(appName, pool);
			return client;
		}
		
		public static Builder withName(String name) {
			return new Builder(name);
		}
	}


}
