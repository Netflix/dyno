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

import io.netty.channel.EventLoopGroup;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisClient;
import com.netflix.dyno.connectionpool.AsyncOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.ConnectionFactory;
import com.netflix.dyno.connectionpool.ConnectionObservor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.ListenableFuture;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.OperationMonitor;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.ThrottledException;
import com.netflix.dyno.connectionpool.impl.ConnectionContextImpl;
import com.netflix.dyno.connectionpool.impl.FutureOperationalResultImpl;
import com.netflix.dyno.connectionpool.impl.OperationResultImpl;

public class RedissonConnectionFactory implements ConnectionFactory<RedisAsyncConnection<String, String>> {
	
	private final EventLoopGroup eventGroupLoop;
	private final OperationMonitor opMonitor;
	
	public RedissonConnectionFactory(EventLoopGroup group, OperationMonitor operationMonitor) {
		eventGroupLoop = group;
		opMonitor = operationMonitor;
	}

	@Override
	public Connection<RedisAsyncConnection<String, String>> createConnection(HostConnectionPool<RedisAsyncConnection<String, String>> pool, ConnectionObservor connectionObservor)  throws DynoConnectException, ThrottledException {
		return new RedissonConnection(pool, eventGroupLoop, opMonitor);
	}
	
	public static class RedissonConnection implements Connection<RedisAsyncConnection<String, String>> {

		private final HostConnectionPool<RedisAsyncConnection<String, String>> hostPool;
		private final RedisClient client;
		private final OperationMonitor opMonitor;
		
		private RedisAsyncConnection<String, String> rConn = null;
		private final AtomicReference<DynoConnectException> lastEx = new AtomicReference<DynoConnectException>(null);

		private final ConnectionContextImpl context = new ConnectionContextImpl();
		
		public RedissonConnection(HostConnectionPool<RedisAsyncConnection<String, String>> hPool, EventLoopGroup eventGroupLoop, OperationMonitor opMonitor) {
			this.hostPool = hPool;
			Host host = hostPool.getHost();
			this.opMonitor = opMonitor;
			this.client = new RedisClient(eventGroupLoop, host.getHostName(), host.getPort());
		}
		
		@Override
		public <R> OperationResult<R> execute(Operation<RedisAsyncConnection<String, String>, R> op) throws DynoException {
			try { 
				R result = op.execute(rConn, null); // Note that connection context is not implemented yet
				return new OperationResultImpl<R>(op.getName(), result, opMonitor)
												 .attempts(1)
												 .setNode(getHost());
				
			} catch (DynoConnectException e) {
				lastEx.set(e);
				throw e;
			}
		}

		@Override
		public <R> ListenableFuture<OperationResult<R>> executeAsync(AsyncOperation<RedisAsyncConnection<String, String>, R> op) throws DynoException {
			final long start = System.currentTimeMillis();
			try { 
				Future<R> future = op.executeAsync(rConn);
				return new FutureOperationalResultImpl<R>(op.getName(), future, start, opMonitor).node(getHost());
				
			} catch (DynoConnectException e) {
				lastEx.set(e);
				throw e;
			}
		}

		@Override
		public void close() {
			rConn.close();
			client.shutdown();
		}

		@Override
		public Host getHost() {
			return hostPool.getHost();
		}

		@Override
		public void open() throws DynoException {
			rConn = client.connectAsync();
		}

		@Override
		public DynoConnectException getLastException() {
			return lastEx.get();
		}

		@Override
		public HostConnectionPool<RedisAsyncConnection<String, String>> getParentConnectionPool() {
			return hostPool;
		}

		@Override
		public void execPing() {
			try {
				rConn.ping().get();
			} catch (InterruptedException e) {
			} catch (ExecutionException e) {
				throw new DynoConnectException(e);
			}
		}

		@Override
		public ConnectionContext getContext() {
			return context;
		}
	}
}
