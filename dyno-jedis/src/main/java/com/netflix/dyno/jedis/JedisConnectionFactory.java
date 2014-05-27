package com.netflix.dyno.jedis;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.NotImplementedException;

import redis.clients.jedis.Jedis;

import com.netflix.dyno.connectionpool.AsyncOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionFactory;
import com.netflix.dyno.connectionpool.ConnectionObservor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.OperationMonitor;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.ThrottledException;
import com.netflix.dyno.connectionpool.impl.OperationResultImpl;

public class JedisConnectionFactory implements ConnectionFactory<Jedis> {

	private final OperationMonitor opMonitor; 
	
	public JedisConnectionFactory(OperationMonitor monitor) {
		this.opMonitor = monitor;
	}
	
	@Override
	public Connection<Jedis> createConnection(HostConnectionPool<Jedis> pool, ConnectionObservor connectionObservor) 
			throws DynoConnectException, ThrottledException {
		
		return new JedisConnection(pool);
	}

	
	private class JedisConnection implements Connection<Jedis> {

		private final HostConnectionPool<Jedis> hostPool;
		private final Jedis jedisClient; 
		
		private DynoConnectException lastDynoException;
		
		public JedisConnection(HostConnectionPool<Jedis> hostPool) {
			this.hostPool = hostPool;
			Host host = hostPool.getHost();
			jedisClient = new Jedis(host.getHostName(), host.getPort());
		}
		
		@Override
		public <R> OperationResult<R> execute(Operation<Jedis, R> op) throws DynoException {
			
			long startTime = System.currentTimeMillis();
			String opName = op.getName();

			OperationResultImpl<R> opResult = null;
			
			try { 
				R result = op.execute(jedisClient, null);
				opMonitor.recordSuccess(opName);
				opResult = new OperationResultImpl<R>(opName, result, opMonitor);
				return opResult;
				
			} catch (DynoException ex) {
				opMonitor.recordFailure(opName, ex.getMessage());
				if (ex instanceof DynoConnectException) {
					lastDynoException = (DynoConnectException) ex;
				}
				throw ex;
				
			} finally {
				long duration = System.currentTimeMillis() - startTime;
				if (opResult != null) {
					opResult.setLatency(duration, TimeUnit.MILLISECONDS);
				}
			}
		}

		@Override
		public <R> Future<OperationResult<R>> executeAsync(AsyncOperation<Jedis, R> op) throws DynoException {
			throw new NotImplementedException();
		}

		@Override
		public void close() {
			jedisClient.quit();
		}

		@Override
		public Host getHost() {
			return hostPool.getHost();
		}

		@Override
		public void open() throws DynoException {
			jedisClient.connect();
		}

		@Override
		public DynoConnectException getLastException() {
			return lastDynoException;
		}

		@Override
		public HostConnectionPool<Jedis> getParentConnectionPool() {
			return hostPool;
		}
	}
}
