package com.netflix.dyno.memcache;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.ConnectionObserver;
import net.spy.memcached.MemcachedClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.AsyncOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.ConnectionFactory;
import com.netflix.dyno.connectionpool.ConnectionObservor;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.HostGroup;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.ThrottledException;
import com.netflix.dyno.connectionpool.impl.ConnectionContextImpl;
import com.netflix.dyno.connectionpool.impl.FutureOperationalResultImpl;
import com.netflix.dyno.connectionpool.impl.OperationResultImpl;

public class MemcachedConnectionFactory implements ConnectionFactory<MemcachedClient>{

	private static final Logger Logger = LoggerFactory.getLogger(MemcachedConnectionFactory.class);
	
	private final SpyMemcachedClientFactory mcClientFactory;
	private final ConnectionObserver connectionObserverAdaptor; 
	
	private final ConcurrentHashMap<SocketAddress, Host> saToHostMap = new ConcurrentHashMap<SocketAddress, Host>();
	private final ConcurrentHashMap<SocketAddress, ConnectionObservor> saToConnObservorMap = new ConcurrentHashMap<SocketAddress, ConnectionObservor>();
	
	public MemcachedConnectionFactory(ConnectionPoolConfiguration config, ConnectionPoolMonitor monitor) {
		
		this.mcClientFactory = new SpyMemcachedClientFactory(config, monitor);
		
		this.connectionObserverAdaptor = new ConnectionObserver() {

			@Override
			public void connectionEstablished(SocketAddress sa, int reconnectCount) {
				Logger.info("Connection established for SocketAddress: " + sa);
			}

			@Override
			public void connectionLost(SocketAddress sa) {
				Logger.info("\n\n HERE -----> Connection lost for SocketAddress: " + sa);
				
				Host hostThatIsLost = saToHostMap.get(sa);
				if (hostThatIsLost != null) {
					ConnectionObservor connObservor = saToConnObservorMap.get(sa);
					if (connObservor != null) {
						connObservor.connectionLost(hostThatIsLost);
					}
				}
			}
			
		};
	}
	
	@Override
	public Connection<MemcachedClient> createConnection(HostConnectionPool<MemcachedClient> pool, ConnectionObservor connObservor) throws DynoConnectException, ThrottledException {

		HostGroup hostGroup = (HostGroup) pool.getHost();
		if (hostGroup == null) {
			return null;
		}
		
		List<Host> activeHosts = hostGroup.getHostList();
		
		for (Host aHost : activeHosts) {
			saToHostMap.put(aHost.getSocketAddress(), aHost);
			saToConnObservorMap.put(aHost.getSocketAddress(), connObservor);
		}
		
		try {
			MemcachedClient mcClient = mcClientFactory.createMemcachedClient(activeHosts);
			mcClient.addObserver(connectionObserverAdaptor);
			return new SpyMemcachedConnection(pool, mcClient);
		} catch (IOException e) {
			throw new DynoConnectException(e);
		}
	}
	
	
	
	private class SpyMemcachedConnection implements Connection<MemcachedClient> {
		
		private final AtomicReference<DynoConnectException> lastEx = new AtomicReference<DynoConnectException>(null);

		private final HostConnectionPool<MemcachedClient> mConnPool; 
		private final MemcachedClient mClient; 
		
		private final ConnectionContextImpl context = new ConnectionContextImpl();
		
		private SpyMemcachedConnection(HostConnectionPool<MemcachedClient> pool, MemcachedClient client) {
			this.mConnPool = pool;
			this.mClient = client;
		}
		

		@Override
		public void close() {

			try { 
				mClient.removeObserver(connectionObserverAdaptor);
				mClient.shutdown();
				
			} catch (Exception e) {
				Logger.error("Failed to shutdown MemcachedClient", e);
			}
		}

		@Override
		public Host getHost() {
			return mConnPool.getHost();
		}

		@Override
		public void open() throws DynoException {
			// Nothing to do here
		}

		@Override
		public <R> OperationResult<R> execute(Operation<MemcachedClient, R> op) throws DynoException {
			
			try { 
				R result = op.execute(mClient, null); // Note that connection context is not implemented yet
				return new OperationResultImpl<R>(op.getName(), result, null)
												 .attempts(1)
												 .setNode(getHost());
				
			} catch (DynoConnectException e) {
				lastEx.set(e);
				throw e;
			}
		}


		@Override
		public DynoConnectException getLastException() {
			return lastEx.get();
		}

		@Override
		public <R> Future<OperationResult<R>> executeAsync(AsyncOperation<MemcachedClient, R> op) throws DynoException {
			
			final long start = System.currentTimeMillis();
			try { 
				Future<R> future = op.executeAsync(mClient);
				return new FutureOperationalResultImpl<R>(op.getName(), future, start, null).node(getHost());
				
			} catch (DynoConnectException e) {
				lastEx.set(e);
				throw e;
			}
		}
	

		@Override
		public HostConnectionPool<MemcachedClient> getParentConnectionPool() {
			return mConnPool;
		}


		private final String PingKey = UUID.randomUUID().toString();
		@Override
		public void execPing() {
			mClient.get(PingKey);
		}


		@Override
		public ConnectionContext getContext() {
			return context;
		}
	}
	
}
