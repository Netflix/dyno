/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.dyno.connectionpool.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionFactory;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.RetryPolicy;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.PoolOfflineException;
import com.netflix.dyno.connectionpool.exception.PoolTimeoutException;

/**
 * Main impl for {@link HostConnectionPool}
 * 
 * This class does not allow shared access to the connections being managed for this pool. 
 * Hence it uses a {@link LinkedBlockingQueue} to manage the available connections. 
 * When a connection needs to be borrowed, we wait or poll the queue. As connections are returned, they are added back into the queue. 
 * This is the normal behavior during the "Active" state of this pool. 
 * 
 * The class also manages another state called "Inactive" where it can be put "Down" where it stops accepting requests for borrowing more connections, 
 * and simply terminates every connection that is returned to it. This is generally useful when the host is going away, or where the error rate 
 * from the connections of this pool are greater than a configured error threshold and then an external component decides to recycle the connection pool. 
 * 
 * @author poberai
 *
 * @param <CL>
 */
public class HostConnectionPoolImpl<CL> implements HostConnectionPool<CL> {

	private static final Logger Logger = LoggerFactory.getLogger(HostConnectionPoolImpl.class);
	
	// The connections available for this connection pool
	private final LinkedBlockingQueue<Connection<CL>> availableConnections = new LinkedBlockingQueue<Connection<CL>>();
	// Track the no of connections open (both available and in use)
	private final AtomicInteger numActiveConnections = new AtomicInteger(0);
	
	// Private members required by this class
	private final Host host; 
	private final ConnectionFactory<CL> connFactory; 
	private final ConnectionPoolConfiguration cpConfig; 
	private final ConnectionPoolMonitor monitor; 
	
	// states that dictate the behavior of the pool
	
	// cp not inited is the starting state of the pool. The pool will not allow connections to be borrowed in this state
	private final ConnectionPoolState<CL> cpNotInited = new ConnectionPoolNotInited();
	// cp active is where connections of the pool can be borrowed and returned
	private final ConnectionPoolState<CL> cpActive = new ConnectionPoolActive(this);
	// cp reconnecting is where connections cannot be borrowed and all returning connections will be shutdown
	private final ConnectionPoolState<CL> cpReconnecting = new ConnectionPoolReconnectingOrDown();
	// similar to reconnecting
	private final ConnectionPoolState<CL> cpDown = new ConnectionPoolReconnectingOrDown();
	
	// The thread safe reference to the pool state
	private final AtomicReference<ConnectionPoolState<CL>> cpState = new AtomicReference<ConnectionPoolState<CL>>(cpNotInited);
	
	public HostConnectionPoolImpl(Host host, ConnectionFactory<CL> conFactory, 
			                      ConnectionPoolConfiguration cpConfig, ConnectionPoolMonitor poolMonitor) {
		this.host = host;
		this.connFactory = conFactory;
		this.cpConfig = cpConfig;
		this.monitor = poolMonitor;
	}
	
	@Override
	public Connection<CL> borrowConnection(int duration, TimeUnit unit) throws DynoException {
		return cpState.get().borrowConnection(duration, unit);
	}

	@Override
	public boolean returnConnection(Connection<CL> connection) {
		return cpState.get().returnConnection(connection);
	}

	@Override
	public boolean closeConnection(Connection<CL> connection) {
		return cpState.get().closeConnection(connection);
	}

	@Override
	public void markAsDown(DynoException reason) {
		
		ConnectionPoolState<CL> currentState = cpState.get();
		
		if (currentState == cpDown) {
			if (Logger.isDebugEnabled()) {
				Logger.debug("CP is already down, hence ignoring mark as down request");
			}
			return;
		}
		
		if (!(cpState.compareAndSet(currentState, cpDown))) {
			// someone already beat us to it
			return;
		}
		
		monitor.hostDown(host, reason);
        monitor.resetConnectionBorrowedLatStats(); // NOTE - SIDE EFFECT
	}

	@Override
    public void reconnect() {

        markAsDown(null);
		reconnect(cpDown);
	
		if (cpState.get() == cpActive) {
			monitor.hostUp(host, this);
		}
	}

	@Override
	public void shutdown() {
		
		Logger.info("Shutting down connection pool for host:" + host);
		cpState.set(cpDown);
		
		List<Connection<CL>> connections = new ArrayList<Connection<CL>>();
		availableConnections.drainTo(connections);
		
		for (Connection<CL> connection : connections) {
			cpState.get().closeConnection(connection);
		}
	}

	@Override
	public int primeConnections() throws DynoException {

		Logger.info("Priming connection pool for host:" + host + ", with conns:" + cpConfig.getMaxConnsPerHost());

		if(cpState.get() != cpNotInited) {
			throw new DynoException("Connection pool has already been inited, cannot prime connections for host:" + host);
		}

        int primedConnectionCount = reconnect(cpNotInited);

        if (primedConnectionCount == 0) {
            Logger.warn("Unable to make any successful connections to host " + host);
            cpState.set(cpNotInited);
            throw new DynoConnectException("Unable to make ANY successful connections to host " + host);
        }

        return primedConnectionCount;
	}

	private int reconnect(ConnectionPoolState<CL> prevState) throws DynoException {

		if (!(cpState.compareAndSet(prevState, cpReconnecting))) {
			Logger.info("Reconnect connections already called by someone else, ignoring reconnect connections request");
			return 0;
		}
		
		int successfullyCreated = 0; 
		
		for (int i=0; i<cpConfig.getMaxConnsPerHost(); i++) {
			boolean success = createConnectionWithRetries();
			if (success) {
				successfullyCreated++;
			}
		}
		
		if (successfullyCreated == cpConfig.getMaxConnsPerHost()) {
			if (!(cpState.compareAndSet(cpReconnecting, cpActive))) {
				throw new IllegalStateException("something went wrong with prime connections");
			}
		} else {
			if (!(cpState.compareAndSet(cpReconnecting, cpDown))) {
				throw new IllegalStateException("something went wrong with prime connections");
			}
		}
		return successfullyCreated;
	}
	
	private boolean createConnectionWithRetries() {
		
		boolean success = false;
		RetryPolicy retry = new RetryNTimes.RetryFactory(3).getRetryPolicy();
		
		retry.begin();
		
		while (retry.allowRetry()) {
			
			try {
				cpActive.createConnection();
				retry.success();
				success = true;
				break;
			} catch (DynoException e) {
				retry.failure(e);
			}
		}
		
		return success;
	}

	@Override
	public Host getHost() {
		return host;
	}

	@Override
	public boolean isActive() {
		return cpState.get() == cpActive;
	}

	@Override
	public boolean isShutdown() {
		return cpState.get() == cpDown;
	}
	
	/**
	 * DO NOT call this method on this pool. This pool needs to manage shared thread safe access to connections
	 * and hence at any given time all connections are being used by some operation. 
	 * In any case getAllConnections() is meant for ping based active monitoring of connections which is not needed for this 
	 * pool since it is "sync" in nature. For sync pools we collect feedback from the operations directly and relay that to 
	 * ConnectionPoolHealthChecker.
	 * 
	 */
	@Override
	public Collection<Connection<CL>> getAllConnections() {
		throw new RuntimeException("Not Implemented");
	}

	@Override
	public int getConnectionTimeout() {
		return cpConfig.getConnectTimeout();
	}

	private interface ConnectionPoolState<CL> { 
		
		
		Connection<CL> createConnection(); 
		
		Connection<CL> borrowConnection(int duration, TimeUnit unit);
		
		boolean returnConnection(Connection<CL> connection);
		
		boolean closeConnection(Connection<CL> connection);
		
	}
	
	
	private class ConnectionPoolActive implements ConnectionPoolState<CL> {

		private final HostConnectionPoolImpl<CL> pool; 
		
		private ConnectionPoolActive(HostConnectionPoolImpl<CL> cp) {
			pool = cp;
		}
		
		@Override
		public Connection<CL> createConnection() {
			
			try { 
				Connection<CL> connection = connFactory.createConnection((HostConnectionPool<CL>) pool, null);
				connection.open();
				availableConnections.add(connection);

				monitor.incConnectionCreated(host);
				numActiveConnections.incrementAndGet();
				
				return connection;
			} catch (DynoConnectException e) {
				if (Logger.isDebugEnabled()) {
                    if (monitor.getConnectionCreateFailedCount() % 10000 == 0) {
                        Logger.error("Failed to create connection", e);
                    } else {
                        Logger.error("Failed to create connection" + e.getMessage());
                    }
				}
				monitor.incConnectionCreateFailed(host, e);
				throw e;
			} catch (RuntimeException e) {
				if (Logger.isDebugEnabled()) {
                    if (monitor.getConnectionCreateFailedCount() % 10000 == 0) {
                        Logger.error("Failed to create connection", e);
                    } else {
                        Logger.error("Failed to create connection" + e.getMessage());
                    }
				}
				monitor.incConnectionCreateFailed(host, e);
				throw new DynoConnectException(e);
			}
		}


		@Override
		public boolean returnConnection(Connection<CL> connection) {
			try {
				if (numActiveConnections.get() > cpConfig.getMaxConnsPerHost()) {
					
					// Just close the connection
					return closeConnection(connection);
					
				} else {
					// add connection back to the pool
					availableConnections.add(connection);
					return false;
				}
			} finally { 
				monitor.incConnectionReturned(host);
			}
		}

		@Override
		public boolean closeConnection(Connection<CL> connection) {
			try  {
				connection.close();
				return true;
			} catch (Exception e) {
				Logger.error("Failed to close connection for host: " + host + " " + e.getMessage());
				return false;
			} finally {
				numActiveConnections.decrementAndGet();
				monitor.incConnectionClosed(host, connection.getLastException());
			}
		}
		
		@Override
		public Connection<CL> borrowConnection(int duration, TimeUnit unit) {

			// Start recording how long it takes to get the connection - for insight/metrics
			long startTime = System.nanoTime()/1000;

			Connection<CL> conn = null;
			try {
				// wait on the connection pool with a timeout
				conn = availableConnections.poll(duration, unit);
			} catch (InterruptedException e) {
				Logger.info("Thread interrupted when waiting on connections");
				throw new DynoConnectException(e);
			}

			long delay = System.nanoTime()/1000 - startTime;

			if (conn == null) {
				throw new PoolTimeoutException("Fast fail waiting for connection from pool")
				.setHost(getHost())
				.setLatency(delay);
			}

            monitor.incConnectionBorrowed(host, delay);
			return conn;
		}
	}


	
	private class ConnectionPoolReconnectingOrDown implements ConnectionPoolState<CL> {
		
		private ConnectionPoolReconnectingOrDown() {
		}

		@Override
		public Connection<CL> createConnection() {
			throw new PoolOfflineException(getHost(), "Cannot create new connection when pool is down");
		}

		@Override
		public Connection<CL> borrowConnection(int duration, TimeUnit unit) {
			throw new PoolOfflineException(getHost(), "Cannot borrow connection when pool is down");
		}

		@Override
		public boolean returnConnection(Connection<CL> connection) {
			
			monitor.incConnectionReturned(host);
			return closeConnection(connection);
		}

		@Override
		public boolean closeConnection(Connection<CL> connection) {
			try  {
				connection.close();
				return true;
			} catch (Exception e) {
				Logger.warn("Failed to close connection for host: " + host + " " + e.getMessage());
				return false;
			} finally {
				numActiveConnections.decrementAndGet();
				monitor.incConnectionClosed(host, connection.getLastException());
			}
		}
	}
	
	private class ConnectionPoolNotInited implements ConnectionPoolState<CL> {
		
		private ConnectionPoolNotInited() {
		}

		@Override
		public Connection<CL> createConnection() {
			throw new DynoConnectException("Pool must be inited first");
		}

		@Override
		public Connection<CL> borrowConnection(int duration, TimeUnit unit) {
			throw new DynoConnectException("Pool must be inited first");
		}

		@Override
		public boolean returnConnection(Connection<CL> connection) {
			throw new DynoConnectException("Pool must be inited first");
		}

		@Override
		public boolean closeConnection(Connection<CL> connection) {
			throw new DynoConnectException("Pool must be inited first");
		}
	}
	
	public String toString() {
		return "HostConnectionPool: [Host: " + host + ", Pool active: " + isActive() + "]";
	}
}
