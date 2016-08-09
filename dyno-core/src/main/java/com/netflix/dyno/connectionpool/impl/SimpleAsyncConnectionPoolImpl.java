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
package com.netflix.dyno.connectionpool.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionFactory;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.impl.lb.CircularList;

public class SimpleAsyncConnectionPoolImpl<CL> implements HostConnectionPool<CL> {

	private static final Logger Logger = LoggerFactory.getLogger(SimpleAsyncConnectionPoolImpl.class);
	
	private final Host host;
	private final ConnectionFactory<CL> connFactory;
	private final ConnectionPoolConfiguration cpConfig;
	private final ConnectionPoolMonitor cpMonitor;
	
	// state to track the connections being used
	private final CircularList<Connection<CL>> rrSelector = new CircularList<Connection<CL>>(new ArrayList<Connection<CL>>());
	private final ConcurrentHashMap<Connection<CL>, Connection<CL>> connMap = new ConcurrentHashMap<Connection<CL>, Connection<CL>>();

	// Tracking state of host connection pool.
	private final AtomicBoolean active = new AtomicBoolean(false);
	private final AtomicBoolean reconnecting = new AtomicBoolean(false);
	
	public SimpleAsyncConnectionPoolImpl(Host host, ConnectionFactory<CL> cFactory, 
										 ConnectionPoolConfiguration config, ConnectionPoolMonitor monitor) {
		
		this.host = host;
		this.connFactory = cFactory;
		this.cpConfig = config;
		this.cpMonitor = monitor;
	}
	
	@Override
	public Connection<CL> borrowConnection(int duration, TimeUnit unit) throws DynoException {
		
		if (!active.get()) {
			throw new DynoConnectException("Cannot connect to pool when pool is shutdown for host: " + host);
		}

		long start = System.currentTimeMillis();
		Connection<CL> connection = rrSelector.getNextElement();
		if (connection == null) {
			throw new DynoConnectException("Cannot find connection for host: " + host);
		}
		cpMonitor.incConnectionBorrowed(host, System.currentTimeMillis() - start);
		return connection;
	}

	@Override
	public boolean returnConnection(Connection<CL> connection) {
		try {
			if (!active.get()) {
				// Just close the connection
				return closeConnection(connection);
				
			} else {
				// do nothing here
				return false;
			}
		} finally { 
			cpMonitor.incConnectionReturned(host);
		}
	}
	
	@Override
	public boolean closeConnection(Connection<CL> connection) {
		try  {
			Connection<CL> prevConnection = connMap.remove(connection);
			if (prevConnection != null) {
				connection.close();
				rrSelector.removeElement(connection);
				cpMonitor.incConnectionClosed(host, connection.getLastException());
			}
			return true;
		} catch (Exception e) {
			Logger.error("Failed to close connection for host: " + host, e);
			return false;
		} finally {
		}
	}


	@Override
	public void markAsDown(DynoException reason) {
		
		if (!active.get()) {
			return; // already marked as down
		}
		
		active.compareAndSet(true, false);
	}

	@Override
	public void reconnect() {
		
		if (active.get()) {
			Logger.info("Pool already active, ignoring reconnect connections request");
			return;
		}
		
		if (reconnecting.get()) {
			Logger.info("Pool already reconnecting, ignoring reconnect connections request");
			return;
		}
		
		if (!(reconnecting.compareAndSet(false, true))) {
			Logger.info("Pool already reconnecting, ignoring reconnect connections request");
			return;
		}
		
		try {
			shutdown();
			primeConnections();
		} finally {
			reconnecting.set(false);
		}
	}
	
	@Override
	public void shutdown() {
		
		Logger.info("Shutting down connection pool for host:" + host);
		active.set(false);
		
		for (Connection<CL> connection : connMap.keySet()) {
			closeConnection(connection);
		}
		
		connMap.clear();
	}

	@Override
	public int primeConnections() throws DynoException {

		Logger.info("Priming connection pool for host:" + host);

		if(active.get()) {
			throw new DynoException("Connection pool has already been inited, cannot prime connections for host:" + host);
		}
		
		int created = 0;
		for (int i=0; i<cpConfig.getMaxConnsPerHost(); i++) {
			try { 
				createConnection();
				created++;
			} catch (DynoConnectException e) {
				Logger.error("Failed to create connection", e);
				cpMonitor.incConnectionCreateFailed(host, e);
				throw e;
			}
		}
		active.compareAndSet(false, true);
		
		return created;
	}

	@Override
	public Collection<Connection<CL>> getAllConnections() {
		return connMap.keySet();
	}

	@Override
	public int getConnectionTimeout() {
		return cpConfig.getConnectTimeout();
	}

	private Connection<CL> createConnection() throws DynoException {
		
		Connection<CL> connection = connFactory.createConnection((HostConnectionPool<CL>) this, null);
		connMap.put(connection, connection);
		connection.open();
		rrSelector.addElement(connection);

		cpMonitor.incConnectionCreated(host);
		return connection;
	}
	
	@Override
	public Host getHost() {
		return host;
	}

	@Override
	public boolean isActive() {
		return active.get();
	}

	@Override
	public boolean isShutdown() {
		return !active.get();
	}
}
