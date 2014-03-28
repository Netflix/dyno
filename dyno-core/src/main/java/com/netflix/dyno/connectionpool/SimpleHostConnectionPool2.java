package com.netflix.dyno.connectionpool;

import java.util.concurrent.LinkedBlockingQueue;

import com.netflix.dyno.connectionpool.exception.DynoException;

public class SimpleHostConnectionPool2<CL> implements HostConnectionPool<CL> {
	
	// The connections available for this connection pool
	private final LinkedBlockingQueue<Connection<CL>> availableConnections = new LinkedBlockingQueue<Connection<CL>>();
	
	@Override
	public Connection<CL> borrowConnection(int timeout) throws DynoException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean returnConnection(Connection<CL> connection) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean closeConnection(Connection<CL> connection) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void markAsDown(DynoException reason) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int primeConnections(int numConnections) throws DynoException,
			InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Host getHost() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isActive() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isShutdown() {
		// TODO Auto-generated method stub
		return false;
	}

}
