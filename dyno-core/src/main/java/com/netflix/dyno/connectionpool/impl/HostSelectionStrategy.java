package com.netflix.dyno.connectionpool.impl;

import java.util.concurrent.TimeUnit;

import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.exception.PoolExhaustedException;

public interface HostSelectionStrategy<CL> {

	public Connection<CL> getConnection(Operation<CL, ?> op, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException;
	
	public void addHost(Host host, HostConnectionPool<CL> hostPool);
	
	public void removeHost(Host host, HostConnectionPool<CL> hostPool);
}
