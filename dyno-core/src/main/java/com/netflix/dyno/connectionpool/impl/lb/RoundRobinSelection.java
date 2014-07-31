package com.netflix.dyno.connectionpool.impl.lb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.exception.PoolExhaustedException;
import com.netflix.dyno.connectionpool.impl.HostSelectionStrategy;

public class RoundRobinSelection<CL> implements HostSelectionStrategy<CL> {

	private final ConcurrentHashMap<Host, HostConnectionPool<CL>> hostPools = new ConcurrentHashMap<Host, HostConnectionPool<CL>>();

	private final CircularList<Host> circularList = new CircularList<Host>(null);

	public RoundRobinSelection() {
		
	}
	
	@Override
	public Connection<CL> getConnection(BaseOperation<CL, ?> op, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {

		int n = circularList.getSize();
		Connection<CL> connection = null;
		
		while ((n > 0)  && (connection == null)) {
			connection = getNextConnection(duration, unit);
			n--;
		}
		return connection;
	}
	
	private Connection<CL> getNextConnection(int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {

		Host host = circularList.getNextElement();

		HostConnectionPool<CL> hostPool = hostPools.get(host);
		if (hostPool == null || !hostPool.isActive()) {
			return null;
		}
		
		Connection<CL> connection = hostPool.borrowConnection(duration, unit);
		return connection;
	}

	@Override
	public Map<BaseOperation<CL, ?>, Connection<CL>> getConnection(Collection<BaseOperation<CL, ?>> ops, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {
		throw new RuntimeException("Not Implemented");
	}

	@Override
	public Connection<CL> getFallbackConnection(BaseOperation<CL, ?> op, int duration, TimeUnit unit) throws NoAvailableHostsException, PoolExhaustedException {
		throw new RuntimeException("Not Implemented");
	}

	@Override
	public void initWithHosts(Map<Host, HostConnectionPool<CL>> hPools) {
		hostPools.putAll(hPools);
		circularList.swapWithList(hostPools.keySet());
	}

	@Override
	public void addHost(Host host, HostConnectionPool<CL> hostPool) {
		
		hostPools.put(host, hostPool);
		List<Host> newHostList = new ArrayList<Host>(circularList.getEntireList());
		newHostList.add(host);
		circularList.swapWithList(newHostList);
	}

	@Override
	public void removeHost(Host host, HostConnectionPool<CL> hostPool) {

		hostPools.remove(host);
		List<Host> newHostList = new ArrayList<Host>(circularList.getEntireList());
		newHostList.remove(host);
		circularList.swapWithList(newHostList);
	}

	
	public String toString() {
		return "RoundRobinSelector: list: " + circularList.toString();
	}

}
