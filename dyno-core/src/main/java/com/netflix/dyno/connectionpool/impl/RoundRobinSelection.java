package com.netflix.dyno.connectionpool.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.exception.PoolExhaustedException;
import com.netflix.dyno.connectionpool.exception.PoolTimeoutException;

public class RoundRobinSelection<CL> implements HostSelectionStrategy<CL> {

	private final ConcurrentHashMap<Host, HostConnectionPool<CL>> currentMap = new ConcurrentHashMap<Host, HostConnectionPool<CL>>();
	
	private final CircularList<HostConnectionPool<CL>> circularList = new CircularList<HostConnectionPool<CL>>(null);
	
	public RoundRobinSelection(Map<Host, HostConnectionPool<CL>> hostPools) {
		
		currentMap.putAll(hostPools);
		circularList.swapWithList(hostPools.values());
	}
	
	@Override
	public Connection<CL> getConnection(BaseOperation<CL, ?> op, int duration, TimeUnit unit) {
		
		int totalHosts = currentMap.size();
		
		if (totalHosts == 0) {
			throw new NoAvailableHostsException("No hosts to load balance over!");
		}
		
		int timeoutPerHost = duration/totalHosts;
		int count = totalHosts;
		
		do {
			
			HostConnectionPool<CL> hostPool = circularList.getNextElement();
			Connection<CL> connection = null; 
			
			try { 
				connection = hostPool.borrowConnection(timeoutPerHost, unit);
				if (connection != null) {
					return connection;
				}
			} catch (PoolTimeoutException e) {
				
			} finally {
				count--;
			}
			
		} while (count > 0);
		
		throw new PoolExhaustedException("Connection pool exhausted!");
	}

	@Override
	public synchronized void addHost(Host host, HostConnectionPool<CL> hostPool) {
		
		HostConnectionPool<CL> oldPool = currentMap.putIfAbsent(host, hostPool);
		if (oldPool != null) {
			return;
		}
		
		circularList.addElement(hostPool);
	}

	@Override
	public synchronized void removeHost(Host host, HostConnectionPool<CL> hostPool) {
		
		HostConnectionPool<CL> poolToRemove = currentMap.get(host);
		if (poolToRemove == null) {
			return;
		}
		
		circularList.removeElement(poolToRemove);
	}

}
