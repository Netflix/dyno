package com.netflix.dyno.connectionpool.impl.health;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.FatalConnectionException;
import com.netflix.dyno.connectionpool.exception.TimeoutException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl.ErrorRateMonitorFactory;

public class ConnectionPoolHealthTracker<CL> {
	
	private static final Logger Logger = LoggerFactory.getLogger(ConnectionPoolHealthTracker.class);
	
	private final ConnectionPoolConfiguration cpConfiguration;
	private final ExecutorService threadPool;
	private final AtomicBoolean stop = new AtomicBoolean(false);
	private final ConcurrentHashMap<Host, ErrorRateMonitor> errorRates = new ConcurrentHashMap<Host, ErrorRateMonitor>();
	private final ConcurrentHashMap<Host, HostConnectionPool<CL>> pendingPools = new ConcurrentHashMap<Host, HostConnectionPool<CL>>();
	
	private final Integer SleepMillis = 10*1000; // 30 seconds
	
	public ConnectionPoolHealthTracker(ConnectionPoolConfiguration config, ExecutorService thPool) {
		cpConfiguration = config;	
		threadPool = thPool;
	}
		
	public void start() {
		
		threadPool.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				
				while (!stop.get() && !Thread.currentThread().isInterrupted()) {
					
					//if (pendingPools.size() > 0) {
						Logger.info("Running, pending pools size: " + pendingPools.size());
					//}
					
					for (Host host : pendingPools.keySet()) {
						
						if (!host.isUp()) {
							Logger.info("Host: " + host + " is marked as down, will not reconnect connection pool");
							pendingPools.remove(host);
							continue;
						}
						
						HostConnectionPool<CL> pool = pendingPools.get(host);
						System.out.println("Host whose pool will be reconnected:  " + host.getHostName());
						System.out.println("Pool is ACTIVE?  " + pool.isActive());
						if (pool.isActive()) {
							// Pool is already active. Move on
							pendingPools.remove(host);
						} else {
							try {
								pool.markAsDown(null);
								pool.reconnect();
								if (pool.isActive()) {
									Logger.info("Host pool reactivated: " + host);
									pendingPools.remove(host);
								} else {
									Logger.info("Could not re-activate pool, will try again later");
								}
							} catch (Exception e) {
								// do nothing, will retry again once thread wakes up
								Logger.warn("Failed to reconnect pool", e);
							}
						}
					}
					
					Thread.sleep(SleepMillis);
				}
				return null;
			}
		});
	}
	
	public void stop() {
		stop.set(true);
	}
	
	public void trackConnectionError(HostConnectionPool<CL> hostPool, DynoException e) {
			
		if (e != null && e instanceof TimeoutException) {
			// don't track timeouts, since that may not be indicative of an actual n/w problem
			// that may just be a slowdown due to pool saturation of larger payloads
			return; 
		}
		
		if (e != null && e instanceof FatalConnectionException) {

			Host host = hostPool.getHost();
			
			ErrorRateMonitor errorMonitor = errorRates.get(host);

			if (errorMonitor == null) {
				errorMonitor = ErrorRateMonitorFactory.createErrorMonitor(cpConfiguration);
				errorRates.putIfAbsent(host, errorMonitor);
				errorMonitor = errorRates.get(host);
			}

			boolean errorRateOk = errorMonitor.trackErrorRate(1);

			if (!errorRateOk) {
				Logger.error("Dyno recycling host connection pool for host: " + host + " due to too many errors");
				hostPool.markAsDown(null);
				pendingPools.put(host, hostPool);
			}
		}
	}
}
