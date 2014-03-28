package com.netflix.dyno.connectionpool;

import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.ThrottledException;



/**
 * Factory used to create and open new connections on a host.
 * 
 * @author poberai
 * 
 * @param <CL>
 */
public interface ConnectionFactory<CL> {
	
    public Connection<CL> createConnection(HostConnectionPool<CL> pool) throws DynoConnectException, ThrottledException;
}
