package com.netflix.dyno.connectionpool;

import com.netflix.dyno.connectionpool.exception.DynoException;

/**
 * Base interface for classes that track error rates for the connection pool.
 *
 * @param <CL> the client type
 */
public interface HealthTracker<CL> {

    void trackConnectionError(HostConnectionPool<CL> hostPool, DynoException e);

}

