package com.netflix.dyno.connectionpool.impl;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;

public interface HostConnectionPoolFactory<CL> {

	HostConnectionPool<CL> createHostConnectionPool(Host host, ConnectionPoolImpl<CL> parentPoolImpl);

	enum Type {
        /** Asynchronous, non-blocking instance */
        Async,

        /** Synchronous, blocking instance */
        Sync;
    }
}
