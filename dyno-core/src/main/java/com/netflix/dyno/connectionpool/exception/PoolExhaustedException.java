package com.netflix.dyno.connectionpool.exception;

import com.netflix.dyno.connectionpool.HostConnectionPool;

/**
 * Indicates there are no connections left in the host connection pool.
 */
public class PoolExhaustedException extends DynoConnectException {

    private final HostConnectionPool hcp;

	private static final long serialVersionUID = 9081993527008721028L;


	public PoolExhaustedException(HostConnectionPool hostConnectionPool, String message) {
		super(message);
        this.hcp = hostConnectionPool;
	}

    public PoolExhaustedException(HostConnectionPool hostConnectionPool, String message, Throwable cause) {
        super(message, cause);
        this.hcp = hostConnectionPool;
    }

    public PoolExhaustedException(HostConnectionPool hostConnectionPool, Throwable t) {
        super(t);
        hcp = hostConnectionPool;
    }

    public HostConnectionPool getHostConnectionPool() {
        return hcp;
    }
}

