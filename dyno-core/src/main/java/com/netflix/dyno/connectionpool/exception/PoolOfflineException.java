package com.netflix.dyno.connectionpool.exception;

import com.netflix.dyno.connectionpool.Host;

/**
 * Indicates the pool is likely not active when accessed
 */
public class PoolOfflineException extends DynoConnectException {

	private static final long serialVersionUID = -345340994112630363L;

	public PoolOfflineException(Host host, String message) {
		super(message);
	}
}