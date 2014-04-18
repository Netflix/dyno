package com.netflix.dyno.connectionpool.exception;

public class PoolExhaustedException extends DynoConnectException { 

	/**
	 * 
	 */
	private static final long serialVersionUID = 9081993527008721028L;

	public PoolExhaustedException(String message) {
		super(message);
	}

	public PoolExhaustedException(Throwable t) {
		super(t);
	}

	public PoolExhaustedException(String message, Throwable cause) {
		super(message, cause);
	}
}

