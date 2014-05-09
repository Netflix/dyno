package com.netflix.dyno.connectionpool.exception;

public class FatalConnectionException extends DynoConnectException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 953422826144906928L;

	public FatalConnectionException(String message) {
		super(message);
	}

	public FatalConnectionException(Throwable t) {
		super(t);
	}

	public FatalConnectionException(String message, Throwable cause) {
		super(message, cause);
	}
}
