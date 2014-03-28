package com.netflix.dyno.connectionpool.exception;


public class TimeoutException extends DynoConnectException implements IsRetryableException, IsDeadConnectionException {

	private static final long serialVersionUID = 5025308550262085866L;

	public TimeoutException(String message) {
		super(message);
	}

	public TimeoutException(Throwable t) {
		super(t);
	}

	public TimeoutException(String message, Throwable cause) {
		super(message, cause);
	}
}