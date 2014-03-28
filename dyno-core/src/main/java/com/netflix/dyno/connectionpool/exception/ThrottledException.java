package com.netflix.dyno.connectionpool.exception;


public class ThrottledException extends DynoConnectException implements IsRetryableException {

	private static final long serialVersionUID = -9132799199614005261L;

	public ThrottledException(String message) {
        super(message);
    }

    public ThrottledException(Throwable t) {
        super(t);
    }

    public ThrottledException(String message, Throwable cause) {
        super(message, cause);
    }
}