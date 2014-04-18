package com.netflix.dyno.connectionpool.exception;

public class InterruptedOperationException extends DynoConnectException {

	private static final long serialVersionUID = 4088251485336355007L;

	public InterruptedOperationException(String message) {
		super(message);
	}

	public InterruptedOperationException(Throwable t) {
		super(t);
	}

	public InterruptedOperationException(String message, Throwable cause) {
		super(message, cause);
	}
}
