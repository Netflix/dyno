package com.netflix.dyno.connectionpool.exception;

public class NoAvailableHostsException extends DynoConnectException {


	private static final long serialVersionUID = -6345231310492496030L;

	public NoAvailableHostsException(String message) {
		super(message);
	}

}
