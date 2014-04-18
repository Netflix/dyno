package com.netflix.dyno.connectionpool.exception;

public class NoAvailableHostsException extends DynoConnectException {

	public NoAvailableHostsException(String message) {
		super(message);
	}

}
