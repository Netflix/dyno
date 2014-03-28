package com.netflix.dyno.connectionpool;

public class Host {

	private final String name;
	private final int port;
	
	public Host(String name, int port) {
		this.name = name;
		this.port = port;
	}
	
	public static final Host NO_HOST = new Host("UNKNOWN", 0);
}
