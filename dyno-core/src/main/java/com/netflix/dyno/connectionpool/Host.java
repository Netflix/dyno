package com.netflix.dyno.connectionpool;

import java.net.InetSocketAddress;

public class Host {

	private final String name;
	private final int port;
	private Status status = Status.Down;
	private InetSocketAddress socketAddress = null;
	
	private String dc; 
	
	public static enum Status {
		Up, Down;
	}
	
	public Host(String name, int port) {
		this(name, port, Status.Down);
	}
	
	public Host(String name, int port, Status status) {
		this.name = name;
		this.port = port;
		this.status = status;
		this.socketAddress = new InetSocketAddress(name, port);
	}

	public String getHostName() {
		return name;
	}
	
	public int getPort() {
		return port;
	}
	
	public String getDC() {
		return dc;
	}
	
	public Host setDC(String datacenter) {
		this.dc = datacenter;
		return this;
	}
	
	public Host setStatus(Status condition) {
		status = condition;
		return this;
	}
	
	public boolean isUp() {
		return status == Status.Up;
	}
	
	public InetSocketAddress getSocketAddress() {
		return socketAddress;
	}
	
	public static final Host NO_HOST = new Host("UNKNOWN", 0);

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((dc == null) ? 0 : dc.hashCode());
		result = prime * result + port;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		
		if (getClass() != obj.getClass()) return false;
		
		Host other = (Host) obj;
		boolean equals = true;
		
		equals &= (name != null) ? name.equals(other.name) : other.name == null;
		equals &= (dc != null) ? dc.equals(other.dc) : other.dc == null;
		equals &= (port == other.port);
		
		return equals;
	}

	@Override
	public String toString() {
		return "Host [name=" + name + ", port=" + port + ", dc: " + dc + ", status: " + status.name() + "]";
	}
	
	
}
