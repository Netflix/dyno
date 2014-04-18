package com.netflix.dyno.connectionpool;

public class Host {

	private final String name;
	private final int port;
	
	public Host(String name, int port) {
		this.name = name;
		this.port = port;
	}
	
	public String getHostName() {
		return name;
	}
	
	public int getPort() {
		return port;
	}
	
	public static final Host NO_HOST = new Host("UNKNOWN", 0);

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
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
		
		equals &= (name != null) ? other.name.equals(name) : other.name == null;
		equals &= (port == other.port);
		
		return equals;
	}

	@Override
	public String toString() {
		return "Host [name=" + name + ", port=" + port + "]";
	}
	
	
}
