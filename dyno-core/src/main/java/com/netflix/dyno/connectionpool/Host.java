/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.dyno.connectionpool;

import java.net.InetSocketAddress;

/**
 * Class encapsulating information about a host.
 * @author poberai
 *
 */
public class Host {

	private final String name;
	private int port;
	private Status status = Status.Down;
	private InetSocketAddress socketAddress = null;
	
	private String rack; 
	
	public static enum Status {
		Up, Down;
	}
	
	public Host(String name, int port) {
		this(name, port, Status.Down);
	}
	
	public Host(String name, Status status) {
		this.name = name;
		this.port = -1;
		this.status = status;
	}

	public Host(String name, int port, Status status) {
		this.name = name;
		this.port = port;
		this.status = status;
		if (port != -1) {
			this.socketAddress = new InetSocketAddress(name, port);
		}
	}

	public String getHostName() {
		return name;
	}
	
	public int getPort() {
		return port;
	}
	
	public Host setPort(int p) {
		this.port = p;
		this.socketAddress = new InetSocketAddress(name, port);
		return this;
	}

	public Status getStatus() {
		return status;
	}

	public String getRack() {
		return rack;
	}
	
	public Host setRack(String rack) {
		this.rack = rack;
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
		result = prime * result + ((rack == null) ? 0 : rack.hashCode());
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
		equals &= (rack != null) ? rack.equals(other.rack) : other.rack == null;
		
		return equals;
	}

	@Override
	public String toString() {
		return "Host [name=" + name + ", port=" + port + ", rack: " + rack + ", status: " + status.name() + "]";
	}
}
