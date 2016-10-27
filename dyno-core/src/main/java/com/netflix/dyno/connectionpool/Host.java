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

import com.netflix.dyno.connectionpool.impl.utils.ConfigUtils;

/**
 * Class encapsulating information about a host.
 *
 * This is immutable except for the host status
 *
 * @author poberai
 * @author ipapapanagiotou
 *
 */

public class Host implements Comparable<Host> {
        public static final int DEFAULT_PORT = 8102; 
        public static final Host NO_HOST = new Host("UNKNOWN", "UNKNOWN", 0, "UNKNOWN");
    
	private final String hostname;
	private final String ipAddress;
	private final int port;
	private final InetSocketAddress socketAddress;
        private final String rack; 
        private final String datacenter;
	private Status status = Status.Down;

	

	
	public static enum Status {
		Up, Down;
	}
	
	public Host(String hostname, int port, String rack) {
		this(hostname, null, port, rack, rack, Status.Down);
	}
	
	public Host(String hostname, String rack, Status status) {
		this(hostname, null, DEFAULT_PORT, rack, rack, status);
	}
	
	public Host(String hostname, int port, String rack, Status status) {
		this(hostname, null, port, rack, rack, status);
	}
	
	public Host(String hostname, String ipAddress, int port, String rack) {
		this(hostname, ipAddress, port, rack, rack, Status.Down);
	}
	
	public Host(String hostname, String ipAddress, String rack, Status status) {
		this(hostname, ipAddress, DEFAULT_PORT, rack, rack, status);
	}

	public Host(String name, String ipAddress, int port, String rack, String datacenter,  Status status) {
		this.hostname = name;
		this.ipAddress = ipAddress;
		this.port = port;
		this.rack = rack;
		this.status = status;
		
		this.datacenter = ConfigUtils.getDataCenter(datacenter);
		
		// Used for the unit tests to prevent host name resolution
		if(port != -1) {
		    this.socketAddress = new InetSocketAddress(name, port);
		} else { 
		    this.socketAddress = null;
		}
		

	}

	public String getHostAddress() {
		if (this.ipAddress != null) {
			return ipAddress;
		}
		return hostname;
	}
	
	public String getHostName() {
            return hostname;
	}
	
	public String getIpAddress() {
		return ipAddress;
	}
	
	public int getPort() {
		return port;
	}
	

	public String getRack() {
		return rack;
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
	


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((hostname == null) ? 0 : hostname.hashCode());
		result = prime * result + ((rack == null) ? 0 : rack.hashCode());
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
		
		equals &= (hostname != null) ? hostname.equals(other.hostname) : other.hostname == null;
		equals &= (rack != null) ? rack.equals(other.rack) : other.rack == null;
		equals &= port == other.port;
		
		return equals;
	}


	@Override
	public int compareTo(Host o) {
	    int compared = this.hostname.compareTo(o.hostname);
	    if( compared != 0) {
		return compared;
	    }
	    compared = this.rack.compareTo(o.hostname);
	    if( compared != 0) {
		return compared;
	    }
	    return Integer.compare(this.port,o.port);
	}

        public String getDatacenter() {
    	return datacenter;
        }
    
     
    
        @Override
        public String toString() {
    	return "Host [hostname=" + hostname + ", ipAddress=" + ipAddress + ", port=" + port + ", rack: " + rack
    		+ ", datacenter: " + datacenter + ", status: " + status.name() + "]";
        }
}
