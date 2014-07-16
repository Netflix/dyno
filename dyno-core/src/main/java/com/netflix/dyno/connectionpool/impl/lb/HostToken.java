package com.netflix.dyno.connectionpool.impl.lb;

import com.netflix.dyno.connectionpool.Host;

public class HostToken implements Comparable<Long> {

	private final Long token;
	private final Host host;

	public HostToken(Long token, Host host) {
		this.token = token;
		this.host = host;
	}

	public Long getToken() {
		return token;
	}

	public Host getHost() {
		return host;
	}

	@Override
	public String toString() {
		return "HostToken [token=" + token + ", host=" + host + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + ((token == null) ? 0 : token.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		
		HostToken other = (HostToken) obj;
		boolean equals = true;
		equals &= (token != null) ? (token.equals(other.token)) : (other.token == null);
		equals &= (host != null) ? (host.equals(other.host)) : (other.host == null);
		return equals;
	}

	@Override
	public int compareTo(Long o) {
		return this.token.compareTo(o);
	}
}
