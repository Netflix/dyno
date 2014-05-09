package com.netflix.dyno.connectionpool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.netflix.dyno.connectionpool.exception.DynoConnectException;

public class HostGroup extends Host {

	private final List<Host> hostList = new ArrayList<Host>();
	
	public HostGroup(String name, int port) {
		super(name, port);
	}

	public void add(Collection<Host> hosts) {
		for (Host host : hosts) {
			if (!host.isUp()) {
				throw new DynoConnectException("Cannot add host that is DOWN");
			}
			hostList.add(host);
		}
	}
	
	public List<Host> getHostList() {
		return hostList;
	}
	
}
