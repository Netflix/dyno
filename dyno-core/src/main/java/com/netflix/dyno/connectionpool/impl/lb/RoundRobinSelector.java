package com.netflix.dyno.connectionpool.impl.lb;

import java.util.ArrayList;
import java.util.List;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.impl.lb.SelectionWIthRemoteZoneFallback.SingleDCSelector;

public class RoundRobinSelector implements SingleDCSelector {

	private String zone = null;
	private final CircularList<Host> circularList = new CircularList<Host>(null);

	@Override
	public void init(String zoneName, List<Host> hosts) {
		zone = zoneName;
		circularList.swapWithList(hosts);
	}

	@Override
	public Host getHostForKey(String key) {
		return circularList.getNextElement();
	}

	@Override
	public boolean isEmpty() {
		return circularList.getEntireList().size() == 0;
	}

	@Override
	public void addHost(Host host) {
		List<Host> newHostList = new ArrayList<Host>(circularList.getEntireList());
		newHostList.add(host);
		circularList.swapWithList(newHostList);
	}

	@Override
	public void removeHost(Host host) {
		List<Host> newHostList = new ArrayList<Host>(circularList.getEntireList());
		newHostList.remove(host);
		circularList.swapWithList(newHostList);
	}
	
	public String toString() {
		return "RoundRobinSelector: zone: " + zone + ", list: " + circularList.toString();
	}

}
