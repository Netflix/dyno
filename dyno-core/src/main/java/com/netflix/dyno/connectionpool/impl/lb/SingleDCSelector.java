package com.netflix.dyno.connectionpool.impl.lb;

import java.util.List;

import com.netflix.dyno.connectionpool.Host;

public interface SingleDCSelector { 

	public void init(String zone, List<Host> hosts);

	public Host getHostForKey(String key);

	public boolean isEmpty();

	public void addHost(Host host);

	public void removeHost(Host host);


	public static interface SingleDCSelectorFactory {
		
		public SingleDCSelector vendSelector(); 
	}

}
