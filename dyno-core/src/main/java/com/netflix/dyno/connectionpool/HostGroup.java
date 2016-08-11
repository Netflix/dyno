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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.netflix.dyno.connectionpool.exception.DynoConnectException;

/**
 * Class representing a group of hosts. This is useful for underlying connection pool implementations
 * where a single multiplexed connection can be used to talk to a group of hosts. 
 * e.g  spy memcached uses this approach where there is a single selector for a group of hosts. 
 * 
 * @author poberai
 *
 */
public class HostGroup extends Host {

	private final List<Host> hostList = new ArrayList<Host>();
	
	public HostGroup(String hostname, String ipAddress, int port) {
		super(hostname, ipAddress, port);
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
