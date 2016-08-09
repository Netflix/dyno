/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.connectionpool.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;

public class HostsUpdater {

	private final HostSupplier hostSupplier; 

	private final AtomicBoolean stop = new AtomicBoolean(false);
	private final AtomicReference<HostStatusTracker> hostTracker = new AtomicReference<HostStatusTracker>(null);
	
	public HostsUpdater(HostSupplier hSupplier) {
		this.hostSupplier = hSupplier;
		this.hostTracker.set(new HostStatusTracker());
	}
	
		
	public HostStatusTracker refreshHosts() {
		
		if (stop.get() || Thread.currentThread().isInterrupted()) {
			return null;
		}
		
		Collection<Host> allHosts = hostSupplier.getHosts();
		if (allHosts == null || allHosts.isEmpty()) {
			throw new NoAvailableHostsException("No available hosts when starting HostsUpdater");
		}
		
		List<Host> hostsUp = new ArrayList<Host>();
		List<Host> hostsDown = new ArrayList<Host>();
		
		for (Host host : allHosts) {
			if (host.isUp()) {
				hostsUp.add(host);
			} else {
				hostsDown.add(host);
			}
		}
		
		HostStatusTracker newTracker = hostTracker.get().computeNewHostStatus(hostsUp, hostsDown);
		hostTracker.set(newTracker);
		
		return hostTracker.get();
	}
	
	public void stop() {
		stop.set(true);
	}
}
