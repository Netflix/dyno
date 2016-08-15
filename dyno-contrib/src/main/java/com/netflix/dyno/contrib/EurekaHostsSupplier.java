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
package com.netflix.dyno.contrib;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.netflix.appinfo.AmazonInfo;
import com.netflix.appinfo.AmazonInfo.MetaDataKey;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.appinfo.InstanceInfo.InstanceStatus;
import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.DiscoveryManager;
import com.netflix.discovery.shared.Application;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;

/**
 * Simple class that implements {@link Supplier}<{@link List}<{@link Host}>>. It provides a List<{@link Host}>
 * using the {@link DiscoveryManager} which is the eureka client. 
 * 
 * Note that the class needs the eureka application name to discover all instances for that application.
 * 
 * @author poberai
 */
public class EurekaHostsSupplier implements HostSupplier {

	private static final Logger Logger = LoggerFactory.getLogger(EurekaHostsSupplier.class);

	// The Dynomite cluster name for discovering nodes
	private final String applicationName;
	private final DiscoveryClient discoveryClient;
	
	public EurekaHostsSupplier(String applicationName, DiscoveryClient dClient) {
		this.applicationName = applicationName.toUpperCase();
		this.discoveryClient = dClient;
	}

	@Override
	public List<Host> getHosts() {
		return getUpdateFromEureka();
	}
	
	private List<Host> getUpdateFromEureka() {
		
		if (discoveryClient == null) {
			Logger.error("Discovery client cannot be null");
			throw new RuntimeException("EurekaHostsSupplier needs a non-null DiscoveryClient");
		}

		Logger.info("Dyno fetching instance list for app: " + applicationName);
		
		Application app = discoveryClient.getApplication(applicationName);
		List<Host> hosts = new ArrayList<Host>();

		if (app == null) {
			return hosts;
		}

		List<InstanceInfo> ins = app.getInstances();

		if (ins == null || ins.isEmpty()) {
			return hosts;
		}

		hosts = Lists.newArrayList(Collections2.transform(ins,
				
				new Function<InstanceInfo, Host>() {
					@Override
					public Host apply(InstanceInfo info) {
						
						Host.Status status = info.getStatus() == InstanceStatus.UP ? Host.Status.Up : Host.Status.Down;
						
						String rack = null;
						try {
							if (info.getDataCenterInfo() instanceof AmazonInfo) {
								AmazonInfo amazonInfo = (AmazonInfo)info.getDataCenterInfo();
								rack = amazonInfo.get(MetaDataKey.availabilityZone);
							}
						}
						catch (Throwable t) {
						    Logger.error("Error getting rack for host " + info.getHostName(), t);
						}
						if(rack == null) {
						    Logger.error("Rack wasn't found for host:" + info.getHostName() + " there may be issues matching it up to the token map");
						}
						Host host = new Host(info.getHostName(), info.getIPAddr(), rack, status);
						return host;
					}
				}));
		
		Logger.info("Dyno found hosts from eureka - num hosts: " + hosts.size());
		
		return hosts;
	}

	@Override
	public String toString() {
		return EurekaHostsSupplier.class.getName();
	}

}
