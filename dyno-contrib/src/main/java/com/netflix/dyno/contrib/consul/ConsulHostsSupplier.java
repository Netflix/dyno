/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.contrib.consul;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.health.model.Check;
import com.ecwid.consul.v1.health.model.HealthService;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.netflix.discovery.DiscoveryManager;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;

/**
 * Simple class that implements {@link Supplier}<{@link List}<{@link Host}>>. It provides a List<{@link Host}>
 * using the {@link DiscoveryManager} which is the consul client. 
 *
 * Note that the class needs the consul application name to discover all instances for that application.
 *
 * Example of register at consul
 *
 curl -X PUT http://localhost:8500/v1/agent/service/register -d "{
 \"ID\": \"dynomite-8102\",
 \"Name\": \"dynomite\",
 \"Tags\": [\"datacenter=dc\",\"cloud=openstack\",\"rack=dc-rack\"],
 \"Address\": \"127.0.0.2\",
 \"Port\": 8102,
 \"Check\": {
 \"Interval\": \"10s\",
 \"HTTP\": \"http://127.0.0.1:22222/ping\"
 }}"
 *
 * @author tiodollar
 */
public class ConsulHostsSupplier implements HostSupplier {

    private static final Logger Logger = LoggerFactory.getLogger(ConsulHostsSupplier.class);

    // The Dynomite cluster name for discovering nodes
    private final String applicationName;
    private final ConsulClient discoveryClient;

    public ConsulHostsSupplier(String applicationName, ConsulClient discoveryClient) {
        this.applicationName = applicationName;
        this.discoveryClient = discoveryClient;
    }

    public static ConsulHostsSupplier newInstance(String applicationName, ConsulHostsSupplier hostsSupplier) {
        return new ConsulHostsSupplier(applicationName, hostsSupplier.getDiscoveryClient());
    }

    @Override
    public List<Host> getHosts() {
        return getUpdateFromConsul();
    }

    private List<Host> getUpdateFromConsul() {

        if (discoveryClient == null) {
            Logger.error("Discovery client cannot be null");
            throw new RuntimeException("ConsulHostsSupplier needs a non-null DiscoveryClient");
        }

        Logger.info("Dyno fetching instance list for app: " + applicationName);

        Response<List<HealthService>> services = discoveryClient.getHealthServices(applicationName, false, QueryParams.DEFAULT);

        List<HealthService> app = services.getValue();
        List<Host> hosts = new ArrayList<Host>();

        if (app != null && app.size() < 0) {
            return hosts;
        }

        hosts = Lists.newArrayList(Collections2.transform(app,

                new Function<HealthService, Host>() {
                    @Override
                    public Host apply(HealthService info) {

                        String hostName = ConsulHelper.findHost(info);
                        Map<String, String> metaData = ConsulHelper.getMetadata(info);

                        Host.Status status = Host.Status.Up;
                        for (com.ecwid.consul.v1.health.model.Check check : info.getChecks()) {
                            if (check.getStatus() == Check.CheckStatus.CRITICAL) {
                                status = Host.Status.Down;
                                break;
                            }
                        }

                        String rack = null;
                        try {
                            if (metaData.containsKey("cloud") && StringUtils.equals(metaData.get("cloud"), "aws")) {
                                rack = metaData.get("availability-zone");

                            } else {
                                rack = metaData.get("rack");
                            }
                        } catch (Throwable t) {
                            Logger.error("Error getting rack for host " + info.getNode(), t);
                        }
                        if (rack == null) {
                            Logger.error("Rack wasn't found for host:" + info.getNode()
                                    + " there may be issues matching it up to the token map");
                        }
                        Host host = new Host(hostName, hostName, info.getService().getPort(), rack,
                                String.valueOf(metaData.get("datacenter")), status);
                        return host;
                    }
                }));

        Logger.info("Dyno found hosts from consul - num hosts: " + hosts.size());

        return hosts;
    }

    @Override
    public String toString() {
        return ConsulHostsSupplier.class.getName();
    }

    public String getApplicationName() {
        return applicationName;
    }

    public ConsulClient getDiscoveryClient() {
        return discoveryClient;
    }

}