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
package com.netflix.dyno.connectionpool.impl;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HostsUpdater {

    private static final Logger Logger = LoggerFactory.getLogger(ConnectionPoolImpl.class);

    private final HostSupplier hostSupplier;
    private final TokenMapSupplier tokenMapSupplier;

    private final AtomicBoolean stop = new AtomicBoolean(false);
    private final AtomicReference<HostStatusTracker> hostTracker = new AtomicReference<HostStatusTracker>(null);

    public HostsUpdater(HostSupplier hSupplier, TokenMapSupplier tokenMapSupplier) {
        this.hostSupplier = hSupplier;
        this.tokenMapSupplier = tokenMapSupplier;
        this.hostTracker.set(new HostStatusTracker());
    }


    public HostStatusTracker refreshHosts() {

        if (stop.get() || Thread.currentThread().isInterrupted()) {
            return null;
        }

        List<Host> allHostsFromHostSupplier = hostSupplier.getHosts();
        if (allHostsFromHostSupplier == null || allHostsFromHostSupplier.isEmpty()) {
            throw new NoAvailableHostsException("No available hosts when starting HostsUpdater");
        }

        List<Host> hostsUpFromHostSupplier = new ArrayList<>();
        List<Host> hostsDownFromHostSupplier = new ArrayList<>();

        for (Host host : allHostsFromHostSupplier) {
            if (host.isUp()) {
                hostsUpFromHostSupplier.add(host);
            } else {
                hostsDownFromHostSupplier.add(host);
            }
        }

        // if nothing has changed, just return the earlier hosttracker.
        if (!hostTracker.get().checkIfChanged(new HashSet<>(hostsUpFromHostSupplier), new HashSet<>(hostsDownFromHostSupplier))) {
            return hostTracker.get();
        }

        /**
         * HostTracker should return the hosts that we get from TokenMapSupplier.
         * Hence get the hosts from HostSupplier and map them to TokenMapSupplier
         * and return them.
         */
        Collections.sort(allHostsFromHostSupplier);
        Set<Host> hostSet = new HashSet<>(allHostsFromHostSupplier);
        // Create a list of host/Tokens
        List<HostToken> hostTokens;
        if (tokenMapSupplier != null) {
            Logger.info("Getting Hosts from TokenMapSupplier");
            hostTokens = tokenMapSupplier.getTokens(hostSet);

            if (hostTokens.isEmpty()) {
                throw new DynoException("No hosts in the TokenMapSupplier");
            }
        } else {
            throw new DynoException("TokenMapSupplier not provided");
        }

        // The key here really needs to be a object that is overlapping between
        // the host from HostSupplier and TokenMapSupplier. Since that is a
        // subset of the Host object itself, Host is the key as well as value here.
        Map<Host, Host> allHostSetFromTokenMapSupplier = new HashMap<>();
        for (HostToken ht : hostTokens) {
            allHostSetFromTokenMapSupplier.put(ht.getHost(), ht.getHost());
        }

        hostsUpFromHostSupplier.clear();
        hostsDownFromHostSupplier.clear();

        for (Host hostFromHostSupplier : allHostsFromHostSupplier) {
            if (hostFromHostSupplier.isUp()) {
                Host hostFromTokenMapSupplier = allHostSetFromTokenMapSupplier.get(hostFromHostSupplier);
                if (hostFromTokenMapSupplier == null) {
                    throw new DynoException("Could not find " + hostFromHostSupplier.getHostName() + " in token map supplier.");
                }

                hostsUpFromHostSupplier.add(new Host(hostFromHostSupplier.getHostName(), hostFromHostSupplier.getIpAddress(),
                        hostFromTokenMapSupplier.getPort(), hostFromTokenMapSupplier.getSecurePort(), hostFromTokenMapSupplier.getRack(),
                        hostFromTokenMapSupplier.getDatacenter(), Host.Status.Up, hostFromTokenMapSupplier.getHashtag(),
                        hostFromTokenMapSupplier.getPassword()));
                allHostSetFromTokenMapSupplier.remove(hostFromTokenMapSupplier);
            } else {
                Host hostFromTokenMapSupplier = allHostSetFromTokenMapSupplier.get(hostFromHostSupplier);

                hostsDownFromHostSupplier.add(new Host(hostFromHostSupplier.getHostName(), hostFromHostSupplier.getIpAddress(),
                        hostFromTokenMapSupplier.getPort(), hostFromTokenMapSupplier.getSecurePort(), hostFromTokenMapSupplier.getRack(),
                        hostFromTokenMapSupplier.getDatacenter(), Host.Status.Down, hostFromTokenMapSupplier.getHashtag(),
                        hostFromTokenMapSupplier.getPassword()));
                allHostSetFromTokenMapSupplier.remove(hostFromTokenMapSupplier);
            }
        }

        // if a node is down, it might be absent in hostSupplier but has its presence in TokenMapSupplier.
        // Add that host to the down list here.
        for (Host h : allHostSetFromTokenMapSupplier.keySet()) {
            hostsDownFromHostSupplier.add(new Host(h.getHostName(), h.getIpAddress(),
                    h.getPort(), h.getSecurePort(), h.getRack(),
                    h.getDatacenter(), Host.Status.Down, h.getHashtag()));

        }

        HostStatusTracker newTracker = hostTracker.get().computeNewHostStatus(hostsUpFromHostSupplier, hostsDownFromHostSupplier);
        hostTracker.set(newTracker);

        return hostTracker.get();
    }

    public void stop() {
        stop.set(true);
    }
}
