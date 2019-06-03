package com.netflix.dyno.connectionpool;

import com.netflix.dyno.connectionpool.impl.utils.ConfigUtils;

import static com.netflix.dyno.connectionpool.Host.DEFAULT_DATASTORE_PORT;
import static com.netflix.dyno.connectionpool.Host.DEFAULT_PORT;

public class HostBuilder {
    private String hostname;
    private int port = DEFAULT_PORT;
    private String rack;
    private String ipAddress = null;
    private int securePort = DEFAULT_PORT;
    private int datastorePort = DEFAULT_DATASTORE_PORT;
    private String datacenter = null;
    private Host.Status status = Host.Status.Down;
    private String hashtag = null;
    private String password = null;

    public HostBuilder setPort(int port) {
        this.port = port;
        return this;
    }

    public HostBuilder setRack(String rack) {
        this.rack = rack;
        return this;
    }

    public HostBuilder setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public HostBuilder setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
        return this;
    }

    public HostBuilder setSecurePort(int securePort) {
        this.securePort = securePort;
        return this;
    }

    public HostBuilder setDatacenter(String datacenter) {
        this.datacenter = datacenter;
        return this;
    }

    public HostBuilder setStatus(Host.Status status) {
        this.status = status;
        return this;
    }

    public HostBuilder setHashtag(String hashtag) {
        this.hashtag = hashtag;
        return this;
    }

    public HostBuilder setPassword(String password) {
        this.password = password;
        return this;
    }

    public HostBuilder setDatastorePort(int datastorePort) {
        this.datastorePort = datastorePort;
        return this;
    }

    public Host createHost() {
        if (datacenter == null) {
            datacenter = ConfigUtils.getDataCenterFromRack(rack);
        }
        return new Host(hostname, ipAddress, port, securePort, datastorePort, rack, datacenter, status, hashtag, password);
    }
}
