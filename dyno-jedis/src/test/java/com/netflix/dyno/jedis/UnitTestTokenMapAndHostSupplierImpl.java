package com.netflix.dyno.jedis;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostBuilder;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import org.apache.commons.lang3.tuple.Pair;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;

public class UnitTestTokenMapAndHostSupplierImpl implements TokenMapSupplier, HostSupplier {
    private final Map<Host, HostToken> hostTokenMap = new HashMap<>();
    private final List<Pair<RedisServer, Integer>> redisServers = new ArrayList<>();

    public UnitTestTokenMapAndHostSupplierImpl(int serverCount, String rack) throws IOException {
        int hostTokenStride = Integer.MAX_VALUE / serverCount;

        for (int i = 0; i < serverCount; i++) {
            int port = findFreePort();
            RedisServer redisServer = new RedisServer(port);
            redisServer.start();
            redisServers.add(Pair.of(redisServer, port));

            Host host = new HostBuilder().setHostname("localhost").setPort(port).setRack(rack).setStatus(Host.Status.Up).createHost();
            hostTokenMap.put(host, new HostToken((long) i * hostTokenStride, host));
        }
    }

    private int findFreePort() {

        int port = 0;
        while (port == 0) {
            try {
                ServerSocket socket = new ServerSocket(0);
                port = socket.getLocalPort();
                socket.close();
            } catch (IOException e) {
                // find next port
            }
        }
        return port;
    }

    @Override
    public List<HostToken> getTokens(Set<Host> activeHosts) {
        return new ArrayList<>(hostTokenMap.values());
    }

    @Override
    public HostToken getTokenForHost(Host host, Set<Host> activeHosts) {
        return hostTokenMap.get(host);
    }

    @Override
    public List<Host> getHosts() {
        return new ArrayList<>(hostTokenMap.keySet());
    }

    public void shutdown() {
        redisServers.forEach(x -> x.getLeft().stop());
    }

    public void pauseServer(int idx) {
        redisServers.get(idx).getLeft().stop();
    }

    public void resumeServer(int idx) {
        redisServers.get(idx).getLeft().start();
    }
}

