package com.netflix.dyno.recipes.lock;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostBuilder;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.lb.CircularList;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class VotingHostsFromTokenRangeTest {

    private String r1 = "rack1";
    private String r2 = "rack2";
    private String r3 = "rack3";
    private TokenMapSupplier tokenMapSupplier;
    private HostSupplier hostSupplier;
    private VotingHostsSelector votingHostsSelector;
    private List<Host> hosts;

    @Before
    public void setUp() {
        Host h1 = new HostBuilder().setHostname("h1").setRack(r1).setStatus(Host.Status.Up).createHost();
        Host h2 = new HostBuilder().setHostname("h2").setRack(r1).setStatus(Host.Status.Up).createHost();
        Host h3 = new HostBuilder().setHostname("h3").setRack(r2).setStatus(Host.Status.Up).createHost();
        Host h4 = new HostBuilder().setHostname("h4").setRack(r2).setStatus(Host.Status.Up).createHost();
        Host h5 = new HostBuilder().setHostname("h5").setRack(r2).setStatus(Host.Status.Up).createHost();
        Host h6 = new HostBuilder().setHostname("h6").setRack(r3).setStatus(Host.Status.Up).createHost();

        Host[] arr = {h1, h2, h3, h4, h5, h6};
        hosts = Arrays.asList(arr);
        final Map<Host, HostToken> tokenMap = new HashMap<>();

        tokenMap.put(h1, new HostToken(1111L, h1));
        tokenMap.put(h2, new HostToken(2222L, h2));
        tokenMap.put(h3, new HostToken(1111L, h3));
        tokenMap.put(h4, new HostToken(2222L, h4));
        tokenMap.put(h5, new HostToken(3333L, h5));
        tokenMap.put(h6, new HostToken(1111L, h6));
        hostSupplier = () -> hosts;
        tokenMapSupplier = new TokenMapSupplier() {
            @Override
            public List<HostToken> getTokens(Set<Host> activeHosts) {
                return new ArrayList<>(tokenMap.values());
            }

            @Override
            public HostToken getTokenForHost(Host host, Set<Host> activeHosts) {
                return tokenMap.get(host);
            }
        };
    }

    private void testVotingSize(int votingSize) {
        votingHostsSelector = new VotingHostsFromTokenRange(hostSupplier, tokenMapSupplier, votingSize);
        CircularList<Host> hosts = votingHostsSelector.getVotingHosts();
        Set<String> resultHosts = hosts.getEntireList().stream().map(h -> h.getHostName()).collect(Collectors.toSet());
        Assert.assertEquals(votingSize, resultHosts.size());
        Assert.assertEquals(votingSize,
                hosts.getEntireList().subList(0, votingSize).stream().filter(h1 -> resultHosts.contains(h1.getHostName())).count());
    }

    @Test
    public void getVotingSize() {
        IntStream.range(1, 6).filter(i -> i%2 != 0).forEach(i -> testVotingSize(i));
    }
}