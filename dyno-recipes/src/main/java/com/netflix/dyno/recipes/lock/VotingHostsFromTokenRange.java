package com.netflix.dyno.recipes.lock;

import com.google.common.collect.ImmutableSet;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.lb.CircularList;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * This class deterministically returns a list of hosts which will be used for voting. We use the TokenRange to get the
 * same set of hosts from all clients.
 */
public class VotingHostsFromTokenRange implements VotingHostsSelector {

    private final TokenMapSupplier tokenMapSupplier;
    private final HostSupplier hostSupplier;
    private final CircularList<Host> votingHosts = new CircularList<>(new ArrayList<>());
    private final int MIN_VOTING_SIZE = 1;
    private final int MAX_VOTING_SIZE = 5;
    private final int effectiveVotingSize;
    private final AtomicInteger calculatedVotingSize = new AtomicInteger(0);

    public VotingHostsFromTokenRange(HostSupplier hostSupplier, TokenMapSupplier tokenMapSupplier, int votingSize) {
        this.tokenMapSupplier = tokenMapSupplier;
        this.hostSupplier = hostSupplier;
        effectiveVotingSize = votingSize == -1 ? MAX_VOTING_SIZE : votingSize;
        if(votingSize % 2 == 0) {
            throw new IllegalStateException("Cannot perform voting with even number of hosts");
        }
        getVotingHosts();
    }

    @Override
    public CircularList<Host> getVotingHosts() {
        if (votingHosts.getSize() == 0) {
            if(effectiveVotingSize % 2 == 0) {
                throw new IllegalStateException("Cannot do voting with even number of nodes for voting");
            }
            List<HostToken> allHostTokens = tokenMapSupplier.getTokens(ImmutableSet.copyOf(hostSupplier.getHosts()));
            if (allHostTokens.size() < MIN_VOTING_SIZE) {
                throw new IllegalStateException(String.format("Cannot perform voting with less than %d nodes", MIN_VOTING_SIZE));
            }
            // Total number of hosts present per rack
            Map<String, Long> numHostsPerRack = allHostTokens.stream().map(ht -> ht.getHost().getRack()).collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
            AtomicInteger numHostsRequired = new AtomicInteger(effectiveVotingSize);
            // Map to keep track of number of hosts to take for voting from this rack
            Map<String, Integer> numHosts = new HashMap<>();
            // Sort racks to get the same order
            List<String> racks = numHostsPerRack.keySet().stream().sorted(Comparator.comparing(String::toString)).collect(Collectors.toList());
            for(String rack: racks) {
                // Take as many hosts as you can from this rack.
                int v = (int) Math.min(numHostsRequired.get(), numHostsPerRack.get(rack));
                numHostsRequired.addAndGet(-v);
                numHosts.put(rack, v);
                calculatedVotingSize.addAndGet(v);
            }
            if(calculatedVotingSize.get() % 2 == 0) {
                throw new IllegalStateException("Could not construct voting pool. Min number of hosts not met!");
            }
            Map<String, List<HostToken>> rackToHostToken = allHostTokens.stream()
                    .collect(Collectors.groupingBy(ht -> ht.getHost().getRack()));
            // Get the final list of voting hosts
            List<Host> finalVotingHosts = rackToHostToken.entrySet().stream()
                    // Sorting on token to get hosts deterministically.
                    .sorted(Comparator.comparing(Map.Entry::getKey))
                    .flatMap(e -> {
                        List<HostToken> temp = e.getValue();
                        temp.sort(HostToken::compareTo);
                        return temp.subList(0, numHosts.get(e.getKey())).stream();
                    })
                    .map(ht -> ht.getHost())
                    .collect(Collectors.toList());
            votingHosts.swapWithList(finalVotingHosts);
        }
        return votingHosts;
    }

    @Override
    public int getVotingSize() {
        return calculatedVotingSize.get();
    }
}
