package com.netflix.dyno.connectionpool.impl.lb;

import com.netflix.dyno.connectionpool.Host;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HostUtils {


    private static final Logger logger = LoggerFactory.getLogger(HostSelectionWithFallback.class);

    /**
     * Calculate replication factor from the given list of hosts
     *
     * @param allHostTokens
     * @param localRack
     * @return replicationFactor
     */
    public static int calculateReplicationFactor(List<HostToken> allHostTokens, String localRack) {
        return calculateReplicationFactorForDC(allHostTokens, null, localRack);
    }

    /**
     * Calculate replication factor for a datacenter.
     * If datacenter is null we use one of the hosts from the list and use its DC.
     *
     * @param allHostTokens
     * @param dataCenter
     * @param localRack
     * @return replicationFactor for the dataCenter
     */
    public static int calculateReplicationFactorForDC(List<HostToken> allHostTokens, String dataCenter, String localRack) {
        Map<Long, Integer> groups = new HashMap<>();

        Set<HostToken> uniqueHostTokens = new HashSet<>(allHostTokens);
        if (dataCenter == null) {
            if (localRack != null) {
                dataCenter = localRack.substring(0, localRack.length() - 1);
            } else {
                // No DC specified. Get the DC from the first host and use its replication factor
                Host host = allHostTokens.get(0).getHost();
                String curRack = host.getRack();
                dataCenter = curRack.substring(0, curRack.length() - 1);
            }
        }

        for (HostToken hostToken : uniqueHostTokens) {
            if (hostToken.getHost().getRack().contains(dataCenter)) {
                Long token = hostToken.getToken();
                if (groups.containsKey(token)) {
                    int current = groups.get(token);
                    groups.put(token, current + 1);
                } else {
                    groups.put(token, 1);
                }
            }
        }

        Set<Integer> uniqueCounts = new HashSet<>(groups.values());

        if (uniqueCounts.size() > 1) {
            throw new RuntimeException("Invalid configuration - replication factor cannot be asymmetric");
        }

        int rf = uniqueCounts.toArray(new Integer[uniqueCounts.size()])[0];

        if (rf > 3) {
            logger.warn("Replication Factor is high: " + uniqueHostTokens);
        }

        return rf;

    }
}
