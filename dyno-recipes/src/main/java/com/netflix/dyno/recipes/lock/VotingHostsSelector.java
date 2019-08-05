package com.netflix.dyno.recipes.lock;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.impl.lb.CircularList;

public interface VotingHostsSelector {
    /**
     * Get the list of hosts eligible for voting
     * @return
     */
    CircularList<Host> getVotingHosts();

    /**
     * Returns the number of voting hosts
     * @return
     */
    int getVotingSize();
}
