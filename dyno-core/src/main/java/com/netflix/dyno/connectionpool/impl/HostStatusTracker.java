/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.dyno.connectionpool.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.HostSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class that can be used in conjunction with a {@link HostSupplier} repeatedly to understand whether the change within the 
 * active and inactive host set.
 * <p>
 * Implementations of {@link ConnectionPool} can then use this utility to adapt to topology changes and hence manage the corresponding 
 * {@link HostConnectionPool} objects for the set of active hosts. 
 * <p>
 * Note the behavior of this class is such that if a host <em>disappears</em> and it was last known as <em>active</em>
 * then it will be moved to <em>inactive</em>. If however it disappears and its last known state was <em>inactive</em>
 * then it is removed from tracking altogether. This is to support terminations/node replacements.
 *
 * @author poberai
 *
 */
public class HostStatusTracker {

    private static final Logger logger = LoggerFactory.getLogger(HostStatusTracker.class);
	
	// the set of active and inactive hosts
	private final Set<Host> activeHosts = new HashSet<Host>();
	private final Set<Host> inactiveHosts = new HashSet<Host>();
	
	public HostStatusTracker() {
	}
	
	public HostStatusTracker(Collection<Host> up, Collection<Host> down) {

		verifyMutuallyExclusive(up, down);
		
		activeHosts.addAll(up);
		inactiveHosts.addAll(down);
	}

	/**
	 * Helper method to check that there is no overlap b/w hosts up and down. 
	 * @param A
	 * @param B
	 */
	private void verifyMutuallyExclusive(Collection<Host> A, Collection<Host> B) {
		
		Set<Host> left = new HashSet<Host>(A);
		Set<Host> right = new HashSet<Host>(B);
		
		boolean modified = left.removeAll(right);
		if (modified) {
			throw new RuntimeException("Host up and down sets are not mutually exclusive!");
		}
	}

	/**
	 *  All we need to check here is that whether the new active set is not exactly the same as the 
	 *  prev active set. If there are any new hosts that have been added or any hosts that are missing
	 *  then return 'true' indicating that the active set has changed. 
	 *  
	 * @param hostsUp
	 * @return true/false indicating whether the active set has changed from the previous set. 
	 */
	public boolean activeSetChanged(Collection<Host> hostsUp) {
		
		return !hostsUp.equals(activeHosts);
	}
	
	/**
	 * This check is more involved than the active set check. Here we 2 conditions to check for
	 * 
	 *   1. We could have new hosts that were in the active set and have shown up in the inactive set. 
	 *   2. We can also have the case where hosts from the active set have disappeared and also not in the provided inactive set. 
	 *      This is where we have simply forgotten about some active host and that it needs to be shutdown
	 * 
	 * @param hostsUp
	 * @param hostsDown
	 * @return true/false indicating whether we have a host that has been shutdown
	 */
	public boolean inactiveSetChanged(Collection<Host> hostsUp, Collection<Host> hostsDown) {
		
		boolean newInactiveHostsFound = false;
		
		// Check for condition 1. 
		for (Host hostDown : hostsDown) {
			if (activeHosts.contains(hostDown)) {
				newInactiveHostsFound = true;
				break;
			}
		}
		
		// Check for condition 2. 
		Set<Host> prevActiveHosts = new HashSet<Host>(activeHosts);
		prevActiveHosts.removeAll(hostsUp);
		
		newInactiveHostsFound = !prevActiveHosts.isEmpty();
		
		return newInactiveHostsFound;
	}
	
	/**
	 * Helper method that checks if anything has changed b/w the current state and the new set of hosts up and down
	 * @param hostsUp
	 * @param hostsDown
	 * @return true/false indicating whether the set of hosts has changed or not.
	 */
	public boolean checkIfChanged(Collection<Host> hostsUp, Collection<Host> hostsDown) {
		boolean changed = activeSetChanged(hostsUp) || inactiveSetChanged(hostsUp, hostsDown);

		if (changed && logger.isDebugEnabled()) {
			Set<Host> changedHostsUp = new HashSet<>(hostsUp);
			changedHostsUp.removeAll(activeHosts);
			changedHostsUp.forEach(x -> logger.debug("New host up: {}", x.getHostAddress()));

			Set<Host> changedHostsDown = new HashSet<>(hostsDown);
			changedHostsDown.removeAll(inactiveHosts);
			changedHostsDown.forEach(x -> logger.debug("New host down: {}", x.getHostAddress()));
		}
		return changed;
	}

	/**
	 * Helper method that actually changes the state of the class to reflect the new set of hosts up and down
	 * Note that the new HostStatusTracker is returned that holds onto the new state. Calling classes must update their
	 * references to use the new HostStatusTracker
	 * 
	 * @param hostsUp
	 * @param hostsDown
	 * @return
	 */
	public HostStatusTracker computeNewHostStatus(Collection<Host> hostsUp, Collection<Host> hostsDown) {
		
		verifyMutuallyExclusive(hostsUp, hostsDown);
		
		Set<Host> nextActiveHosts = new HashSet<Host>(hostsUp);
		
		// Get the hosts that are currently down
		Set<Host> nextInactiveHosts = new HashSet<Host>(hostsDown);

        // add any previous hosts that were currently down iff they are still reported by the HostSupplier
		Set<Host> union = new HashSet<>(hostsUp);
        union.addAll(hostsDown);
        if (!union.containsAll(inactiveHosts)) {
            logger.info("REMOVING at least one inactive host from {} b/c it is no longer reported by HostSupplier",
                    inactiveHosts);
            inactiveHosts.retainAll(union);
        }
		nextInactiveHosts.addAll(inactiveHosts);
		
		// Now remove from the total set of inactive hosts any host that is currently up. 
		// This typically happens when a host moves from the inactive state to the active state. 
		// And hence it will be there in the prev inactive set, and will also be there in the new active set
		// for this round.
		for (Host host : nextActiveHosts) {
			nextInactiveHosts.remove(host);
		}
		
		// Now add any host that is not in the new active hosts set and that was in the previous active set
		Set<Host> prevActiveHosts = new HashSet<Host>(activeHosts);
		prevActiveHosts.removeAll(hostsUp);
		
		// If anyone is remaining in the prev set then add it to the inactive set, since it has gone away
		nextInactiveHosts.addAll(prevActiveHosts);
		
		for (Host host : nextActiveHosts) {
			host.setStatus(Status.Up);
		}
		for (Host host : nextInactiveHosts) {
			host.setStatus(Status.Down);
		}
		return new HostStatusTracker(nextActiveHosts, nextInactiveHosts);
	}
	
	public boolean isHostUp(Host host) {
		return activeHosts.contains(host);
	}
	
	public Collection<Host> getActiveHosts() {
		return activeHosts;
	}
	
	public Collection<Host> getInactiveHosts() {
		return inactiveHosts;
	}

	/**
	 * Returns the total number of hosts being tracked by this instance. Note that this is calculated
	 * on every invocation.
	 *
	 * @return Integer
	 */
	public int getHostCount() {
		// The host collections are never null since they are initialized during construction of this instance.
		return activeHosts.size() + inactiveHosts.size();
	}
	
	public String toString() {
		return "HostStatusTracker \nactiveSet: " + activeHosts.toString() + "\ninactiveSet: " + inactiveHosts.toString();  
	}
}
