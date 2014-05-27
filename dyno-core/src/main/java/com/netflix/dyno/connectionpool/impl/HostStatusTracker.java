package com.netflix.dyno.connectionpool.impl;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.Nullable;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.netflix.dyno.connectionpool.Host;

public class HostStatusTracker {
	
	private final Set<Host> activeHosts = new HashSet<Host>();
	private final Set<Host> inactiveHosts = new HashSet<Host>();
	
	public HostStatusTracker() {
	}
	
	public HostStatusTracker(Collection<Host> up, Collection<Host> down) {

		verifyMutuallyExclusive(up, down);
		
		activeHosts.addAll(up);
		inactiveHosts.addAll(down);
	}

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
	
	public boolean checkIfChanged(Collection<Host> hostsUp, Collection<Host> hostsDown) {
		return activeSetChanged(hostsUp) || inactiveSetChanged(hostsUp, hostsDown);
	}
	
	public HostStatusTracker computeNewHostStatus(Collection<Host> hostsUp, Collection<Host> hostsDown) {
		
		verifyMutuallyExclusive(hostsUp, hostsDown);
		
		Set<Host> nextActiveHosts = new HashSet<Host>(hostsUp);
		
		// Get the hosts that are currently down
		Set<Host> nextInactiveHosts = new HashSet<Host>(hostsDown);
		// add any previous hosts that were currently down
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
	
	public String toString() {
		return "HostStatusTracker \nactiveSet: " + activeHosts.toString() + "\ninactiveSet: " + inactiveHosts.toString();  
	}

	public static class UnitTest {
		
		@Test
		public void testMutuallyExclusive() throws Exception {
			
			Set<Host> up = getHostSet("A", "B", "D", "E");
			Set<Host> down = getHostSet("C", "F", "H");
			
			new HostStatusTracker(up, down);
			
			up = getHostSet();
			down = getHostSet("C", "F", "H");
			
			new HostStatusTracker(up, down);

			up = getHostSet("A", "C", "D");
			down = getHostSet();
			
			new HostStatusTracker(up, down);
		}
		
		@Test (expected=RuntimeException.class)
		public void testNotMutuallyExclusive() throws Exception {
			
			Set<Host> up = getHostSet("A", "B", "D", "E");
			Set<Host> down = getHostSet("C", "F", "H", "E");
			
			new HostStatusTracker(up, down);
		}
		
		@Test
		public void testEurekaUpdates() throws Exception {
			
			Set<Host> up = getHostSet("A", "B", "D", "E");
			Set<Host> down = getHostSet("C", "F", "H");
			
			// First time update
			HostStatusTracker tracker = new HostStatusTracker(up, down);
			
			verifySet(tracker.activeHosts, "A", "E", "D" ,"B");
			verifySet(tracker.inactiveHosts, "C", "H" ,"F");
			
			// Round 2. New server 'J' shows up
			tracker = tracker.computeNewHostStatus(getHostSet("A", "B", "E", "D", "J"), getHostSet());

			verifySet(tracker.activeHosts, "A", "E", "D", "J", "B");
			verifySet(tracker.inactiveHosts, "C", "H" ,"F");
			
			// Round 3. server 'A' goes from active to inactive
			tracker = tracker.computeNewHostStatus(getHostSet("B", "E", "D", "J"), getHostSet("A", "C", "H", "F"));

			verifySet(tracker.activeHosts, "E", "D", "J", "B");
			verifySet(tracker.inactiveHosts, "C", "A", "H" ,"F");

			// Round 4. New servers 'X' and 'Y' show up and "D" goes from active to inactive
			tracker = tracker.computeNewHostStatus(getHostSet("X", "Y", "B", "E", "J"), getHostSet("A", "C", "D", "H", "F"));

			verifySet(tracker.activeHosts, "X", "Y", "B", "E", "J");
			verifySet(tracker.inactiveHosts, "C", "A", "H", "D", "F");

			// Round 5. server "B" goes MISSING
			tracker = tracker.computeNewHostStatus(getHostSet("X", "Y", "E", "J"), getHostSet("A", "C", "D", "H", "F"));

			verifySet(tracker.activeHosts, "X", "Y", "E", "J");
			verifySet(tracker.inactiveHosts, "C", "A", "H", "D", "F", "B");

			// Round 6. server "E" and "J" go MISSING and new server "K" show up and "A" and "C" go from inactive to active
			tracker = tracker.computeNewHostStatus(getHostSet("X", "Y", "A", "K", "C"), getHostSet("D", "H", "F"));

			verifySet(tracker.activeHosts, "X", "Y", "A", "C", "K");
			verifySet(tracker.inactiveHosts, "E", "J", "H", "D", "F", "B");
			
			// Round 7. all active hosts go from active to inactive
			tracker = tracker.computeNewHostStatus(getHostSet(), getHostSet("D", "H", "F", "X", "Y", "A", "K", "C"));

			verifySet(tracker.activeHosts, "");
			verifySet(tracker.inactiveHosts, "E", "J", "H", "D", "F", "B", "X", "Y", "A", "K", "C");
			
			// Round 8. 'X' 'Y' 'A' and 'C' go from inactive to active and 'K' disappears from down list
			tracker = tracker.computeNewHostStatus(getHostSet("X", "Y", "A", "C"), getHostSet("D", "H", "F"));

			verifySet(tracker.activeHosts, "X", "Y", "A", "C");
			verifySet(tracker.inactiveHosts, "E", "J", "H", "D", "F", "B",  "K");

			// Round 9. All inactive hosts disappear
			tracker = tracker.computeNewHostStatus(getHostSet("X", "Y", "A", "C"), getHostSet());

			verifySet(tracker.activeHosts, "X", "Y", "A", "C");
			verifySet(tracker.inactiveHosts, "E", "J", "H", "D", "F", "B",  "K");

			// Round 9. All active hosts disappear
			tracker = tracker.computeNewHostStatus(getHostSet(), getHostSet("K", "J"));

			verifySet(tracker.activeHosts, "");
			verifySet(tracker.inactiveHosts, "E", "J", "H", "D", "F", "B",  "K", "X", "Y", "A", "C");

			// Round 10. All hosts disappear
			tracker = tracker.computeNewHostStatus(getHostSet(), getHostSet());

			verifySet(tracker.activeHosts, "");
			verifySet(tracker.inactiveHosts, "E", "J", "H", "D", "F", "B",  "K", "X", "Y", "A", "C");
		}
		
		private Set<Host> getHostSet(String ...names) { 

			Set<Host> set = new HashSet<Host>();
			if (names != null && names.length > 0) {
				for (String name : names) {
					if (!name.isEmpty()) {
						set.add(new Host(name, 1234));
					}
				}
			}
			return set;
		}
		
		private void verifySet(Set<Host> set, String ... names) {
			
			Set<String> expected = new HashSet<String>();
			if (names != null && names.length > 0) {
				expected = new HashSet<String>(Arrays.asList(names));
			}
			
			Set<String> result = new HashSet<String>( Collections2.transform(set, new Function<Host, String>() {

				@Override
				@Nullable
				public String apply(@Nullable Host input) {
					return input.getHostName();
				}
			}));
			
			Assert.assertEquals(expected, result);
		}
	}
}
