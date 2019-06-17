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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Transform;

public class HostStatusTrackerTest {

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

    @Test(expected = RuntimeException.class)
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

        verifySet(tracker.getActiveHosts(), "A", "E", "D", "B");
        verifySet(tracker.getInactiveHosts(), "C", "H", "F");

        // Round 2. New server 'J' shows up
        tracker = tracker.computeNewHostStatus(getHostSet("A", "B", "E", "D", "J"), getHostSet("C", "H", "F"));

        verifySet(tracker.getActiveHosts(), "A", "E", "D", "J", "B");
        verifySet(tracker.getInactiveHosts(), "C", "H", "F");

        // Round 3. server 'A' goes from active to inactive
        tracker = tracker.computeNewHostStatus(getHostSet("B", "E", "D", "J"), getHostSet("A", "C", "H", "F"));

        verifySet(tracker.getActiveHosts(), "E", "D", "J", "B");
        verifySet(tracker.getInactiveHosts(), "C", "A", "H", "F");

        // Round 4. New servers 'X' and 'Y' show up and "D" goes from active to inactive
        tracker = tracker.computeNewHostStatus(getHostSet("X", "Y", "B", "E", "J"), getHostSet("A", "C", "D", "H", "F"));

        verifySet(tracker.getActiveHosts(), "X", "Y", "B", "E", "J");
        verifySet(tracker.getInactiveHosts(), "C", "A", "H", "D", "F");

        // Round 5. server "B" goes MISSING
        tracker = tracker.computeNewHostStatus(getHostSet("X", "Y", "E", "J"), getHostSet("A", "C", "D", "H", "F"));

        verifySet(tracker.getActiveHosts(), "X", "Y", "E", "J");
        verifySet(tracker.getInactiveHosts(), "C", "A", "H", "D", "F", "B");

        // Round 6. server "E" and "J" go MISSING and new server "K" shows up and "A" and "C" go from inactive to active
        tracker = tracker.computeNewHostStatus(getHostSet("X", "Y", "A", "K", "C"), getHostSet("D", "H", "F"));

        verifySet(tracker.getActiveHosts(), "X", "Y", "A", "C", "K");
        verifySet(tracker.getInactiveHosts(), "H", "D", "F", "E", "J");

        // Round 7. all active hosts go from active to inactive
        tracker = tracker.computeNewHostStatus(getHostSet(), getHostSet("D", "H", "F", "X", "Y", "A", "K", "C"));

        verifySet(tracker.getActiveHosts(), "");
        verifySet(tracker.getInactiveHosts(), "H", "D", "F", "X", "Y", "A", "K", "C");

        // Round 8. 'X' 'Y' 'A' and 'C' go from inactive to active and 'K' disappears from down list
        tracker = tracker.computeNewHostStatus(getHostSet("X", "Y", "A", "C"), getHostSet("D", "H", "F"));

        verifySet(tracker.getActiveHosts(), "X", "Y", "A", "C");
        verifySet(tracker.getInactiveHosts(), "H", "D", "F");

        // Round 9. All inactive hosts disappear
        tracker = tracker.computeNewHostStatus(getHostSet("X", "Y", "A", "C"), getHostSet());

        verifySet(tracker.getActiveHosts(), "X", "Y", "A", "C");
        verifySet(tracker.getInactiveHosts(), "");

        // Round 9. All active hosts disappear
        tracker = tracker.computeNewHostStatus(getHostSet(), getHostSet("K", "J"));

        verifySet(tracker.getActiveHosts(), "");
        verifySet(tracker.getInactiveHosts(), "J", "K", "X", "Y", "A", "C");

        // Round 10. All hosts disappear
        tracker = tracker.computeNewHostStatus(getHostSet(), getHostSet());

        verifySet(tracker.getActiveHosts(), "");
        verifySet(tracker.getInactiveHosts(), "");
    }

    private Set<Host> getHostSet(String... names) {

        Set<Host> set = new HashSet<Host>();
        if (names != null && names.length > 0) {
            for (String name : names) {
                if (!name.isEmpty()) {
                    set.add(new Host(name, 1234, "r1"));
                }
            }
        }
        return set;
    }

    private void verifySet(Collection<Host> hosts, String... names) {

        Set<String> expected = new HashSet<String>();
        if (names != null && names.length > 0) {
            for (String n : names) {
                if (n != null && !n.isEmpty()) {
                    expected.add(n);
                }
            }
        }

        Set<String> result = new HashSet<String>(CollectionUtils.transform(hosts, new Transform<Host, String>() {

            @Override
            public String get(Host x) {
                return x.getHostAddress();
            }
        }));

        Assert.assertEquals(expected, result);
    }
}

