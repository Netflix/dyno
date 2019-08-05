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
package com.netflix.dyno.connectionpool.impl.lb;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.netflix.dyno.connectionpool.HostBuilder;
import org.junit.Assert;
import org.junit.Test;

public class HostTokenTest {

    @Test
    public void testEquals() throws Exception {

        HostToken t1 = new HostToken(1L, new HostBuilder().setHostname("foo").setPort(1234).setRack("foo_rack").createHost());
        HostToken t2 = new HostToken(1L, new HostBuilder().setHostname("foo").setPort(1234).setRack("foo_rack").createHost());

        Assert.assertEquals(t1, t2);

        // change token
        HostToken t3 = new HostToken(2L, new HostBuilder().setHostname("foo").setPort(1234).setRack("foo_rack").createHost());
        Assert.assertFalse(t1.equals(t3));

        // change host name
        HostToken t4 = new HostToken(1L, new HostBuilder().setHostname("foo1").setPort(1234).setRack("foo_rack").createHost());
        Assert.assertFalse(t1.equals(t4));
    }

    @Test
    public void testSort() throws Exception {

        HostToken t1 = new HostToken(1L, new HostBuilder().setHostname("foo1").setPort(1234).setRack("foo_rack").createHost());
        HostToken t2 = new HostToken(2L, new HostBuilder().setHostname("foo2").setPort(1234).setRack("foo_rack").createHost());
        HostToken t3 = new HostToken(3L, new HostBuilder().setHostname("foo3").setPort(1234).setRack("foo_rack").createHost());
        HostToken t4 = new HostToken(4L, new HostBuilder().setHostname("foo4").setPort(1234).setRack("foo_rack").createHost());
        HostToken t5 = new HostToken(5L, new HostBuilder().setHostname("foo5").setPort(1234).setRack("foo_rack").createHost());

        HostToken[] arr = {t5, t2, t4, t3, t1};
        List<HostToken> list = Arrays.asList(arr);

        Assert.assertEquals(t5, list.get(0));
        Assert.assertEquals(t2, list.get(1));
        Assert.assertEquals(t4, list.get(2));
        Assert.assertEquals(t3, list.get(3));
        Assert.assertEquals(t1, list.get(4));

        Collections.sort(list, new Comparator<HostToken>() {
            @Override
            public int compare(HostToken o1, HostToken o2) {
                return o1.compareTo(o2);
            }
        });

        Assert.assertEquals(t1, list.get(0));
        Assert.assertEquals(t2, list.get(1));
        Assert.assertEquals(t3, list.get(2));
        Assert.assertEquals(t4, list.get(3));
        Assert.assertEquals(t5, list.get(4));
    }
}
