package com.netflix.dyno.connectionpool.impl.lb;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.Host;

public class HostTokenTest {

	@Test
	public void testEquals() throws Exception {

		HostToken t1 = new HostToken(1L, new Host("foo", 1234));
		HostToken t2 = new HostToken(1L, new Host("foo", 1234));

		Assert.assertEquals(t1, t2);

		// change token
		HostToken t3 = new HostToken(2L, new Host("foo", 1234));
		Assert.assertFalse(t1.equals(t3));

		// change host name
		HostToken t4 = new HostToken(1L, new Host("foo1", 1234));
		Assert.assertFalse(t1.equals(t4));
	}

	@Test
	public void testSort() throws Exception {

		HostToken t1 = new HostToken(1L, new Host("foo1", 1234));
		HostToken t2 = new HostToken(2L, new Host("foo2", 1234));
		HostToken t3 = new HostToken(3L, new Host("foo3", 1234));
		HostToken t4 = new HostToken(4L, new Host("foo4", 1234));
		HostToken t5 = new HostToken(5L, new Host("foo5", 1234));

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
