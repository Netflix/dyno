package com.netflix.dyno.connectionpool.impl.hash;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.impl.hash.DynoBinarySearch.DynoTokenRange;

public class DynoBinarySearchTest {

	@Test
	public void testTokenRange() throws Exception {

		DynoTokenRange<Integer> r1 = new DynoTokenRange<Integer>(20, 40);

		Assert.assertEquals(1, r1.compareTo(10));
		Assert.assertEquals(1, r1.compareTo(15));
		Assert.assertEquals(1, r1.compareTo(20));
		Assert.assertEquals(0, r1.compareTo(22));
		Assert.assertEquals(0, r1.compareTo(30));
		Assert.assertEquals(0, r1.compareTo(40));
		Assert.assertEquals(-1, r1.compareTo(42));


		// First Range
		r1 = new DynoTokenRange<Integer>(null, 40);

		Assert.assertEquals(0, r1.compareTo(10));
		Assert.assertEquals(0, r1.compareTo(15));
		Assert.assertEquals(0, r1.compareTo(20));
		Assert.assertEquals(0, r1.compareTo(22));
		Assert.assertEquals(0, r1.compareTo(30));
		Assert.assertEquals(0, r1.compareTo(40));
		Assert.assertEquals(-1, r1.compareTo(42));
	}

	@Test
	public void testTokenSearch() throws Exception {

		List<Integer> list = Arrays.asList(10, 20, 30, 40, 50, 60, 70, 80, 90, 100);

		DynoBinarySearch<Integer> search = new DynoBinarySearch<Integer>(list);

		for (int i=0; i<=133; i++) {

			Integer token = search.getTokenOwner(i);
			Integer expected = getExpectedToken(i);

			Assert.assertEquals(expected, token);
		}
	}


	@Test
	public void testTokenDistribution() throws Exception {

		List<Long> tokens = Arrays.asList(611697721L, 2043353485L, 3475009249L);

		DynoBinarySearch<Long> search = new DynoBinarySearch<Long>(tokens);

		Map<Long, Long> tokenCount = new HashMap<Long, Long>();

		tokenCount.put(611697721L, 0L);
		tokenCount.put(2043353485L, 0L);
		tokenCount.put(3475009249L, 0L);

		Murmur1HashPartitioner hash = new Murmur1HashPartitioner();

		for (int i=0; i<1000000; i++) {

			// Compute the hash
			long lHash = hash.hash("" + i);

			// Now lookup the node
			Long token = search.getTokenOwner(lHash);

			Long count = tokenCount.get(token);
			count++;
			tokenCount.put(token, count);
		}

		long total = 0;
		for (Long value : tokenCount.values()) {
			total += value;
		}

		for (Long token : tokenCount.keySet()) {

			Long value = tokenCount.get(token);
			Long percent = value*100/total;

			Assert.assertTrue("Percentage is off for tokenCount: " + tokenCount, percent >= 30 && percent <= 35);
		}
	}

	private int getExpectedToken(int key) throws Exception {

		if (key < 10) {
			return 10;
		}

		if (key > 100) {
			return 10;
		}

		if (key % 10 == 0) {
			return key;
		}

		return key + (10 - key%10);
	}
}

