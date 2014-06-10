package com.netflix.dyno.connectionpool.impl.hash;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

public class DynoBinarySearch<T extends Comparable<T>> {
	
	private final List<DynoTokenRange> rangeList = new ArrayList<DynoTokenRange>();
	
	public DynoBinarySearch(List<T> list) {

		if (list.isEmpty()) {
			throw new RuntimeException("List must not be empty");
		}

		if (list.size() == 1) {
			rangeList.add(new DynoTokenRange(list.get(0)));
			
		} else {
			for (int i=0; i<(list.size()-1); i++) {
				rangeList.add(new DynoTokenRange(list.get(i), list.get(i+1)));
			}
			// add the last range
			rangeList.add(new DynoTokenRange(list.get(list.size()-1)));
		}
	}


	public T getTokenOwner(T token) {
		
		// Some quick boundary checks
		
		if (rangeList.size() == 1) {
			return rangeList.get(0).getTokenOwner();
		}
		
		DynoTokenRange firstRange = rangeList.get(0);
		DynoTokenRange lastRange = rangeList.get(rangeList.size()-1);
		
		if (firstRange.compareTo(token) > 0) {
			// First range is greater than this token. Wrap around to last range.
			return lastRange.getTokenOwner();
		}
		
		int index = Collections.binarySearch(rangeList, token);
		
		if (index < 0) {
			throw new RuntimeException("Token not found!: " + token);
		}
		
		return rangeList.get(index).getTokenOwner();
	}
	
	public String toString() {
		
		StringBuilder sb = new StringBuilder("[DynoBinarySearch:\n");
		for (DynoTokenRange r : rangeList) {
			sb.append(r.toString()).append("\n");
		}
		sb.append("]");
		return sb.toString();
	}
	
	private class DynoTokenRange implements Comparable<T> {
		
		final T start; 
		final T end;
		boolean isLastRange = false;
		
		DynoTokenRange(T s, T e) {
			this.start = s;
			this.end = e;
			
			if (!(lessThan(start, end))) {
				throw new RuntimeException("Bad Range: start must be less than end: " + this.toString());
			}
			
			isLastRange = false;
		}
		
		DynoTokenRange(T start) {
			this.start = start;
			this.end = null;
			isLastRange = true;
		}

		public T getTokenOwner() {
			return start;
		}
		
		public String toString() {
			if (isLastRange) {
				return "start: " + start;
			} else {
				return "start: " + start + ", end: " + end;
			}
		}

		@Override
		public int compareTo(T key) {

			if (isLastRange) {
				
				if (lessThanEquals(start, key)) {
					return 0;  // This key is within this range
				} else {
					return -1;
				}
			}
			
			// Functionality for any other range i.e in another position in the list except for the last 
			if (lessThanEquals(start, key) && lessThan(key, end)) {
				return 0;  // This key is within this range
			}
			
			if (lessThanEquals(end, key)) {
				return -1;
			} else {
				return 1;
			}
		}
		
		private boolean lessThan(T left, T right) {
			//return left.compareTo(right) < 0;
			return left.compareTo(right) < 0;
		}

		private boolean lessThanEquals(T left, T right) {
			return left.compareTo(right) <= 0;
		}
	}
	
	
	public static class UnitTest {
		
		@Test
		public void testTokenSearch() throws Exception {
			
			List<Integer> list = Arrays.asList(10, 20, 30, 40, 50, 60, 70, 80, 90, 100);
			
			DynoBinarySearch<Integer> search = new DynoBinarySearch<Integer>(list);
			
			for (int i=0; i<130; i++) {
				
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
				return 100;
			}
			
			if (key > 100) {
				return 100;
			}
			
			if (key % 10 == 0) {
				return key;
			}
			
			return key - key%10;
		}
	}
	
}
