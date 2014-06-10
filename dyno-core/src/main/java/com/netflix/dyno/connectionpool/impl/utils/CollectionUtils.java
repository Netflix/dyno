package com.netflix.dyno.connectionpool.impl.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CollectionUtils {

	public interface Transform<X,Y> {
		public Y get(X x); 
	}
	
	public interface MapEntryTransform<X,Y,Z> {
		public Z get(X x, Y y); 
	}
	
	public static interface Predicate<X> {
		public boolean apply(X x); 
	}
	
	public static <X,Y> Collection<Y> transform(Collection<X> from, Transform<X,Y> transform) {
		
		List<Y> list = new ArrayList<Y>();
		for (X x : from) {
			Y y = transform.get(x);
			list.add(y);
		}
		return list;
	}

	public static <X> Collection<X> filter(Collection<X> from, Predicate<X> predicate) {
		
		List<X> list = new ArrayList<X>();
		for (X x : from) {
			if (predicate.apply(x)) {
				list.add(x);
			}
		}
		return list;
	}
	

	public static <X> X find(Collection<X> from, Predicate<X> predicate) {
		
		for (X x : from) {
			if (predicate.apply(x)) {
				return x;
			}
		}
		return null;
	}

	public static <X,Y> Map<X,Y> filterKeys(Map<X,Y> from, Predicate<X> predicate) {
		
		Map<X,Y> toMap = new HashMap<X,Y>();
		for (X x : from.keySet()) {
			if (predicate.apply(x)) {
				toMap.put(x, from.get(x));
			}
		}
		return toMap;
	}
	
	public static <X,Y,Z> void transform(Map<X,Y> from, Map<X,Z> to, MapEntryTransform<X,Y,Z> transform) {
		
		for (X x : from.keySet()) {
			Y fromValue = from.get(x);
			Z toValue = transform.get(x, fromValue);
			to.put(x, toValue);
		}
	}

	public static <X,Y,Z> Map<X,Z> transform(Map<X,Y> from, MapEntryTransform<X,Y,Z> transform) {
		
		Map<X,Z> toMap = new HashMap<X,Z>();
		transform(from, toMap, transform);
		return toMap;
	}

	public static <X,Y> MapDifference<X,Y> difference(Map<X,Y> left, Map<X,Y> right) {
		
		MapDifference<X,Y> diff = new MapDifference<X,Y>();
		
		for (X keyInLeft: left.keySet()) {

			if (!right.containsKey(keyInLeft)) {
				diff.leftOnly.put(keyInLeft, left.get(keyInLeft));
			}
		}
		for (X keyInRight: right.keySet()) {

			if (!left.containsKey(keyInRight)) {
				diff.rightOnly.put(keyInRight, right.get(keyInRight));
			}
		}
		return diff;
	}
	
	public static class MapDifference<X,Y> {
		
		private Map<X,Y> leftOnly = new HashMap<X,Y>();
		private Map<X,Y> rightOnly = new HashMap<X,Y>();
		
		public Map<X,Y> entriesOnlyOnLeft() {
			return leftOnly;
		}
		public Map<X,Y> entriesOnlyOnRight() {
			return rightOnly;
		}
	}

	public static <X> List<X> newArrayList(X ... args) {
		return Arrays.asList(args);
	}
}
