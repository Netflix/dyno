package com.netflix.dyno.connectionpool.impl.hash;

public class UnsignedIntsUtils {
	
	static final long INT_MASK = 0xffffffffL;

	private UnsignedIntsUtils() {}

	/**
	 * Returns the value of the given {@code int} as a {@code long}, when treated as unsigned.
	 */
	public static long toLong(int value) {
		return value & INT_MASK;
	}
}
