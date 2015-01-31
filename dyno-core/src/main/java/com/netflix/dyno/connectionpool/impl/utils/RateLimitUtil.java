package com.netflix.dyno.connectionpool.impl.utils;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class RateLimitUtil {

	private final AtomicReference<InnerState> ref = new AtomicReference<InnerState>(null);
	
	private RateLimitUtil(int rps) {
		this.ref.set(new InnerState(rps));
	}
	
	public static RateLimitUtil create(int n) {
		return new RateLimitUtil(n);
	}
	
	public int getRps() {
		return ref.get().getRps();
	}
	
	public boolean acquire() {
		
		if (ref.get().checkSameSecond()) {
			long timeToSleepMs = ref.get().increment();
			if (timeToSleepMs != -1) {
				try {
					Thread.sleep(timeToSleepMs);
					return false;
				} catch (InterruptedException e) {
					// do nothing here
					return false;
				}
			} else {
				return true;
			}
			
		} else {
			
			InnerState oldState = ref.get();
			InnerState newState = new InnerState(oldState.limit);
			
			ref.compareAndSet(oldState, newState);
			return false;
		}
	}
	
	
	private class InnerState {
		
		private final AtomicInteger counter = new AtomicInteger();
		private final AtomicLong second = new AtomicLong(0L);
		private final AtomicLong origTime = new AtomicLong(0L);

		private final int limit; 
		
		private InnerState(int limit) {
			this.limit = limit;
			counter.set(0);
			origTime.set(System.currentTimeMillis());
			second.set(origTime.get()/1000);
		}
		
		private boolean checkSameSecond() {
			long time = System.currentTimeMillis();
			return second.get() == time/1000;
		}
		
		private long increment() {
			
			if (counter.get() < limit) {
				counter.incrementAndGet();
				return -1;
			} else {
				return System.currentTimeMillis() - origTime.get();
			}
		}
		
		private int getRps() {
			return limit;
		}
	}
}
