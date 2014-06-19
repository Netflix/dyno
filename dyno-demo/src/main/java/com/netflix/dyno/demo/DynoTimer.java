package com.netflix.dyno.demo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.util.concurrent.AtomicDouble;

public class DynoTimer {

	private final int numBuckets;
	private final int computeMillis;
	private final int resetMillis;

	private final AtomicReference<InnerState> state = new AtomicReference<InnerState>(null);
	private final AtomicLong lastComputeTime = new AtomicLong(0L);
	private final AtomicLong lastResetTime = new AtomicLong(0L);
	
	private final AtomicBoolean resetLock = new AtomicBoolean(false);

	public DynoTimer(int nBuckets, int computeFreqSeconds, int resetFreqSeconds) {
		
		numBuckets = nBuckets;
		computeMillis = computeFreqSeconds*1000;
		resetMillis = resetFreqSeconds*1000;

		state.set(new InnerState(numBuckets));
		
		lastComputeTime.set(System.currentTimeMillis());
		lastResetTime.set(System.currentTimeMillis());
	}
	
	public void record(Long duration, TimeUnit unit) {
		recordLatencyMillis(TimeUnit.MILLISECONDS.convert(duration, unit));
	}
	
	public void recordLatencyMillis(long durationMillis) {
		
		state.get().recordLatencyMillis(durationMillis);
		
		long currentTime = System.currentTimeMillis();
		
		/** CHECK IF LATENCIES NEED TO BE RE COMPUTED */
		if ((currentTime - lastComputeTime.get()) > computeMillis) {
			state.get().computeLatencies();
			lastComputeTime.set(currentTime);
		}
		
		/** CHECK IF LATENCIES NEED TO BE RE RESET */
//		if ((currentTime - lastResetTime.get()) > resetMillis) {
//			
//			if (resetLock.get()) {
//				return; // lock already acquired
//			}
//			
//			// acquire lock
//			if (!resetLock.compareAndSet(false, true)) {
//				return;
//			}
//			
//			try { 
//				state.set(new InnerState(numBuckets));
//				lastResetTime.set(currentTime);
//			} finally {
//				resetLock.set(false); //release lock
//			}
//		}
	}

	public Double getP50Millis() {
		return state.get().p50.get();
	}
	
	public Double getP99Millis() {
		return state.get().p99.get();
	}

	public Double getP995Millis() {
		return state.get().p995.get();
	}

	public Double getP999Millis() {
		return state.get().p999.get();
	}
	
	public Double getAvgMillis() {
		return state.get().avg.get();
	}

	public String toString() {
		return state.get().toString();
	}
	
	//private static final AtomicInteger cc = new AtomicInteger(0);

	private class InnerState {
		
		private final List<Bucket> buckets = new ArrayList<Bucket>();
		private final int n; 
		
		private final AtomicDouble p50 = new AtomicDouble(0);
		private final AtomicDouble p99 = new AtomicDouble(0);
		private final AtomicDouble p995 = new AtomicDouble(0);
		private final AtomicDouble p999 = new AtomicDouble(0);
		
		private final AtomicDouble avg = new AtomicDouble(0);
		
		private final AtomicBoolean computeLock = new AtomicBoolean(false);

		private InnerState(int nBuckets) {
			
			n = nBuckets;
			for (int i=0; i<nBuckets; i++) {
				buckets.add(new Bucket());
			}
		}
		
		private void recordLatencyMillis(long durationMillis) {
			
			long  currentTime = System.currentTimeMillis();
			int bIndex = (int)(currentTime % n);
			buckets.get(bIndex).addDuration(durationMillis);
		}


		private void computeLatencies() {

			if (computeLock.get()) {
				return;
			}

			// Acquire lock
			if (!computeLock.compareAndSet(false, true)) {
				return;
			}

			try {

				List<Double> sorted = new ArrayList<Double>();
				for (Bucket b : buckets) {
					sorted.add(b.getAvgDuration());
				}
				Collections.sort(sorted);
				
				p50.set(Percentile.P_50.getValue(sorted));
				p99.set(Percentile.P_99.getValue(sorted));
				p995.set(Percentile.P_995.getValue(sorted));
				p999.set(Percentile.P_999.getValue(sorted));

				Long totalDuration = 0L; Long totalCount = 0L;
				for (Bucket b : buckets) {
					totalDuration += b.getTotalDuration();
					totalCount += b.count.get();
				}

				Double avgDuration = totalCount > 0 ? (double)((double)totalDuration/(double)totalCount) : 0;
				avg.set(avgDuration);
				
			} finally {
				computeLock.set(false);  // release lock
				System.out.println("Latencies: " + this.toString());
			}
		}
		
		public String toString() {
			return "Avg: " + avg.get() + ", P50: " + p50.get() + ", P99: " + p99.get() + ", P99.5: " + p995.get() + ", P99.9: " + p999.get();
		}
	}
	
	private class Bucket implements Comparable<Bucket> {
		
		private final AtomicLong duration = new AtomicLong(0L);
		private final AtomicLong count = new AtomicLong(0L);
		
		private void addDuration(Long durationMillis) {
			duration.addAndGet(durationMillis);
			count.incrementAndGet();
		}
		
		private Long getTotalDuration() {
			return duration.get();
		}
		
		private Double getAvgDuration() {
			return count.get() > 0 ? (double)((double)duration.get()/(double)count.get()) : 0;
		}

		@Override
		public int compareTo(Bucket o) {
			return getAvgDuration().compareTo(o.getAvgDuration());
		}
		
		public String toString() {
			return ""+getAvgDuration();// + ":" + count.get(); 
		}
	}
	
	private enum Percentile { 
		
		P_50(0.50), P_99(0.99), P_995(0.995), P_999(0.999);
		
		private double p; 
		
		private Percentile(double pVal) {
			p = pVal;
		}
		
		private Double getValue(List<Double> sortedList) {
			int size = sortedList.size();
			if (size == 0) {
				return 0.0;
			}
			int index = (int)(p*size);
			return sortedList.get(index);
		}
	}
	
	public static void main(String[] args) {
		
		final DynoTimer timer = new DynoTimer(1000, 3, 120);
		
		final ExecutorService thPool = Executors.newFixedThreadPool(10);
		final AtomicBoolean stop = new AtomicBoolean(false);
		
		for (int i=0; i<10; i++) {
			
			thPool.submit(new Callable<Void>() {
				
				final Random rand = new Random();
				
				@Override
				public Void call() throws Exception {
					
					while (!stop.get()) {
						
						long duration = (long)rand.nextInt(40);
						timer.record(duration, TimeUnit.MILLISECONDS);
						Thread.sleep(rand.nextInt(40));
					}
					return null;
				}
				
			});
		}
		
		try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		System.out.println("Avg: " + timer.getAvgMillis());
		System.out.println("P50: " + timer.getP50Millis());
		System.out.println("P99: " + timer.getP99Millis());
		System.out.println("P995: " + timer.getP995Millis());
		System.out.println("P999: " + timer.getP999Millis());
		
		thPool.shutdownNow();
		
		
	}
}
