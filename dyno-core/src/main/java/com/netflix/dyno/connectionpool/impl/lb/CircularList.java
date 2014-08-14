/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.dyno.connectionpool.impl.lb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.math.stat.descriptive.SummaryStatistics;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils.Predicate;

/**
 * Utility class that encapsulates a thread safe circular list. It also provides functionality to be able to dynamically add and remove 
 * elements from the list in a thread safe manner while callers to the class are still using the list. 
 * 
 * This utility is mainly useful for ROUND ROBIN style load balancers. It is also useful for Connection pool monitors that need to track 
 * state of operations against a connection pool in a bounded circular buffer
 *  
 * @author poberai
 *
 * @param <T>
 */
public class CircularList<T> {

	// The thread safe reference to the inner list. Maintaining an atomic ref at this level helps enabling swapping out of the entire list 
	// underneath when there is a change to the list such as element addition or removal
	private final AtomicReference<InnerList> ref  = new AtomicReference<InnerList>(null);
	
	/**
	 * Constructor
	 * @param origList
	 */
	public CircularList(Collection<T> origList) {
		ref.set(new InnerList(origList));
	}

	/**
	 * Get the next element in the list
	 * @return T
	 */ 
	public T getNextElement() {
		return ref.get().getNextElement();
	}

	/**
	 * Swap the entire inner list with a new list
	 * @param newList
	 */
	public void swapWithList(Collection<T> newList) {
		
		InnerList newInnerList = new InnerList(newList);
		ref.set(newInnerList);
	}
	
	/**
	 * Add an element to the list. This causes the inner list to be swapped out
	 * @param element
	 */
	public void addElement(T element) {
		
		List<T> origList = ref.get().list;
		boolean isPresent = origList.contains(element);
		if (isPresent) {
			return;
		}
		
		List<T> newList = new ArrayList<T>(origList);
		newList.add(element);
		
		swapWithList(newList);
	}
	
	/**
	 * Remove an element from this list. This causes the inner list to be swapped out
	 * @param element
	 */
	public void removeElement(T element) {
		
		List<T> origList = ref.get().list;
		boolean isPresent = origList.contains(element);
		if (!isPresent) {
			return;
		}
		
		List<T> newList = new ArrayList<T>(origList);
		newList.remove(element);
		
		swapWithList(newList);
	}
	
	/**
	 * Helpful utility to access the inner list. Must be used with care since the inner list can change. 
	 * @return List<T>
	 */
	public List<T> getEntireList() {
		InnerList iList = ref.get();
		return iList != null ? iList.getList() : null;
	}
	
	/**
	 * Gets the size of the bounded list underneath. Note that this num can change if the inner list is swapped out.
	 * @return
	 */
	public int getSize() {
		InnerList iList = ref.get();
		return iList != null ? iList.getList().size() : 0;
	}

	/**
	 * The inner list which manages the circular access to the actual list. 
	 * @author poberai
	 *
	 */
	private class InnerList { 
		
		private List<T> list = new ArrayList<T>();
		private final Integer size;

		// The rotating index over the list. currentIndex always indicates the index of the element that was last accessed
		private AtomicInteger currentIndex = new AtomicInteger(0);
		
		private InnerList(Collection<T> newList) {
			if (newList != null) {
				list.addAll(newList);
				size = list.size();
			} else {
				size = 0;
			}
		}
		
		private int getNextIndex() {
			int current = currentIndex.incrementAndGet();
			return (current % size);
		}

		private T getNextElement() {
			
			if (list == null || list.size() == 0) {
				return null;
			}
			
			if (list.size() == 1) {
				return list.get(0);
			}
			
			return list.get(getNextIndex());
		}
		
		private List<T> getList() {
			return list;
		}
	}
	
	public static class UnitTest { 
		
		private static final List<Integer> iList = new ArrayList<Integer>();
		private static final CircularList<Integer> cList = new CircularList<Integer>(iList);
		private static final Integer size = 10;
		
		private static ExecutorService threadPool;
		
		@BeforeClass
		public static void beforeClass() {
			threadPool = Executors.newFixedThreadPool(5);
		}
		
		@Before
		public void beforeTest() {
			
			iList.clear();
			for (int i=0; i<size; i++) {
				iList.add(i);
			}
			cList.swapWithList(iList);
		}
		
		@AfterClass
		public static void afterClass() {
			threadPool.shutdownNow();
		}

		@Test
		public void testSingleThread() throws Exception {
			
			TestWorker worker = new TestWorker();
			
			for (int i=0; i<100; i++) {
				worker.process();
			}
			
			System.out.println(worker.map);
			
			for (Integer key : worker.map.keySet()) {
				Assert.assertTrue(worker.map.toString(), 10 == worker.map.get(key));
			}
		}
		
		@Test
		public void testSingleThreadWithElementAdd() throws Exception {
			
			final AtomicBoolean stop = new AtomicBoolean(false);
			
			Future<Map<Integer, Integer>> future = threadPool.submit(new Callable<Map<Integer, Integer>>() {

				@Override
				public Map<Integer, Integer> call() throws Exception {
					
					TestWorker worker = new TestWorker();
					
					while(!stop.get()) {
						worker.process();
					}
					
					return worker.map;
				}
			});
			
			Thread.sleep(500);
			
			List<Integer> newList = new ArrayList<Integer>();
			newList.addAll(iList);
			for (int i=10; i<15; i++) {
				newList.add(i);
			}
			
			cList.swapWithList(newList);
			
			Thread.sleep(100);
			
			
			stop.set(true);
			
			Map<Integer, Integer> result = future.get();
			
			Map<Integer, Integer> subMap = CollectionUtils.filterKeys(result, new Predicate<Integer>() {
				@Override
				public boolean apply(Integer input) {
					return input != null && input < 10 ;
				}
			});
			
			List<Integer> list = new ArrayList<Integer>(subMap.values());
			checkValues(list);
			
			subMap = CollectionUtils.difference(result, subMap).entriesOnlyOnLeft();
			list = new ArrayList<Integer>(subMap.values());
			checkValues(list);
		}

		
		@Test
		public void testSingleThreadWithElementRemove() throws Exception {
			
			final AtomicBoolean stop = new AtomicBoolean(false);
			
			Future<Map<Integer, Integer>> future = threadPool.submit(new Callable<Map<Integer, Integer>>() {

				@Override
				public Map<Integer, Integer> call() throws Exception {
					
					TestWorker worker = new TestWorker();
					
					while(!stop.get()) {
						worker.process();
					}
					
					return worker.map;
				}
			});
			
			Thread.sleep(200);
			
			List<Integer> newList = new ArrayList<Integer>();
			newList.addAll(iList);
			
			final List<Integer> removedElements = new ArrayList<Integer>();
			removedElements.add(newList.remove(2));
			removedElements.add(newList.remove(5));
			removedElements.add(newList.remove(6));
			
			cList.swapWithList(newList);
			
			Thread.sleep(200);
			stop.set(true);
			
			Map<Integer, Integer> result = future.get();

			Map<Integer, Integer> subMap = CollectionUtils.filterKeys(result, new Predicate<Integer>() {
				@Override
				public boolean apply(Integer input) {
					return !removedElements.contains(input);
				}
			});
			
			checkValues(new ArrayList<Integer>(subMap.values()));
		}
		
		@Test
		public void testMultipleThreads() throws Exception {
			
			final AtomicBoolean stop = new AtomicBoolean(false);
			final CyclicBarrier barrier = new CyclicBarrier(5);
			final List<Future<Map<Integer, Integer>>> futures = new ArrayList<Future<Map<Integer, Integer>>>();
			
			for (int i=0; i<5; i++) {
				futures.add(threadPool.submit(new Callable<Map<Integer, Integer>>() {

					@Override
					public Map<Integer, Integer> call() throws Exception {
						
						barrier.await();
						
						TestWorker worker = new TestWorker();
						
						while (!stop.get()) {
							worker.process();
						}
						
						return worker.map;
					}
				}));
			}
			
			Thread.sleep(200);
			stop.set(true);
			
			Map<Integer, Integer> totalMap = getTotalMap(futures);
			checkValues(new ArrayList<Integer>(totalMap.values()));
		}
		
		
		@Test
		public void testMultipleThreadsWithElementAdd() throws Exception {
			
			final AtomicBoolean stop = new AtomicBoolean(false);
			final CyclicBarrier barrier = new CyclicBarrier(5);
			final List<Future<Map<Integer, Integer>>> futures = new ArrayList<Future<Map<Integer, Integer>>>();
			
			for (int i=0; i<5; i++) {
				futures.add(threadPool.submit(new Callable<Map<Integer, Integer>>() {

					@Override
					public Map<Integer, Integer> call() throws Exception {
						
						barrier.await();
						
						TestWorker worker = new TestWorker();
						
						while (!stop.get()) {
							worker.process();
						}
						
						return worker.map;
					}
				}));
			}
			
			Thread.sleep(200);
			
			List<Integer> newList = new ArrayList<Integer>(iList);
			for (int i=10; i<15; i++) {
				newList.add(i);
			}
			
			cList.swapWithList(newList);
			
			Thread.sleep(200);
			stop.set(true);
			
			Map<Integer, Integer> result = getTotalMap(futures);
			
			Map<Integer, Integer> subMap = CollectionUtils.filterKeys(result, new Predicate<Integer>() {
				@Override
				public boolean apply(Integer input) {
					return input < 10;
				}
			});
			
			checkValues(new ArrayList<Integer>(subMap.values()));
			
			subMap = CollectionUtils.difference(result, subMap).entriesOnlyOnLeft();
			checkValues(new ArrayList<Integer>(subMap.values()));
		}

		@Test
		public void testMultipleThreadsWithElementsRemoved() throws Exception {
			
			final AtomicBoolean stop = new AtomicBoolean(false);
			final CyclicBarrier barrier = new CyclicBarrier(5);
			final List<Future<Map<Integer, Integer>>> futures = new ArrayList<Future<Map<Integer, Integer>>>();
			
			for (int i=0; i<5; i++) {
				futures.add(threadPool.submit(new Callable<Map<Integer, Integer>>() {

					@Override
					public Map<Integer, Integer> call() throws Exception {
						
						barrier.await();
						
						TestWorker worker = new TestWorker();
						
						while (!stop.get()) {
							worker.process();
						}
						
						return worker.map;
					}
				}));
			}
			
			Thread.sleep(200);
			
			List<Integer> newList = new ArrayList<Integer>(iList);
			
			final List<Integer> removedElements = new ArrayList<Integer>();
			removedElements.add(newList.remove(2));
			removedElements.add(newList.remove(5));
			removedElements.add(newList.remove(6));
			
			cList.swapWithList(newList);
			
			Thread.sleep(200);
			stop.set(true);
			
			Map<Integer, Integer> result = getTotalMap(futures);
			
			Map<Integer, Integer> subMap = CollectionUtils.filterKeys(result, new Predicate<Integer>() {

				@Override
				public boolean apply(Integer x) {
					return !removedElements.contains(x);
				}
			});
			
			checkValues(new ArrayList<Integer>(subMap.values()));
		}

		private class TestWorker {
			
			private final ConcurrentHashMap<Integer, Integer> map = new ConcurrentHashMap<Integer, Integer>();
			
			private void process() {
				
				Integer element = cList.getNextElement(); 
				Integer count = map.get(element);
				if (count == null) {
					map.put(element, 1);
				} else {
					map.put(element, count+1);
				}
			}
		}
		
		private static Map<Integer, Integer> getTotalMap(List<Future<Map<Integer, Integer>>> futures) throws InterruptedException, ExecutionException {
			
			Map<Integer, Integer> totalMap = new HashMap<Integer, Integer>();
			
			for (Future<Map<Integer, Integer>> f : futures) {
				
				Map<Integer, Integer> map = f.get();
				
				for (Integer element : map.keySet()) {
					Integer count = totalMap.get(element);
					if (count == null) {
						totalMap.put(element, map.get(element));
					} else {
						totalMap.put(element, map.get(element) + count);
					}
				}
			}
			return totalMap;
		}
		

		private static double checkValues(List<Integer> values) {
			
			System.out.println("Values: " + values);
			SummaryStatistics ss = new SummaryStatistics();
			for (int i=0; i<values.size(); i++) {
				ss.addValue(values.get(i));
			}
			
			double mean = ss.getMean();
			double stddev = ss.getStandardDeviation();
			
			double p = ((stddev*100)/mean);
			System.out.println("Percentage diff: " + p);
			
			Assert.assertTrue("" + p + " " + values, p<0.1);
			return p;
		}
	}
}

