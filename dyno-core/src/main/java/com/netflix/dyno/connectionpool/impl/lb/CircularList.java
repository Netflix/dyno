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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

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
	public synchronized T getNextElement() {
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
	public synchronized void addElement(T element) {
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
	public synchronized void removeElement(T element) {
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
		private final List<T> list;
		private final Integer size;

		// The rotating index over the list. currentIndex always indicates the index of the element that was last accessed
		// Using AtomicLong instead of AtomicInteger to avoid resetting value on overflow. Range of long is good enough
		// to not wrap currentIndex.
		private final AtomicLong currentIndex = new AtomicLong(0L);
		
		private InnerList(Collection<T> newList) {
			if (newList != null) {
				list = new ArrayList<>(newList);
				size = list.size();
			} else {
				list = null;
				size = 0;
			}
		}
		
		private int getNextIndex() {
			return (int) (currentIndex.incrementAndGet() % size);
		}

		private T getNextElement() {
			return (list == null || list.size() == 0) ?  null : list.get(getNextIndex());
		}
		
		private List<T> getList() {
			return list;
		}
	}
}

