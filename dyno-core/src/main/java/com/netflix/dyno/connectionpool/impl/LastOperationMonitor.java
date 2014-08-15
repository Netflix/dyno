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
package com.netflix.dyno.connectionpool.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.netflix.dyno.connectionpool.OperationMonitor;

/**
 * Simple in memory map based impl of {@link OperationMonitor}
 * Mainly used for testing. 
 * 
 * @author poberai
 *
 */
public class LastOperationMonitor implements OperationMonitor {

	private final ConcurrentHashMap<String, Long> latestTimings = new ConcurrentHashMap<String, Long>();
	private final ConcurrentHashMap<String, AtomicInteger> opCounters = new ConcurrentHashMap<String, AtomicInteger>();
	private final ConcurrentHashMap<String, AtomicInteger> opFailureCounters = new ConcurrentHashMap<String, AtomicInteger>();
	
	@Override
	public void recordLatency(String opName, long duration, TimeUnit unit) {
		latestTimings.put(opName, TimeUnit.MILLISECONDS.convert(duration, unit));
	}

	@Override
	public void recordSuccess(String opName) {
		AtomicInteger count = opCounters.get(opName);
		if (count == null) {
			opCounters.put(opName, new AtomicInteger(1));
		} else {
			count.incrementAndGet();
		}
	}

	@Override
	public void recordFailure(String opName, String reason) {
		AtomicInteger count = opFailureCounters.get(opName);
		if (count == null) {
			opFailureCounters.put(opName, new AtomicInteger(1));
		} else {
			count.incrementAndGet();
		}
	}

}
