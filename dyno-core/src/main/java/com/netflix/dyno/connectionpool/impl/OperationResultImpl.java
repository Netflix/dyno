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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.OperationMonitor;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.exception.DynoException;

/**
 * Impl for {@link OperationResult}
 * It tracks operation result, op attempts, latency, execution host etc
 * 
 * @author poberai
 *
 * @param <R>
 */
public class OperationResultImpl<R> implements OperationResult<R> {

	private final String opName;
	private final R result; 
	private final Future<R> futureResult;
	private Host host = null;
	private long duration = 0;
	private int attempts = 0;
	private final OperationMonitor opMonitor; 
	private final ConcurrentHashMap<String, String> metadata = new ConcurrentHashMap<String, String>();
	
	public OperationResultImpl(String name, R r, OperationMonitor monitor) {
		opName = name;
		result = r;
		futureResult = null;
		opMonitor = monitor;
	}
	
	public OperationResultImpl(String name, Future<R> future, OperationMonitor monitor) {
		opName = name;
		result = null;
		futureResult = future;
		opMonitor = monitor;
	}

	@Override
	public Host getNode() {
		return host;
	}

	@Override
	public R getResult() {
		try {
			return futureResult != null ? futureResult.get() : result;
		} catch (Exception e) {
			throw new DynoException(e);
		}
	}

	@Override
	public long getLatency() {
		return duration;
	}

	@Override
	public long getLatency(TimeUnit units) {
		return units.convert(duration, TimeUnit.MILLISECONDS);
	}

	@Override
	public int getAttemptsCount() {
		return attempts;
	}

	@Override
	public OperationResultImpl<R> setAttemptsCount(int count) {
		attempts = count;
		return this;
	}

	public OperationResultImpl<R> setNode(Host h) {
		host = h;
		return this;
	}
	
	public OperationResultImpl<R> attempts(int count) {
		attempts = count;
		return this;
	}
	
	public OperationResultImpl<R> latency(long time) {
		this.duration = time;
		if (opMonitor != null) {
			opMonitor.recordLatency(opName, time, TimeUnit.MILLISECONDS);
		}
		return this;
	}
	
	@Override
	public OperationResultImpl<R> setLatency(long time, TimeUnit unit) {
		this.duration = TimeUnit.MILLISECONDS.convert(time, unit);
		if (opMonitor != null) {
			opMonitor.recordLatency(opName, time, unit);
		}
		return this;
	}

	@Override
	public Map<String, String> getMetadata() {
		return metadata;
	}

	@Override
	public OperationResultImpl<R> addMetadata(String key, String value) {
		metadata.put(key, value);
		return this;
	}

	@Override
	public OperationResultImpl<R> addMetadata(Map<String, Object> map) {
		for (String key : map.keySet()) {
			metadata.put(key, map.get(key).toString());
		}
		return this;
	}
}
