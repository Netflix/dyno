package com.netflix.dyno.connectionpool.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.netflix.dyno.connectionpool.ConnectionContext;

public class ConnectionContextImpl implements ConnectionContext {

	private final ConcurrentHashMap<String, Object> context = new ConcurrentHashMap<String, Object>();
	
	@Override
	public void setMetadata(String key, Object obj) {
		context.put(key, obj);
	}

	@Override
	public Object getMetadata(String key) {
		return context.get(key);
	}

	@Override
	public boolean hasMetadata(String key) {
		return context.containsKey(key);
	}

	@Override
	public void reset() {
		context.clear();
	}

	@Override
	public Map<String, Object> getAll() {
		return context;
	}

}
