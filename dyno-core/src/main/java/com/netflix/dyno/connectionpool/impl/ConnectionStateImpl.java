package com.netflix.dyno.connectionpool.impl;

import java.util.concurrent.ConcurrentHashMap;

import com.netflix.dyno.connectionpool.ConnectionContext;

public class ConnectionStateImpl implements ConnectionContext {

	private final ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<String, Object>();
	
	@Override
	public void setMetadata(String key, Object obj) {
		map.put(key, obj);
	}

	@Override
	public Object getMetadata(String key) {
		return map.get(key);
	}

	@Override
	public boolean hasMetadata(String key) {
		return map.get(key) != null;
	}

}
