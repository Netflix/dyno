package com.netflix.dyno.connectionpool;

public interface BaseOperation<CL, R> {

	public String getName();
	
	public String getKey();
}
