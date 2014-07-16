package com.netflix.dyno.connectionpool;

import java.util.List;

import com.netflix.dyno.connectionpool.impl.lb.HostToken;

public interface HashPartitioner {

	public Long hash(int key);
	
	public Long hash(long key);
	
	public Long hash(String key);
	
	public HostToken getToken(List<HostToken> hostTokens, Long keyHash);
}
