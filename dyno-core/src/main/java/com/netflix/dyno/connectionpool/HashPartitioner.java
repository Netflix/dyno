package com.netflix.dyno.connectionpool;

import java.util.List;

public interface HashPartitioner {

	public Long hash(int key);
	
	public Long hash(long key);
	
	public Long hash(String key);
	
	public HostToken getToken(List<HostToken> hostTokens, Long keyHash);
}
