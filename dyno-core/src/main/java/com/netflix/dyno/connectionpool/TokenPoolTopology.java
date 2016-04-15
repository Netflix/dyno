package com.netflix.dyno.connectionpool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class TokenPoolTopology {

	private final ConcurrentHashMap<String, List<TokenStatus>> map = new ConcurrentHashMap<String, List<TokenStatus>>();
	private final int replicationFactor;
	
	public TokenPoolTopology (int replicationFactor) {
		this.replicationFactor = replicationFactor;
	}
	
	public void addToken(String rack, Long token, HostConnectionPool<?> hostPool) {
		
		List<TokenStatus> list = map.get(rack);
		if (list == null) {
			list = new ArrayList<TokenStatus>();
			map.put(rack, list);
		}
		
		list.add(new TokenStatus(token, hostPool));
	}
	
	public ConcurrentHashMap<String, List<TokenStatus>> getAllTokens() {
		return map;
	}

	public int getReplicationFactor() {
		return replicationFactor;
	}

	public String toString() {
		
		ArrayList<String> keyList = new ArrayList<String>(map.keySet());
		Collections.sort(keyList);
		
		StringBuilder sb = new StringBuilder();
		sb.append("TokenPoolTopology\n");
		
		for (String key : keyList) {
			sb.append("\nRack: " + key + "\n");
			List<TokenStatus> list = map.get(key);
			Collections.sort(list);
			for (TokenStatus token : list) {
				sb.append(token.toString()).append("\n");
			}
		}
		
		return sb.toString();
	}
	
	public static class TokenStatus implements Comparable<TokenStatus> {
		
		private Long token; 
		private HostConnectionPool<?> hostPool;
		
		private TokenStatus(Long t, HostConnectionPool<?> pool) {
			token = t;
			hostPool = pool;
		}
		
		public Long getToken() {
			return token;
		}
		
		public HostConnectionPool<?> getHostPool() {
			return hostPool;
		}
		
		@Override
		public int compareTo(TokenStatus o) {
			return this.token.compareTo(o.token);
		} 
		
		public String toString() {
			return token + " ==> " + hostPool.toString();
		}
	}
	
}
