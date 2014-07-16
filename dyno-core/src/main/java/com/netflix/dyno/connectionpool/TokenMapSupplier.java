package com.netflix.dyno.connectionpool;

import java.util.List;

import com.netflix.dyno.connectionpool.impl.lb.HostToken;

public interface TokenMapSupplier {

	public List<HostToken> getTokens();
}
