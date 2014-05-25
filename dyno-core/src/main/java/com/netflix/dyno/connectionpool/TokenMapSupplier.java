package com.netflix.dyno.connectionpool;

import java.util.List;

public interface TokenMapSupplier {

	public List<HostToken> getTokens();
}
