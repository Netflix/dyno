package com.netflix.dyno.connectionpool;

import java.util.Collection;

public interface HostSupplier {

	public Collection<Host> getHosts();
}
