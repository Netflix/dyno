package com.netflix.dyno.connectionpool;

import java.util.List;

public interface HostSupplier {

	public List<Host> getHosts();
}
