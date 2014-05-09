package com.netflix.dyno.connectionpool;

public interface ConnectionObservor {

	public void connectionEstablished(Host host);

	public void connectionLost(Host host);
}
