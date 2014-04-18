package com.netflix.dyno.connectionpool;


public interface HostConnectionStats {

	public boolean isHostUp();

	public long getConnectionsBorrowed();
	
	public long getConnectionsReturned();
	
	public long getConnectionsCreated();
	
	public long getConnectionsClosed();

	public long getConnectionsCreateFailed();

	public long getOperationSuccessCount();

	public long getOperationErrorCount();
}

