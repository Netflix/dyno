package com.netflix.dyno.connectionpool;

import java.util.Date;

public interface HostConnectionStats {

	public boolean isHostUp();

	public Date getPoolCreationTime();

	public long getConnectionsBorrowed();
	
	public long getConnectionsReturned();
	
	public long getConnectionsCreated();
	
	public long getConnectionsClosed();

	public long getConnectionsCreateFailed();

	public long getOperationSuccessCount();

	public long getOperationErrorCount();
}

