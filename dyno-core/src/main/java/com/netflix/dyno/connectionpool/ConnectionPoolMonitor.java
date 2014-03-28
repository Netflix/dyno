package com.netflix.dyno.connectionpool;


import java.util.Map;

/**
 * Monitoring interface to receive notification of pool events. A concrete
 * monitor will make event stats available to a monitoring application and may
 * also log events to a log file.
 * 
 * @author poberai
 */
public interface ConnectionPoolMonitor {
	
    /**
     * Succeeded in executing an operation
     * 
     * @param host
     * @param latency
     */
	public void incOperationSuccess(Host host, long latency);

	public long getOperationSuccessCount();
    
    /**
     * Errors trying to execute an operation.
     * 
     * @param reason
     * @param host
     */
	public void incOperationFailure(Host host, Exception reason);

	public long getOperationFailureCount();

    
    /**
     * An operation failed but the connection pool will attempt to fail over to
     * another host/connection.  
     */
    public void incFailover(Host host, Exception reason);

    public long getFailoverCount();

   
    /**
     * Created a connection successfully
     */
    public void incConnectionCreated(Host host);

    public long getConnectionCreatedCount();

    /**
     * Closed a connection
     * 
     * @param reason
     *           
     */
    public void incConnectionClosed(Host host, Exception reason);

    public long getConnectionClosedCount();

    /**
     * Attempt to create a connection failed
     * 
     * @param host
     * @param reason
     */
    public void incConnectionCreateFailed(Host host, Exception reason);

    public long getConnectionCreateFailedCount();

    /**
     * Incremented for each connection borrowed
     * 
     * @param host
     *            Host from which the connection was borrowed
     * @param delay
     *            Time spent in the connection pool borrowing the connection
     */
    public void incConnectionBorrowed(Host host, long delay);

    public long getConnectionBorrowedCount();

    /**
     * Incremented for each connection returned.
     * 
     * @param host
     *            Host to which connection is returned
     */
    public void incConnectionReturned(Host host);

    public long getConnectionReturnedCount();

    /**
     * Timeout trying to get a connection from the pool
     */
    public long getPoolExhaustedTimeoutCount();

    /**
     * Timeout waiting for a response from the cluster
     */
    public long getOperationTimeoutCount();

    /**
     * @return Count of socket timeouts trying to execute an operation
     */
    public long getSocketTimeoutCount();
    
    /**
     * @return Get number of unknown errors
     */
    public long getUnknownErrorCount();
    
    /**
     * @return Get number of invalid requests (i.e. bad argument values)
     */
    public long getBadRequestCount();
    
    /**
     * @return Count of times no hosts at all were available to execute an operation.
     */
    public long getNoHostCount();
    
    /**
     * @return Number of times operations were cancelled 
     */
    public long getInterruptedCount();

    /**
     * @return Return the number of hosts in the pool
     */
    public long getHostCount();
    
    /**
     * Return the number of times a host was added to the pool.  This
     * number will be incremented multiple times if the same hosts is 
     * added and removed multiple times.  
     * A constantly increasing number of host added and host removed
     * may indicate a problem with the host discovery service
     */
    public long getHostAddedCount();
    
    /**
     * Return the number of times any host was removed to the pool.  This
     * number will be incremented multiple times if the same hosts is 
     * added and removed multiple times.  
     * A constantly increating number of host added and host removed
     * may indicate a problem with the host discovery service
     */
    public long getHostRemovedCount();
    
    /**
     * @return Return the number of times any host was marked as down.
     */
    public long getHostDownCount();

    /**
     * @return Return the number of active hosts
     */
    public long getHostUpCount();

    /**
     * A host was added and given the associated pool. The pool is immutable and
     * can be used to get info about the number of open connections
     * 
     * @param host
     * @param pool
     */
    public void hostAdded(Host host, HostConnectionPool<?> pool);

    /**
     * A host was removed from the pool. This is usually called when a downed
     * host is removed from the ring.
     * 
     * @param host
     */
    public void hostRemoved(Host host);

    /**
     * A host was identified as downed.
     * 
     * @param host
     * @param reason
     *            Exception that caused the host to be identified as down
     */
    public void hostDown(Host host, Exception reason);

    /**
     * A host was reactivated after being marked down
     * 
     * @param host
     * @param pool
     */
    public void hostUp(Host host, HostConnectionPool<?> pool);

    /**
     * @return Return a mapping of all hosts and their statistics
     */
    public Map<Host, HostConnectionStats> getHostStats();

}