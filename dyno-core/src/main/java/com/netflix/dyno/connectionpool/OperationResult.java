package com.netflix.dyno.connectionpool;

import java.util.concurrent.TimeUnit;

public interface OperationResult<R> {
    /**
     * @return Get the host on which the operation was performed
     */
    Host getNode();

    /**
     * @return Get the result data
     */
    R getResult();

    /**
     * @return Return the length of time to perform the operation. Does not include
     * connection pool overhead. This time is in nanoseconds
     */
    long getLatency();

    /**
     * @return Return the length of time to perform the operation to the remote service. Does not include
     * connection pool overhead.
     * 
     * @param units
     */
    long getLatency(TimeUnit units);

    /**
     * @return Return the number of times the operation had to be retried. This includes
     * retries for aborted connections.
     */
    int getAttemptsCount();

    /**
     * Set the number of attempts executing this connection
     * @param count
     */
    void setAttemptsCount(int count);
    
    void setLatency(long duration, TimeUnit unit);
}