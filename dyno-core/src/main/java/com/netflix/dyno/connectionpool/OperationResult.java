package com.netflix.dyno.connectionpool;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface OperationResult<R> {
    /**
     * @return Get the host on which the operation was performed
     */
    public Host getNode();
    
    /**
     * @param node
     */
    public OperationResult<R> setNode(Host node);

    /**
     * @return Get the result data
     */
    public R getResult();

    /**
     * @return Return the length of time to perform the operation. Does not include
     * connection pool overhead. This time is in nanoseconds
     */
    public long getLatency();

    /**
     * @return Return the length of time to perform the operation to the remote service. Does not include
     * connection pool overhead.
     * 
     * @param units
     */
    public long getLatency(TimeUnit units);

    /**
     * @return Return the number of times the operation had to be retried. This includes
     * retries for aborted connections.
     */
    public int getAttemptsCount();

    /**
     * Set the number of attempts executing this connection
     * @param count
     */
    public OperationResult<R> setAttemptsCount(int count);
    
    /**
     * Set latency after executing the operation. This method is useful to apps that do async operations
     * and can proxy back latency stats to Dyno once they receive their result via the future.
     * 
     * @param duration
     * @param unit
     */
    public OperationResult setLatency(long duration, TimeUnit unit);
    
    /**
     * Method that returns any other metadata that is associated with this OperationResult.
     * 
     * @return Map<String, String>
     */
    public Map<String, String> getMetadata();
    
    /**
     * Add metadata to the OperationResult. Can be used within different layers of Dyno to add metadata about each layer. 
     * Very useful for providing insight into the operation when debugging.
     * @param key
     * @param value
     */
    public OperationResult<R> addMetadata(String key, String value);

    /**
     * Add metadata to the OperationResult. Can be used within different layers of Dyno to add metadata about each layer. 
     * Very useful for providing insight into the operation when debugging.
     * @param Map<String, Object>
     */
    public OperationResult<R> addMetadata(Map<String, Object> map);
}