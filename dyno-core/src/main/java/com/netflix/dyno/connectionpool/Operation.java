package com.netflix.dyno.connectionpool;

import com.netflix.dyno.connectionpool.exception.DynoException;



public interface Operation<CL, R> {
    /**
     * Execute the operation on the client object and return the results.
     * 
     * @param client - The client object
     * @param state  - State and metadata specific to the connection
     * @return
     * @throws DynoException
     */
    R execute(CL client, ConnectionContext state) throws DynoException;
}
