package com.netflix.dyno.connectionpool;

import java.util.concurrent.Future;

import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;

/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

/**
 * Interface to an instance of a connection on a host.
 * 
 * @author poberai
 * 
 * @param <CL>
 */
public interface Connection<CL> {

    /**
     * Execute an operation on the connection and return a result
     * 
     * @param <R>
     * @param op
     * @throws DynoException
     */
    public <R> OperationResult<R> execute(Operation<CL, R> op) throws DynoException;
    
    public <R> Future<OperationResult<R>> executeAsync(AsyncOperation<CL, R> op) throws DynoException;

    /**
     * Shut down the connection. isOpen() will now return false.
     */
    public void close();

    /**
     * @return Get the host for this connection
     */
    public Host getHost();

    /**
     * Open a new connection
     * 
     * @throws DynoException
     */
    public void open() throws DynoException;

    /**
     * Can be used by clients to indicate connection exception. 
     * This can be analyzed by connection pools later
     * e.g remove host from connection pool etc. 
     * 
     * @return
     */
    public DynoConnectException getLastException();
    
    /**
     * 
     * @return
     */
    public HostConnectionPool<CL> getParentConnectionPool();
}
