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
package com.netflix.dyno.connectionpool;

import com.netflix.dyno.connectionpool.exception.DynoException;

/**
 * Interface that represents a synchronous operation on the connection
 * @author poberai
 *
 * @param <CL>
 * @param <R>
 */
public interface Operation<CL, R> extends BaseOperation<CL, R> {
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
