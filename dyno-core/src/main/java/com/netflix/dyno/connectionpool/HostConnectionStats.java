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

/**
 * Stats for connection operations for each {@code Host}
 * These are tracked by the {@link ConnectionPoolMonitor} for the {@link ConnectionPool}
 *
 * @author poberai
 */
public interface HostConnectionStats {

    /**
     * @return boolean
     */
    public boolean isHostUp();

    /**
     * @return long
     */
    public long getConnectionsBorrowed();

    /**
     * @return long
     */
    public long getConnectionsReturned();

    /**
     * @return long
     */
    public long getConnectionsCreated();

    /**
     * @return long
     */
    public long getConnectionsClosed();

    /**
     * @return long
     */
    public long getConnectionsCreateFailed();

    /**
     * @return long
     */
    public long getOperationSuccessCount();

    /**
     * @return long
     */
    public long getOperationErrorCount();
}

