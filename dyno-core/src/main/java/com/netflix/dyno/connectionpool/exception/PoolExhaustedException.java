/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.connectionpool.exception;

import com.netflix.dyno.connectionpool.HostConnectionPool;

/**
 * Indicates there are no connections left in the host connection pool.
 */
public class PoolExhaustedException extends DynoConnectException {

    private final HostConnectionPool hcp;

	private static final long serialVersionUID = 9081993527008721028L;


	public PoolExhaustedException(HostConnectionPool hostConnectionPool, String message) {
		super(message);
        this.hcp = hostConnectionPool;
	}

    public PoolExhaustedException(HostConnectionPool hostConnectionPool, String message, Throwable cause) {
        super(message, cause);
        this.hcp = hostConnectionPool;
    }

    public PoolExhaustedException(HostConnectionPool hostConnectionPool, Throwable t) {
        super(t);
        this.hcp = hostConnectionPool;
    }

    public HostConnectionPool getHostConnectionPool() {
        return this.hcp;
    }
}

