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


/**
 * Indicates that a thread waiting to obtain a connection from the pool has timed-out while waiting; it's likely
 * that all connections are busy servicing requests.
 */
public class PoolTimeoutException extends DynoConnectException implements IsRetryableException {

	private static final long serialVersionUID = -8579946319118318717L;

    public PoolTimeoutException(String message) {
        super(message);
    }

    public PoolTimeoutException(Throwable t) {
        super(t);
    }

    public PoolTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
}