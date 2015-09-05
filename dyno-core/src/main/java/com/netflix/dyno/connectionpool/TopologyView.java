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

import java.util.List;
import java.util.Map;

/**
 * Represents a read-only view of the dynomite server topology.
 */
public interface TopologyView {

    /**
     * Retrieves a read-only view of the server topology
     *
     * @return An unmodifiable map of server-id to list of token status
     */
    Map<String, List<TokenPoolTopology.TokenStatus>> getTopologySnapshot();

    /**
     * Returns the token for the given key.
     *
     * @param key The key of the record stored in dynomite
     * @return Long The token that owns the given key
     */
    Long getTokenForKey(String key);

}
