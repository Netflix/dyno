/*******************************************************************************
 * Copyright 2018 Netflix
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

package com.netflix.dyno.jedis;

import com.netflix.dyno.connectionpool.Operation;
import lombok.Getter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.SafeEncoder;

@Getter
public abstract class JedisGenericOperation<R> implements Operation<Jedis, R> {
    private final String name;
    private final String stringKey;
    private final byte[] binaryKey;

    public JedisGenericOperation(String key, String opName) {
        this.stringKey = key;
        this.binaryKey = SafeEncoder.encode(key);
        this.name = opName;
    }
}
