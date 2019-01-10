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
package com.netflix.dyno.recipes.json;

import lombok.Getter;
import lombok.ToString;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

@ToString
@Getter
public enum ExistenceModifier implements ProtocolCommand {
    DEFAULT(""),
    NOT_EXISTS("NX"),
    MUST_EXIST("XX");

    private final byte[] raw;

    ExistenceModifier(String modifier) {
        this.raw = SafeEncoder.encode(modifier);
    }
}
