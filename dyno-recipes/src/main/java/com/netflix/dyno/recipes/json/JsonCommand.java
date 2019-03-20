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
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.util.SafeEncoder;

@Getter
public enum JsonCommand implements ProtocolCommand {
    SET("JSON.SET"),
    GET("JSON.GET"),
    DEL("JSON.DEL"),
    TYPE("JSON.TYPE"),
    MGET("JSON.MGET"),
    ARRAPPEND("JSON.ARRAPPEND"),
    ARRINSERT("JSON.ARRINSERT"),
    ARRLEN("JSON.ARRLEN"),
    OBJKEYS("JSON.OBJKEYS"),
    OBJLEN("JSON.OBJLEN");

    private final byte[] raw;

    JsonCommand(String opName) {
        this.raw = SafeEncoder.encode(opName);
    }
}
