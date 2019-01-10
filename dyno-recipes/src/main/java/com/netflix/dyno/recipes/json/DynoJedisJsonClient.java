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

import com.google.gson.Gson;
import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.jedis.JedisGenericOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DynoJedisJsonClient {
    private static final Logger LOG = LoggerFactory.getLogger(DynoJedisJsonClient.class);
    private static final Gson gson = new Gson();
    private final DynoJedisClient client;

    public DynoJedisJsonClient(DynoJedisClient client) {
        this.client = client;
    }

    public OperationResult<String> set(String key, Object object) {
        return set(key, JsonPath.ROOT_PATH, object, ExistenceModifier.DEFAULT);
    }

    public OperationResult<String> set(String key, ExistenceModifier flag, Object object) {
        return set(key, JsonPath.ROOT_PATH, object, flag);
    }

    public OperationResult<String> set(String key, JsonPath path, Object object) {
        return set(key, path, object, ExistenceModifier.DEFAULT);
    }

    public OperationResult<String> set(String key, JsonPath path, Object object, ExistenceModifier flag) {
        return client.moduleCommand(new JedisGenericOperation<String>(key, JsonCommand.GET.toString()) {
            @Override
            public String execute(Jedis client, ConnectionContext state) throws DynoException {
                final List<byte[]> args = new ArrayList<>();

                args.add(SafeEncoder.encode(key));
                args.add(SafeEncoder.encode(path.toString()));
                args.add(SafeEncoder.encode(gson.toJson(object)));
                if (flag != ExistenceModifier.DEFAULT) {
                    args.add(flag.getRaw());
                }
                client.getClient().sendCommand(JsonCommand.SET, args.toArray(new byte[args.size()][]));

                return client.getClient().getStatusCodeReply();
            }
        });
    }

    public OperationResult<Object> get(String key) {
        return get(key, JsonPath.ROOT_PATH);
    }

    public OperationResult<Object> get(String key, JsonPath... paths) {
        return client.moduleCommand(new JedisGenericOperation<Object>(key, JsonCommand.GET.toString()) {
            @Override
            public Object execute(Jedis client, ConnectionContext state) throws DynoException {
                final List<byte[]> args = new ArrayList<>();

                args.add(SafeEncoder.encode(key));
                Arrays.stream(paths).forEach(x -> args.add(SafeEncoder.encode(x.toString())));
                client.getClient().sendCommand(JsonCommand.GET, args.toArray(new byte[args.size()][]));

                return gson.fromJson(client.getClient().getBulkReply(), Object.class);
            }
        });
    }

    public OperationResult<Long> del(String key) {
        return this.del(key, JsonPath.ROOT_PATH);
    }

    public OperationResult<Long> del(String key, JsonPath path) {
        return client.moduleCommand(new JedisGenericOperation<Long>(key, JsonCommand.DEL.toString()) {
            @Override
            public Long execute(Jedis client, ConnectionContext state) throws DynoException {
                final List<byte[]> args = new ArrayList<>();

                args.add(SafeEncoder.encode(key));
                args.add(SafeEncoder.encode(path.toString()));
                client.getClient().sendCommand(JsonCommand.DEL, args.toArray(new byte[args.size()][]));

                return client.getClient().getIntegerReply();
            }
        });
    }

    public OperationResult<Class<?>> type(String key) {
        return this.type(key, JsonPath.ROOT_PATH);
    }

    public OperationResult<Class<?>> type(String key, JsonPath path) {
        return client.moduleCommand(new JedisGenericOperation<Class<?>>(key, JsonCommand.DEL.toString()) {
            @Override
            public Class<?> execute(Jedis client, ConnectionContext state) throws DynoException {
                final List<byte[]> args = new ArrayList<>();

                args.add(SafeEncoder.encode(key));
                args.add(SafeEncoder.encode(path.toString()));
                client.getClient().sendCommand(JsonCommand.TYPE, args.toArray(new byte[args.size()][]));

                final String reply = client.getClient().getBulkReply();
                switch (reply) {
                    case "null":
                        return null;
                    case "boolean":
                        return boolean.class;
                    case "integer":
                        return int.class;
                    case "number":
                        return float.class;
                    case "string":
                        return String.class;
                    case "object":
                        return Object.class;
                    case "array":
                        return List.class;
                    default:
                        throw new java.lang.RuntimeException(reply);
                }
            }
        });
    }

    public OperationResult<List<Object>> mget(List<String> keys, JsonPath path) {
        return client.moduleCommand(new JedisGenericOperation<List<Object>>("", JsonCommand.MGET.toString()) {
            @Override
            public List<Object> execute(Jedis client, ConnectionContext state) throws DynoException {
                final List<byte[]> args = new ArrayList<>();

                keys.forEach(key -> args.add(SafeEncoder.encode(key)));
                args.add(SafeEncoder.encode(path.toString()));
                client.getClient().sendCommand(JsonCommand.MGET, args.toArray(new byte[args.size()][]));

                final List<String> reply = client.getClient().getMultiBulkReply();
                final List<Object> response = new ArrayList<>(reply.size());
                reply.forEach(r -> response.add(gson.fromJson(r, Object.class)));

                return response;
            }
        });
    }

    public OperationResult<Long> arrappend(String key, JsonPath path, Object... items) {
        return client.moduleCommand(new JedisGenericOperation<Long>(key, JsonCommand.ARRAPPEND.toString()) {
            @Override
            public Long execute(Jedis client, ConnectionContext state) throws DynoException {
                final List<byte[]> args = new ArrayList<>();

                args.add(SafeEncoder.encode(key));
                args.add(SafeEncoder.encode(path.toString()));
                Arrays.asList(items)
                        .forEach(i -> args.add(SafeEncoder.encode(gson.toJson(i))));
                client.getClient().sendCommand(JsonCommand.ARRAPPEND, args.toArray(new byte[args.size()][]));

                return client.getClient().getIntegerReply();
            }
        });
    }

    public OperationResult<Long> arrinsert(String key, JsonPath path, int index, Object... items) {
        return client.moduleCommand(new JedisGenericOperation<Long>(key, JsonCommand.ARRINSERT.toString()) {
            @Override
            public Long execute(Jedis client, ConnectionContext state) throws DynoException {
                final List<byte[]> args = new ArrayList<>();

                args.add(SafeEncoder.encode(key));
                args.add(SafeEncoder.encode(path.toString()));
                args.add(SafeEncoder.encode(Integer.toString(index)));
                Arrays.asList(items)
                        .forEach(i -> args.add(SafeEncoder.encode(gson.toJson(i))));
                client.getClient().sendCommand(JsonCommand.ARRINSERT, args.toArray(new byte[args.size()][]));

                return client.getClient().getIntegerReply();
            }
        });
    }

    public OperationResult<Long> arrlen(String key, JsonPath path) {
        return client.moduleCommand(new JedisGenericOperation<Long>(key, JsonCommand.ARRLEN.toString()) {
            @Override
            public Long execute(Jedis client, ConnectionContext state) throws DynoException {
                final List<byte[]> args = new ArrayList<>();

                args.add(SafeEncoder.encode(key));
                args.add(SafeEncoder.encode(path.toString()));
                client.getClient().sendCommand(JsonCommand.ARRLEN, args.toArray(new byte[args.size()][]));

                return client.getClient().getIntegerReply();
            }
        });
    }

    public OperationResult<List<String>> objkeys(String key) {
        return objkeys(key, JsonPath.ROOT_PATH);
    }

    public OperationResult<List<String>> objkeys(String key, JsonPath path) {
        return client.moduleCommand(new JedisGenericOperation<List<String>>(key, JsonCommand.OBJKEYS.toString()) {
            @Override
            public List<String> execute(Jedis client, ConnectionContext state) throws DynoException {
                final List<byte[]> args = new ArrayList<>();

                args.add(SafeEncoder.encode(key));
                args.add(SafeEncoder.encode(path.toString()));
                client.getClient().sendCommand(JsonCommand.OBJKEYS, args.toArray(new byte[args.size()][]));

                return client.getClient().getMultiBulkReply();
            }
        });
    }

    public OperationResult<Long> objlen(String key) {
        return objlen(key, JsonPath.ROOT_PATH);
    }

    public OperationResult<Long> objlen(String key, JsonPath path) {
        return client.moduleCommand(new JedisGenericOperation<Long>(key, JsonCommand.OBJLEN.toString()) {
            @Override
            public Long execute(Jedis client, ConnectionContext state) throws DynoException {
                final List<byte[]> args = new ArrayList<>();

                args.add(SafeEncoder.encode(key));
                args.add(SafeEncoder.encode(path.toString()));
                client.getClient().sendCommand(JsonCommand.OBJLEN, args.toArray(new byte[args.size()][]));

                return client.getClient().getIntegerReply();
            }
        });
    }
}
