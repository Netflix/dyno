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
package com.netflix.dyno.jedis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import com.netflix.discovery.DiscoveryClient;
import com.netflix.discovery.EurekaClient;
import com.netflix.dyno.connectionpool.CompressionOperation;
import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.ConnectionPoolConfiguration;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.CursorBasedResult;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.MultiKeyCompressionOperation;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.TokenRackMapper;
import com.netflix.dyno.connectionpool.TopologyView;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.ZipUtils;
import com.netflix.dyno.contrib.ArchaiusConnectionPoolConfiguration;
import com.netflix.dyno.contrib.DynoCPMonitor;
import com.netflix.dyno.contrib.DynoOPMonitor;
import com.netflix.dyno.contrib.EurekaHostsSupplier;
import com.netflix.dyno.jedis.operation.BaseKeyOperation;
import com.netflix.dyno.jedis.operation.MultiKeyOperation;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import redis.clients.jedis.*;
import redis.clients.jedis.commands.BinaryJedisCommands;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.commands.MultiKeyBinaryCommands;
import redis.clients.jedis.commands.MultiKeyCommands;
import redis.clients.jedis.commands.ScriptingCommands;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.params.ZIncrByParams;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.netflix.dyno.connectionpool.ConnectionPoolConfiguration.CompressionStrategy;

public class DynoJedisClient implements JedisCommands, BinaryJedisCommands, MultiKeyCommands,
        ScriptingCommands, MultiKeyBinaryCommands, DynoJedisCommands {

    private static final Logger Logger = org.slf4j.LoggerFactory.getLogger(DynoJedisClient.class);
    private static final String DYNO_EXIPREHASH_METADATA_KEYPREFIX = "_metadata:";

    private final String appName;
    private final String clusterName;
    private final ConnectionPool<Jedis> connPool;
    private final AtomicReference<DynoJedisPipelineMonitor> pipelineMonitor = new AtomicReference<DynoJedisPipelineMonitor>();

    protected final DynoOPMonitor opMonitor;

    protected final ConnectionPoolMonitor cpMonitor;

    public DynoJedisClient(String name, String clusterName, ConnectionPool<Jedis> pool, DynoOPMonitor operationMonitor,
                           ConnectionPoolMonitor cpMonitor) {
        this.appName = name;
        this.clusterName = clusterName;
        this.connPool = pool;
        this.opMonitor = operationMonitor;
        this.cpMonitor = cpMonitor;
    }

    public ConnectionPoolImpl<Jedis> getConnPool() {
        return (ConnectionPoolImpl<Jedis>) connPool;
    }

    public String getApplicationName() {
        return appName;
    }

    public String getClusterName() {
        return clusterName;
    }

    /**
     * The following commands are supported
     *
     * <ul>
     * <lh>String Operations</lh>
     * <li>{@link #get(String) GET}</li>
     * <li>{@link #getSet(String, String) GETSET}</li>
     * <li>{@link #set(String, String) SET}</li>
     * <li>{@link #setex(String, int, String) SETEX}</li>
     * <li>{@link #psetex(String, long, String) PSETEX)</li>
     * </ul>
     * <ul>
     * <lh>Hash Operations</lh>
     * <li>{@link #hget(String, String) HGET}</li>
     * <li>{@link #hgetAll(String) HGETALL}</li>
     * <li>{@link #hmget(String, String...) HMGET}</li>
     * <li>{@link #hmset(String, Map) HMSET}</li>
     * <li>{@link #hscan(String, String) HSCAN}</li>
     * <li>{@link #hset(String, String, String) HSET}</li>
     * <li>{@link #hsetnx(String, String, String) HSETNX}</li>
     * <li>{@link #hvals(String) HVALS}</li>
     * </ul>
     *
     * <ul>
     * <li>{@link #get(byte[]) GET}</li>
     * <li>{@link #set(byte[], byte[]) SET}</li>
     * <li>{@link #setex(byte[], int, byte[]) SETEX}</li>
     * </ul>
     *
     * @param <T> the parameterized type
     */
    private abstract class CompressionValueOperation<T> extends BaseKeyOperation<T>
            implements CompressionOperation<Jedis, T> {

        private CompressionValueOperation(String k, OpName o) {
            super(k, o);
        }

        private CompressionValueOperation(byte[] k, OpName o) {
            super(k, o);
        }

        /**
         * Compresses the value based on the threshold defined by
         * {@link ConnectionPoolConfiguration#getValueCompressionThreshold()}
         *
         * @param value
         * @return
         */
        @Override
        public String compressValue(String value, ConnectionContext ctx) {
            String result = value;
            int thresholdBytes = connPool.getConfiguration().getValueCompressionThreshold();

            try {
                // prefer speed over accuracy here so rather than using
                // getBytes() to get the actual size
                // just estimate using 2 bytes per character
                if ((2 * value.length()) > thresholdBytes) {
                    result = ZipUtils.compressStringToBase64String(value);
                    ctx.setMetadata("compression", true);
                }
            } catch (IOException e) {
                Logger.warn(
                        "UNABLE to compress [" + value + "] for key [" + getStringKey() + "]; sending value uncompressed");
            }

            return result;
        }

        @Override
        public byte[] compressValue(byte[] value, ConnectionContext ctx) {
            byte[] result = value;
            int thresholdBytes = connPool.getConfiguration().getValueCompressionThreshold();

            try {
                // prefer speed over accuracy here so rather than using
                // getBytes() to get the actual size
                // just estimate using 2 bytes per character
                if ((2 * value.length) > thresholdBytes) {
                    result = ZipUtils.compressBytesNonBase64(value);
                    ctx.setMetadata("compression", true);
                }
            } catch (IOException e) {
                Logger.warn(
                  "UNABLE to compress [" + value + "] for key [" + getStringKey() + "]; sending value uncompressed");
            }

            return result;
        }
        @Override
        public String decompressValue(String value, ConnectionContext ctx) {
            try {
                if (ZipUtils.isCompressed(value)) {
                    ctx.setMetadata("decompression", true);
                    return ZipUtils.decompressFromBase64String(value);
                }
            } catch (IOException e) {
                Logger.warn("Unable to decompress value [" + value + "]");
            }

            return value;
        }

        @Override
        public byte[] decompressValue(byte[] value, ConnectionContext ctx) {
            try {
                if (ZipUtils.isCompressed(value)) {
                    ctx.setMetadata("decompression", true);
                    return ZipUtils.decompressBytesNonBase64(value);
                }
            } catch (IOException e) {
                Logger.warn("Unable to decompress value [" + Arrays.toString(value) + "]");
            }

            return value;
        }

    }

    /**
     * The following commands are supported
     *
     * <ul>
     * <lh>String Operations</lh>
     * <li>{@link #mget(String...) MGET}</li>
     * <li>{@link #mset(String...) MSET}</li>
     * <li>{@link #msetnx(String...) MSETNX}</li>
     * </ul>
     *
     * @param <T> the parameterized type
     */
    private abstract class CompressionValueMultiKeyOperation<T> extends MultiKeyOperation<T>
            implements MultiKeyCompressionOperation<Jedis, T> {

        private CompressionValueMultiKeyOperation(List keys, OpName o) {
            super(keys, o);
        }

        /**
         * Accepts a set of keys and values and compresses the value based on the threshold defined by
         * {@link ConnectionPoolConfiguration#getValueCompressionThreshold()}
         *
         * @param ctx and keyValues
         * @return
         */
        @Override
        public String[] compressMultiKeyValue(ConnectionContext ctx, String... keyValues) {
            List<String> items = Arrays.asList(keyValues);
            List<String> newItems = new ArrayList<String>();

            for (int i = 0; i < items.size(); i++) {
                /*
                 * String... keyValues is a List of keys and values.
                 * The value always comes second and this is the one
                 * we want to compress.
                 */
                if (i % 2 == 0) {
                    String value = items.get(i);

                    try {
                        if ((2 * value.length()) > connPool.getConfiguration().getValueCompressionThreshold()) {
                            newItems.add(i, ZipUtils.compressStringToBase64String(value));
                            ctx.setMetadata("compression", true);
                        }

                    } catch (IOException e) {
                        Logger.warn(
                                "UNABLE to compress [" + value + "] for key [" + getStringKey() + "]; sending value uncompressed");
                    }
                } else {
                    newItems.add(items.get(i));
                }
            }
            return (String[]) newItems.toArray();
        }

        @Override
        public byte[][] compressMultiKeyValue(ConnectionContext ctx, byte[]... keyValues) {
            List<byte[]> items = Arrays.asList(keyValues);
            List<byte[]> newItems = new ArrayList<>();

            for (int i = 0; i < items.size(); i++) {
                /*
                 * byte[]... keyValues is a List of keys and values.
                 * The value always comes second and this is the one
                 * we want to compress.
                 */
                if (i % 2 == 0) {
                    byte[] value = items.get(i);

                    try {
                        if ((2 * value.length) > connPool.getConfiguration().getValueCompressionThreshold()) {
                            newItems.add(i, ZipUtils.compressBytesNonBase64(value));
                            ctx.setMetadata("compression", true);
                        }

                    } catch (IOException e) {
                        Logger.warn(
                          "UNABLE to compress [" + Arrays.toString(value) + "] for key ["
                              + Arrays.toString(getBinaryKey()) + "]; sending value uncompressed");
                    }
                } else {
                    newItems.add(items.get(i));
                }
            }
            return (byte[][]) newItems.toArray();
        }

        @Override
        public String decompressValue(ConnectionContext ctx, String value) {
            try {
                if (ZipUtils.isCompressed(value)) {
                    ctx.setMetadata("decompression", true);
                    return ZipUtils.decompressFromBase64String(value);
                }
            } catch (IOException e) {
                Logger.warn("Unable to decompress value [" + value + "]");
            }

            return value;
        }

        @Override
        public byte[] decompressValue(ConnectionContext ctx, byte[] value) {
            try {
                if (ZipUtils.isCompressed(value)) {
                    ctx.setMetadata("decompression", true);
                    return ZipUtils.decompressBytesNonBase64(value);
                }
            } catch (IOException e) {
                Logger.warn("Unable to decompress value [" + value + "]");
            }

            return value;
        }

    }

    public TopologyView getTopologyView() {
        return this.getConnPool();
    }

    public <R> OperationResult<R> moduleCommand(JedisGenericOperation<R> handler) {
        return connPool.executeWithFailover(handler);
    }

    protected <T> T execOp(String key, OpName opName, Function<Jedis, T> fn) {
        return connPool.executeWithFailover(new BaseKeyOperation<T>(key, opName) {
            @Override
            public T execute(Jedis client, ConnectionContext state) throws DynoException {
                return fn.apply(client);
            }
        }).getResult();
    }

    protected <T> T execOp(byte[] key, OpName opName, Function<Jedis, T> fn) {
        return connPool.executeWithFailover(new BaseKeyOperation<T>(key, opName) {
            @Override
            public T execute(Jedis client, ConnectionContext state) throws DynoException {
                return fn.apply(client);
            }
        }).getResult();
    }

    protected <T> T execMultiOp(String[] keys, OpName opName, Function<Jedis, T> fn) {
        return connPool.executeWithFailover(new MultiKeyOperation<T>(Arrays.asList(keys), opName) {
            @Override
            public T execute(Jedis client, ConnectionContext state) throws DynoException {
                return fn.apply(client);
            }
        }).getResult();
    }

    protected <T> T execMultiOp(byte[][] keys, OpName opName, Function<Jedis, T> fn) {
        return connPool.executeWithFailover(new MultiKeyOperation<T>(Arrays.asList(keys), opName) {
            @Override
            public T execute(Jedis client, ConnectionContext state) throws DynoException {
                return fn.apply(client);
            }
        }).getResult();
    }

    @FunctionalInterface
    interface CompressionFunction<Jedis, ConnectionContext, CompressionOperation, T> {
        T apply(Jedis jedis, ConnectionContext state, CompressionOperation op);
    }

    protected <T> T execCompressOp(String key, OpName opName,
                                 CompressionFunction<Jedis, ConnectionContext, CompressionOperation, T> fn) {
        return connPool.executeWithFailover(new CompressionValueOperation<T>(key, opName) {
            @Override
            public T execute(final Jedis client, final ConnectionContext state) throws DynoException {
                return fn.apply(client, state, this);
            }
        }).getResult();
    }

    protected <T> T execCompressOp(byte[] key, OpName opName,
                                 CompressionFunction<Jedis, ConnectionContext, CompressionOperation, T> fn) {
        return connPool.executeWithFailover(new CompressionValueOperation<T>(key, opName) {
            @Override
            public T execute(final Jedis client, final ConnectionContext state) throws DynoException {
                return fn.apply(client, state, this);
            }
        }).getResult();
    }

    @FunctionalInterface
    interface MultiKeyCompressionFunction<Jedis, ConnectionContext, MultiKeyCompressionOperation, T> {
        T apply(Jedis jedis, ConnectionContext state, MultiKeyCompressionOperation op);
    }

    protected <T> T execCompressMultiOp(String[] keys, OpName opName,
                                      MultiKeyCompressionFunction<Jedis, ConnectionContext, MultiKeyCompressionOperation, T> fn) {
        return connPool.executeWithFailover(new CompressionValueMultiKeyOperation<T>(Arrays.asList(keys), opName) {
            @Override
            public T execute(final Jedis client, final ConnectionContext state) throws DynoException {
                return fn.apply(client, state, this);
            }
        }).getResult();
    }

    protected <T> T execCompressMultiOp(byte[][] keys, OpName opName,
                                      MultiKeyCompressionFunction<Jedis, ConnectionContext, MultiKeyCompressionOperation, T> fn) {
        return connPool.executeWithFailover(new CompressionValueMultiKeyOperation<T>(Arrays.asList(keys), opName) {
            @Override
            public T execute(final Jedis client, final ConnectionContext state) throws DynoException {
                return fn.apply(client, state, this);
            }
        }).getResult();
    }


    /******************* Jedis String Commands **************/

    @Override
    public Long append(final String key, final String value) {
        return execOp(key, OpName.APPEND, client -> client.append(key, value));
    }

    @Override
    public Long decr(final String key) {
        return execOp(key, OpName.DECR, client -> client.decr(key));
    }

    @Override
    public Long decrBy(final String key, final long delta) {
        return execOp(key, OpName.DECRBY, client -> client.decrBy(key, delta));
    }

    @Override
    public Long del(final String key) {
        return execOp(key, OpName.DEL, client -> client.del(key));
    }

    @Override
    public Long unlink(String key) {
        return execOp(key, OpName.UNLINK, client -> client.unlink(key));
    }

    public byte[] dump(final String key) {
        return execOp(key, OpName.DUMP, client -> client.dump(key));
    }

    @Override
    public String restore(String key, int ttl, byte[] serializedValue) {
        return execOp(key, OpName.RESTORE, client -> client.restore(key, ttl, serializedValue));
    }

    @Override
    public String restoreReplace(String key, int ttl, byte[] serializedValue) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Boolean exists(final String key) {
        return execOp(key, OpName.EXISTS, client -> client.exists(key));
    }

    @Override
    public Long expire(final String key, final int seconds) {
        return execOp(key, OpName.EXPIRE, client -> client.expire(key, seconds));
    }

    @Override
    public Long expireAt(final String key, final long unixTime) {
        return execOp(key, OpName.EXPIREAT, client -> client.expireAt(key, unixTime));
    }

    @Override
    public Object eval(String script, int keyCount, String... params) {
        if (keyCount == 0) {
            throw new DynoException("Need at least one key in script");
        }
        return execOp(params[0], OpName.EVAL, client -> client.eval(script, keyCount, params));
    }

    @Override
    public Object eval(String script, List<String> keys, List<String> args) {
        String[] params = ArrayUtils.addAll(keys.toArray(new String[0]), args.toArray(new String[0]));
        return eval(script, keys.size(), params);
    }

    @Override
    public Object eval(String script) {
        return eval(script, 0);
    }

    @Override
    public Object evalsha(String sha1, int keyCount, String... params) {
        if (keyCount == 0) {
            throw new DynoException("Need at least one key in script");
        }
        return execOp(params[0], OpName.EVALSHA, client -> client.evalsha(sha1, keyCount, params));
    }

    @Override
    public Object evalsha(String sha1, List<String> keys, List<String> args) {
        String[] params = ArrayUtils.addAll(keys.toArray(new String[0]), args.toArray(new String[0]));
        return evalsha(sha1, keys.size(), params);
    }

    @Override
    public Object evalsha(String sha1) {
        return evalsha(sha1, 0);
    }

    @Override
    public Boolean scriptExists(String sha1) {
        return execOp(sha1, OpName.SCRIPT_EXISTS, client -> client.scriptExists(sha1));
    }

    @Override
    public List<Boolean> scriptExists(String... sha1) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String scriptLoad(String script) {
        return execOp(script, OpName.SCRIPT_LOAD, client -> client.scriptLoad(script));
    }

    public String scriptFlush() {
        return execOp("", OpName.SCRIPT_FLUSH, client -> client.scriptFlush());
    }

    public String scriptKill() {
        return execOp("", OpName.SCRIPT_KILL, client -> client.scriptKill());
    }

    @Override
    public String get(final String key) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.GET, client -> client.get(key));
        } else {
            return execCompressOp(key, OpName.GET, (client, state, op) -> op.decompressValue(client.get(key), state));
        }
    }

    @Override
    public Boolean getbit(final String key, final long offset) {
        return execOp(key, OpName.GETBIT, client -> client.getbit(key, offset));
    }

    @Override
    public String getrange(final String key, final long startOffset, final long endOffset) {
        return execOp(key, OpName.GETRANGE, client -> client.getrange(key, startOffset, endOffset));
    }

    @Override
    public String getSet(final String key, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.GETSET, client -> client.getSet(key, value));
        } else {
            return execCompressOp(
              key,
              OpName.GETSET,
              (client, state, op) -> op.decompressValue(client.getSet(key, op.compressValue(value, state)), state)
            );
        }
    }

    @Override
    public Long hdel(final String key, final String... fields) {
        return execOp(key, OpName.HDEL, client -> client.hdel(key, fields));
    }

    @Override
    public Boolean hexists(final String key, final String field) {
        return execOp(key, OpName.HEXISTS, client -> client.hexists(key, field));
    }

    @Override
    public String hget(final String key, final String field) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HGET, client -> client.hget(key, field));
        } else {
            return execCompressOp(
              key,
              OpName.HGET,
              (client, state, op) -> op.decompressValue(client.hget(key, field), state)
            );
        }
    }

    @Override
    public Map<String, String> hgetAll(final String key) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HGETALL, client -> client.hgetAll(key));
        } else {
            return execCompressOp(
              key,
              OpName.HGETALL,
              (client, state, op) -> CollectionUtils.transform(client.hgetAll(key),
                                                               (key1, val) -> op.decompressValue(val, state))
            );
        }
    }

    @Override
    public Long hincrBy(final String key, final String field, final long value) {
        return execOp(key, OpName.HINCRBY, client -> client.hincrBy(key, field, value));
    }

    /* not supported by RedisPipeline 2.7.3 */
    public Double hincrByFloat(final String key, final String field, final double value) {
        return execOp(key, OpName.HINCRBYFLOAT, client -> client.hincrByFloat(key, field, value));
    }

    @Override
    public Long hsetnx(final String key, final String field, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HSETNX, client -> client.hsetnx(key, field, value));
        } else {
            return execCompressOp(
              key,
              OpName.HSETNX,
              (client, state, op) -> client.hsetnx(key, field, op.compressValue(value, state))
            );
        }
    }

    @Override
    public Set<String> hkeys(final String key) {
        return execOp(key, OpName.HKEYS, client -> client.hkeys(key));
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(final String key, final String cursor) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HSCAN, client -> client.hscan(key, cursor));
        } else {
            return execCompressOp(
              key,
              OpName.HSCAN,
              (client, state, op) ->
                new ScanResult<>(cursor, new ArrayList(CollectionUtils.transform(
                  client.hscan(key, cursor).getResult(),
                  entry -> {
                      entry.setValue(op.decompressValue(entry.getValue(), state));
                      return entry;
                  })))
            );
        }
    }

    private String getCursorValue(final ConnectionContext state, final CursorBasedResult cursor) {
        if (state != null && state.getMetadata("host") != null && cursor != null) {
            return cursor.getCursorForHost(state.getMetadata("host").toString());
        }

        return "0";
    }

    private List<OperationResult<ScanResult<String>>> scatterGatherScan(final CursorBasedResult<String> cursor,
                                                                        final int count, final String... pattern) {

        if (!(cursor instanceof TokenRackMapper)) {
            throw new DynoException("cursor does not implement the TokenRackMapper interface");
        }
        return new ArrayList<>(connPool.executeWithRing((TokenRackMapper) cursor, new BaseKeyOperation<ScanResult<String>>("SCAN", OpName.SCAN) {
            @Override
            public ScanResult<String> execute(final Jedis client, final ConnectionContext state) throws DynoException {
                if (pattern != null && pattern.length > 0) {
                    ScanParams sp = new ScanParams().count(count);
                    for (String s : pattern) {
                        sp.match(s);
                    }
                    return client.scan(getCursorValue(state, cursor), sp);
                } else {
                    return client.scan(getCursorValue(state, cursor));
                }
            }
        }));
    }

    @Override
    public Long hlen(final String key) {
        return execOp(key, OpName.HLEN, client -> client.hlen(key));
    }

    @Override
    public List<String> hmget(final String key, final String... fields) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HMGET, client -> client.hmget(key, fields));
        } else {
            return execCompressOp(
              key,
              OpName.HMGET,
              (client, state, op) ->
                new ArrayList<>(CollectionUtils.transform(client.hmget(key, fields),
                                                                s -> op.decompressValue(s, state)))
            );
        }
    }

    @Override
    public String hmset(final String key, final Map<String, String> hash) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HMSET, client -> client.hmset(key, hash));
        } else {
            return execCompressOp(
              key,
              OpName.HMSET,
              (client, state, op) ->
                client.hmset(key, CollectionUtils.transform(hash, (key1, val) -> op.compressValue(val, state)))
            );
        }
    }

    @Override
    public Long hset(final String key, final String field, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HSET, client -> client.hset(key, field, value));
        } else {
            return execCompressOp(
              key,
              OpName.HSET,
              (client, state, op) -> client.hset(key, field, op.compressValue(value, state))
            );
        }
    }

    @Override
    public Long hset(String key, Map<String, String> hash) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HSET, client -> client.hset(key, hash));
        } else {
            throw new UnsupportedOperationException("not yet implemented");
        }
    }

    @Override
    public List<String> hvals(final String key) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HVALS, client -> client.hvals(key));
        } else {
            return execCompressOp(
              key,
              OpName.HVALS,
              (client, state, op) ->
                new ArrayList<>(CollectionUtils.transform(client.hvals(key), s -> op.decompressValue(s, state)))
            );
        }
    }

    @Override
    public Long incr(final String key) {
        return execOp(key, OpName.INCR, client -> client.incr(key));
    }

    @Override
    public Long incrBy(final String key, final long delta) {
        return execOp(key, OpName.INCRBY, client -> client.incrBy(key, delta));
    }

    public Double incrByFloat(final String key, final double increment) {
        return execOp(key, OpName.INCRBYFLOAT, client -> client.incrByFloat(key, increment));
    }

    @Override
    public String lindex(final String key, final long index) {
        return execOp(key, OpName.LINDEX, client -> client.lindex(key, index));
    }

    @Override
    public Long linsert(final String key, final ListPosition where, final String pivot, final String value) {
        return execOp(key, OpName.LINSERT, client -> client.linsert(key, where, pivot, value));
    }

    @Override
    public Long llen(final String key) {
        return execOp(key, OpName.LLEN, client -> client.llen(key));
    }

    @Override
    public String lpop(final String key) {
        return execOp(key, OpName.LPOP, client -> client.lpop(key));
    }

    @Override
    public Long lpush(final String key, final String... values) {
        return execOp(key, OpName.LPUSH, client -> client.lpush(key, values));
    }

    @Override
    public Long lpushx(final String key, final String... values) {
        return execOp(key, OpName.LPUSHX, client -> client.lpushx(key, values));
    }

    @Override
    public List<String> lrange(final String key, final long start, final long end) {
        return execOp(key, OpName.LRANGE, client -> client.lrange(key, start, end));
    }

    @Override
    public Long lrem(final String key, final long count, final String value) {
        return execOp(key, OpName.LREM, client -> client.lrem(key, count, value));
    }

    @Override
    public String lset(final String key, final long index, final String value) {
        return execOp(key, OpName.LSET, client -> client.lset(key, index, value));
    }

    @Override
    public String ltrim(final String key, final long start, final long end) {
        return execOp(key, OpName.LTRIM, client -> client.ltrim(key, start, end));
    }

    @Override
    public Long persist(final String key) {
        return execOp(key, OpName.PERSIST, client -> client.persist(key));
    }

    @Override
    public Long pexpireAt(final String key, final long millisecondsTimestamp) {
        return execOp(key, OpName.PEXPIREAT, client -> client.pexpireAt(key, millisecondsTimestamp));
    }

    @Override
    public Long pttl(final String key) {
        return execOp(key, OpName.PTTL, client -> client.pttl(key));
    }

    @Override
    public Long touch(String key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String rename(String oldkey, String newkey) {
        return execOp(oldkey, OpName.RENAME, client -> client.rename(oldkey, newkey));
    }

    @Override
    public Long renamenx(String oldkey, String newkey) {
        return execOp(oldkey, OpName.RENAMENX, client -> client.renamenx(oldkey, newkey));
    }

    public String restore(final String key, final Integer ttl, final byte[] serializedValue) {
        return execOp(key, OpName.RESTORE, client -> client.restore(key, ttl, serializedValue));
    }

    @Override
    public String rpop(final String key) {
        return execOp(key, OpName.RPOP, client -> client.rpop(key));
    }

    @Override
    public String rpoplpush(final String srckey, final String dstkey) {
        return execOp(srckey, OpName.RPOPLPUSH, client -> client.rpoplpush(srckey, dstkey));
    }

    @Override
    public Long rpush(final String key, final String... values) {
        return execOp(key, OpName.RPUSH, client -> client.rpush(key, values));
    }

    @Override
    public Long rpushx(final String key, final String... values) {
        return execOp(key, OpName.RPUSHX, client -> client.rpushx(key, values));
    }

    @Override
    public Long sadd(final String key, final String... members) {
        return execOp(key, OpName.SADD, client -> client.sadd(key, members));
    }

    @Override
    public Long scard(final String key) {
        return execOp(key, OpName.SCARD, client -> client.scard(key));
    }

    @Override
    public Set<String> sdiff(final String... keys) {
        return execOp(keys[0], OpName.SDIFF, client -> client.sdiff(keys));
    }

    @Override
    public Long sdiffstore(final String dstkey, final String... keys) {
        return execOp(dstkey, OpName.SDIFFSTORE, client -> client.sdiffstore(dstkey, keys));
    }

    @Override
    public String set(final String key, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.SET, client -> client.set(key, value));
        } else {
            return execCompressOp(
              key,
              OpName.SET,
              (client, state, op) -> client.set(key, op.compressValue(value, state))
            );
        }
    }

    @Deprecated
    /**
     * use {@link set(String, String, SetParams)} instead
     */
    public String set(final String key, final String value, final String nxxx, final String expx, final long time) {
        SetParams setParams = SetParams.setParams();
        if (nxxx.equalsIgnoreCase("NX")) {
            setParams.nx();
        } else if (nxxx.equalsIgnoreCase("XX")) {
            setParams.xx();
        }
        if (expx.equalsIgnoreCase("EX")) {
            setParams.ex((int) time);
        } else if (expx.equalsIgnoreCase("PX")) {
            setParams.px(time);
        }

        return set(key, value, setParams);
    }

    @Override
    public String set(final String key, final String value, final SetParams setParams) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy())
            return execOp(key, OpName.SET, client -> client.set(key, value, setParams));
        else {
            return execCompressOp(
              key,
              OpName.SET,
              (client, state, op) -> client.set(key, op.compressValue(value, state), setParams)
            );
        }
    }

    @Override
    public Boolean setbit(final String key, final long offset, final boolean value) {
        return execOp(key, OpName.SETBIT, client -> client.setbit(key, offset, value));
    }

    @Override
    public Boolean setbit(final String key, final long offset, final String value) {
        return execOp(key, OpName.SETBIT, client -> client.setbit(key, offset, value));
    }

    @Override
    public String setex(final String key, final int seconds, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.SETEX, client -> client.setex(key, seconds, value));
        } else {
            return execCompressOp(
              key,
              OpName.SETEX,
              (client, state, op) -> client.setex(key, seconds, op.compressValue(value, state))
            );
        }
    }

    @Override
    public String psetex(final String key, final long milliseconds, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.PSETEX, client -> client.psetex(key, milliseconds, value));
        } else {
            return execCompressOp(
              key,
              OpName.PSETEX,
              (client, state, op) -> client.psetex(key, milliseconds, op.compressValue(value, state))
            );
        }
    }

    @Override
    public Long setnx(final String key, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.SETNX, client -> client.setnx(key, value));
        } else {
            return execCompressOp(
              key,
              OpName.SETNX,
              (client, state, op) -> client.setnx(key, op.compressValue(value, state))
            );
        }
    }

    @Override
    public Long setrange(final String key, final long offset, final String value) {
        return execOp(key, OpName.SETRANGE, client -> client.setrange(key, offset, value));
    }

    @Override
    public Boolean sismember(final String key, final String member) {
        return execOp(key, OpName.SISMEMBER, client -> client.sismember(key, member));
    }

    @Override
    public Set<String> smembers(final String key) {
        return execOp(key, OpName.SMEMBERS, client -> client.smembers(key));
    }

    public Long smove(final String srckey, final String dstkey, final String member) {
        return execOp(srckey, OpName.SMOVE, client -> client.smove(srckey, dstkey, member));
    }
    @Override
    public List<String> sort(String key) {
        return execOp(key, OpName.SORT, client -> client.sort(key));
    }

    @Override
    public List<String> sort(String key, SortingParams sortingParameters) {
        return execOp(key, OpName.SORT, client -> client.sort(key, sortingParameters));
    }

    @Override
    public String spop(final String key) {
        return execOp(key, OpName.SPOP, client -> client.spop(key));
    }

    @Override
    public Set<String> spop(String key, long count) {
        return execOp(key, OpName.SPOP, client -> client.spop(key, count));
    }

    @Override
    public String srandmember(final String key) {
        return execOp(key, OpName.SRANDMEMBER, client -> client.srandmember(key));
    }

    @Override
    public List<String> srandmember(String key, int count) {
        return execOp(key, OpName.SRANDMEMBER, client -> client.srandmember(key, count));
    }

    @Override
    public Long srem(final String key, final String... members) {
        return execOp(key, OpName.SREM, client -> client.srem(key, members));
    }

    @Override
    public ScanResult<String> sscan(final String key, final String cursor) {
        return execOp(key, OpName.SSCAN, client -> client.sscan(key, cursor));
    }

    @Override
    public ScanResult<String> sscan(final String key, final String cursor, final ScanParams params) {
        return execOp(key, OpName.SSCAN, client -> client.sscan(key, cursor, params));
    }

    @Override
    public Long strlen(final String key) {
        return execOp(key, OpName.STRLEN, client -> client.strlen(key));
    }

    @Override
    public String substr(String key, int start, int end) {
        return execOp(key, OpName.SUBSTR, client -> client.substr(key, start, end));
    }

    @Override
    public Long ttl(final String key) {
        return execOp(key, OpName.TTL, client -> client.ttl(key));
    }

    @Override
    public String type(final String key) {
        return execOp(key, OpName.TYPE, client -> client.type(key));
    }

    @Override
    public Long zadd(String key, double score, String member) {
        return execOp(key, OpName.ZADD, client -> client.zadd(key, score, member));
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) {
        return execOp(key, OpName.ZADD, client -> client.zadd(key, scoreMembers));
    }

    @Override
    public Long zadd(String key, double score, String member, ZAddParams params) {
        return execOp(key, OpName.ZADD, client -> client.zadd(key, score, member, params));
    }

    @Override
    public Long zcard(final String key) {
        return execOp(key, OpName.ZCARD, client -> client.zcard(key));
    }

    @Override
    public Long zcount(final String key, final double min, final double max) {
        return execOp(key, OpName.ZCOUNT, client -> client.zcount(key, min, max));
    }

    @Override
    public Long zcount(String key, String min, String max) {
        return execOp(key, OpName.ZCOUNT, client -> client.zcount(key, min, max));
    }

    @Override
    public Double zincrby(final String key, final double score, final String member) {
        return execOp(key, OpName.ZINCRBY, client -> client.zincrby(key, score, member));
    }

    @Override
    public Set<String> zrange(String key, long start, long end) {
        return execOp(key, OpName.ZRANGE, client -> client.zrange(key, start, end));
    }

    @Override
    public Long zrank(final String key, final String member) {
        return execOp(key, OpName.ZRANK, client -> client.zrank(key, member));
    }

    @Override
    public Long zrem(String key, String... member) {
        return execOp(key, OpName.ZREM, client -> client.zrem(key, member));
    }

    @Override
    public Long zremrangeByRank(final String key, final long start, final long end) {
        return execOp(key, OpName.ZREMRANGEBYRANK, client -> client.zremrangeByRank(key, start, end));
    }

    @Override
    public Long zremrangeByScore(final String key, final double start, final double end) {
        return execOp(key, OpName.ZREMRANGEBYSCORE, client -> client.zremrangeByScore(key, start, end));
    }

    @Override
    public Set<String> zrevrange(String key, long start, long end) {
        return execOp(key, OpName.ZREVRANGE, client -> client.zrevrange(key, start, end));
    }

    @Override
    public Long zrevrank(final String key, final String member) {
        return execOp(key, OpName.ZREVRANK, client -> client.zrevrank(key, member));
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        return execOp(key, OpName.ZRANGEWITHSCORES, client -> client.zrangeWithScores(key, start, end));
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        return execOp(key, OpName.ZREVRANGEWITHSCORES, client -> client.zrevrangeWithScores(key, start, end));
    }

    @Override
    public Double zscore(final String key, final String member) {
        return execOp(key, OpName.ZSCORE, client -> client.zscore(key, member));
    }

    @Override
    public ScanResult<Tuple> zscan(final String key, final String cursor) {
        return execOp(key, OpName.ZSCAN, client -> client.zscan(key, cursor));
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) {
        return execOp(key, OpName.ZRANGEBYSCORE, client -> client.zrangeByScore(key, min, max));
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) {
        return execOp(key, OpName.ZRANGEBYSCORE, client -> client.zrangeByScore(key, min, max));
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        return execOp(key, OpName.ZRANGEBYSCORE, client -> client.zrangeByScore(key, min, max, offset, count));
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
        return execOp(key, OpName.ZRANGEBYSCORE, client -> client.zrangeByScore(key, min, max, offset, count));
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min) {
        return execOp(key, OpName.ZREVRANGEBYSCORE, client -> client.zrevrangeByScore(key, max, min));
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return execOp(key, OpName.ZREVRANGEBYSCORE, client -> client.zrevrangeByScore(key, max, min, offset, count));
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min) {
        return execOp(key, OpName.ZREVRANGEBYSCORE, client -> client.zrevrangeByScore(key, max, min));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        return execOp(key, OpName.ZRANGEBYSCOREWITHSCORES, client -> client.zrangeByScoreWithScores(key, min, max));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        return execOp(key, OpName.ZREVRANGEBYSCOREWITHSCORES, client -> client.zrevrangeByScoreWithScores(key, max, min));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return execOp(key, OpName.ZRANGEBYSCOREWITHSCORES, client -> client.zrangeByScoreWithScores(key, min, max, offset, count));
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        return execOp(key, OpName.ZREVRANGEBYSCORE, client -> client.zrevrangeByScore(key, max, min, offset, count));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        return execOp(key, OpName.ZRANGEBYSCOREWITHSCORES, client -> client.zrangeByScoreWithScores(key, min, max));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        return execOp(key, OpName.ZREVRANGEBYSCOREWITHSCORES, client -> client.zrevrangeByScoreWithScores(key, max, min));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        return execOp(key, OpName.ZRANGEBYSCOREWITHSCORES, client -> client.zrangeByScoreWithScores(key, min, max, offset, count));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return execOp(key, OpName.ZREVRANGEBYSCOREWITHSCORES, client -> client.zrevrangeByScoreWithScores(key, max, min, offset, count));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        return execOp(key, OpName.ZREVRANGEBYSCOREWITHSCORES, client -> client.zrevrangeByScoreWithScores(key, max, min, offset, count));
    }

    @Override
    public Long zremrangeByScore(String key, String start, String end) {
        return execOp(key, OpName.ZREMRANGEBYSCORE, client -> client.zremrangeByScore(key, start, end));
    }

    @Override
    public Long zlexcount(String key, String min, String max) {
        return execOp(key, OpName.ZLEXCOUNT, client -> client.zlexcount(key, min, max));
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max) {
        return execOp(key, OpName.ZRANGEBYLEX, client -> client.zrangeByLex(key, min, max));
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
        return execOp(key, OpName.ZRANGEBYLEX, client -> client.zrangeByLex(key, min, max, offset, count));
    }

    @Override
    public Long zremrangeByLex(String key, String min, String max) {
        return execOp(key, OpName.ZREMRANGEBYLEX, client -> client.zremrangeByLex(key, min, max));
    }

    @Override
    public List<String> blpop(int timeout, String key) {
        return execOp(key, OpName.BLPOP, client -> client.blpop(timeout, key));
    }

    @Override
    public List<String> brpop(int timeout, String key) {
        return execOp(key, OpName.BRPOP, client -> client.brpop(timeout, key));
    }

    @Override
    public String echo(String string) {
        return execOp(string, OpName.ECHO, client -> client.echo(string));
    }

    @Override
    public Long move(String key, int dbIndex) {
        return execOp(key, OpName.MOVE, client -> client.move(key, dbIndex));
    }

    @Override
    public Long bitcount(String key) {
        return execOp(key, OpName.BITCOUNT, client -> client.bitcount(key));
    }

    @Override
    public Long pfadd(String key, String... elements) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public long pfcount(String key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long bitcount(String key, long start, long end) {
        return execOp(key, OpName.BITCOUNT, client -> client.bitcount(key, start, end));
    }

    /**
     * MULTI-KEY COMMANDS
     */

    @Override
    public List<String> blpop(int timeout, String... keys) {
        return execMultiOp(keys, OpName.BLPOP, client -> client.blpop(timeout, keys));
    }

    @Override
    public List<String> brpop(int timeout, String... keys) {
        return execMultiOp(keys, OpName.BRPOP, client -> client.brpop(timeout, keys));
    }

    @Override
    public List<String> blpop(String... keys) {
        return execMultiOp(keys, OpName.BLPOP, client -> client.blpop(keys));
    }

    @Override
    public List<String> brpop(String... keys) {
        return execMultiOp(keys, OpName.BRPOP, client -> client.brpop(keys));
    }

    @Override
    public Set<String> keys(String pattern) {

        Set<String> allResults = new HashSet<String>();
        Collection<OperationResult<Set<String>>> results = d_keys(pattern);
        for (OperationResult<Set<String>> result : results) {
            allResults.addAll(result.getResult());
        }
        return allResults;
    }

    /**
     * Use this with care, especially in the context of production databases.
     *
     * @param pattern Specifies the mach set for keys
     * @return a collection of operation results
     * @see <a href="http://redis.io/commands/KEYS">keys</a>
     */
    public Collection<OperationResult<Set<String>>> d_keys(final String pattern) {

        Logger.warn("Executing d_keys for pattern: " + pattern);

        Collection<OperationResult<Set<String>>> results = connPool
                .executeWithRing(new CursorBasedResultImpl<String>(new LinkedHashMap<String, ScanResult<String>>()), new BaseKeyOperation<Set<String>>(pattern, OpName.KEYS) {

                    @Override
                    public Set<String> execute(Jedis client, ConnectionContext state) throws DynoException {
                        return client.keys(pattern);
                    }
                });

        return results;
    }

    @Override
    public Long pexpire(String key, long milliseconds) {
        return execOp(key, OpName.PEXPIRE, client -> client.pexpire(key, milliseconds));
    }

    /**
     * Get values for all the keys provided. Returns a list of string values
     * corresponding to individual keys. If one of the key is missing, the
     * return list has null as its corresponding value.
     *
     * @param keys: variable list of keys to query
     * @return list of string values
     * @see <a href="http://redis.io/commands/MGET">mget</a>
     */
    @Override
    public List<String> mget(String... keys) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execMultiOp(keys, OpName.MGET, client -> client.mget(keys));
        } else {
            return execCompressMultiOp(
              keys,
              OpName.MGET,
              (client, state, op) -> new ArrayList<>(CollectionUtils.transform(client.mget(keys),
                                                                               s -> op.decompressValue(state, s)))
            );
        }
    }

    @Override
    public Long exists(String... keys) {
        return execMultiOp(keys, OpName.EXISTS, client -> client.exists(keys));
    }

    @Override
    public Long del(String... keys) {
        return execMultiOp(keys, OpName.DEL, client -> client.del(keys));
    }

    @Override
    public Long unlink(String... keys) {
        return execMultiOp(keys, OpName.UNLINK, client -> client.unlink(keys));
    }

    @Override
    public Long msetnx(String... keysvalues) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execMultiOp(keysvalues, OpName.MSETNX, client -> client.msetnx(keysvalues));
        } else {
            return execCompressMultiOp(
              keysvalues,
              OpName.MSETNX,
              (client, state, op) -> client.msetnx(op.compressMultiKeyValue(state, keysvalues))
            );
        }
    }

    @Override
    public String mset(String... keysvalues) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(keysvalues[0], OpName.MSET, client -> client.mset(keysvalues));
        } else {
            return execCompressMultiOp(
              keysvalues,
              OpName.MSET,
              (client, state, op) -> client.mset(op.compressMultiKeyValue(state, keysvalues))
            );
        }
    }

    @Override
    public Set<String> sinter(String... keys) {
        return execMultiOp(keys, OpName.SINTER, client -> client.sinter(keys));
    }

    public Long sinterstore(final String dstkey, final String... keys) {
        return execOp(dstkey, OpName.SINTERSTORE, client -> client.sinterstore(dstkey, keys));
    }

    @Override
    public Long sort(String key, SortingParams sortingParameters, String dstkey) {
        return execOp(key, OpName.SORT, client -> client.sort(key, sortingParameters, dstkey));
    }

    @Override
    public Long sort(String key, String dstkey) {
        return execOp(key, OpName.SORT, client -> client.sort(key, dstkey));
    }

    @Override
    public Set<String> sunion(String... keys) {
        return execMultiOp(keys, OpName.SUNION, client -> client.sunion(keys));
    }

    @Override
    public Long sunionstore(String dstkey, String... keys) {
        return execOp(dstkey, OpName.SUNIONSTORE, client -> client.sunionstore(dstkey, keys));
    }

    @Override
    public String watch(String... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String unwatch() {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zinterstore(String dstkey, String... sets) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zinterstore(String dstkey, ZParams params, String... sets) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min) {
        return execOp(key, OpName.ZREVRANGEBYLEX, client -> client.zrevrangeByLex(key, max, min));
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
        return execOp(key, OpName.ZREVRANGEBYLEX, client -> client.zrevrangeByLex(key, max, min, offset, count));
    }

    @Override
    public Long zunionstore(String dstkey, String... sets) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zunionstore(String dstkey, ZParams params, String... sets) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String brpoplpush(String source, String destination, int timeout) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long publish(String channel, String message) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void subscribe(JedisPubSub jedisPubSub, String... channels) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String randomKey() {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long bitop(BitOP op, String destKey, String... srcKeys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * NOT SUPPORTED ! Use {@link #dyno_scan(CursorBasedResult, int, String...)}
     * instead.
     *
     * @param cursor
     * @return nothing -- throws UnsupportedOperationException when invoked
     * @see #dyno_scan(CursorBasedResult, int, String...)
     */
    @Override
    public ScanResult<String> scan(String cursor) {
        throw new UnsupportedOperationException("Not supported - use dyno_scan(String, CursorBasedResult");
    }

    public CursorBasedResult<String> dyno_scan(String... pattern) {
        return this.dyno_scan(10, pattern);
    }

    public CursorBasedResult<String> dyno_scan(int count, String... pattern) {
        return this.dyno_scan(null, count, pattern);
    }

    public CursorBasedResult<String> dyno_scan(CursorBasedResult<String> cursor, int count, String... pattern) {
        if (cursor == null) {
            // Create a temporary cursor context which will maintain a map of token to rack
            cursor = new CursorBasedResultImpl<>(new LinkedHashMap<String, ScanResult<String>>());
        }
        final Map<String, ScanResult<String>> results = new LinkedHashMap<>();

        List<OperationResult<ScanResult<String>>> opResults = scatterGatherScan(cursor, count, pattern);
        for (OperationResult<ScanResult<String>> opResult : opResults) {
            results.put(opResult.getNode().getHostAddress(), opResult.getResult());
        }
        return new CursorBasedResultImpl<>(results, ((TokenRackMapper) cursor).getTokenRackMap());
    }

    @Override
    public String pfmerge(String destkey, String... sourcekeys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public long pfcount(String... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long touch(String... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }


    @Override
    public ScanResult<String> scan(String arg0, ScanParams arg1) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long bitpos(String arg0, boolean arg1) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long bitpos(String arg0, boolean arg1, BitPosParams arg2) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long geoadd(String arg0, Map<String, GeoCoordinate> arg1) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long geoadd(String arg0, double arg1, double arg2, String arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Double geodist(String arg0, String arg1, String arg2) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Double geodist(String arg0, String arg1, String arg2, GeoUnit arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<String> geohash(String arg0, String... arg1) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoCoordinate> geopos(String arg0, String... arg1) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadius(String arg0, double arg1, double arg2, double arg3, GeoUnit arg4) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadiusReadonly(String key, double longitude, double latitude, double radius, GeoUnit unit) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadius(String arg0, double arg1, double arg2, double arg3, GeoUnit arg4,
                                             GeoRadiusParam arg5) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadiusReadonly(String key, double longitude, double latitude, double radius, GeoUnit unit, GeoRadiusParam param) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String arg0, String arg1, double arg2, GeoUnit arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMemberReadonly(String key, String member, double radius, GeoUnit unit) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String arg0, String arg1, double arg2, GeoUnit arg3,
                                                     GeoRadiusParam arg4) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMemberReadonly(String key, String member, double radius, GeoUnit unit, GeoRadiusParam param) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<Long> bitfield(String key, String... arguments) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long hstrlen(String key, String field) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public ScanResult<Entry<String, String>> hscan(String arg0, String arg1, ScanParams arg2) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zadd(String arg0, Map<String, Double> arg1, ZAddParams arg2) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Double zincrby(String arg0, double arg1, String arg2, ZIncrByParams arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public ScanResult<Tuple> zscan(String arg0, String arg1, ScanParams arg2) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /******************* End Jedis String Commands **************/



    /******************* Jedis Binary Commands **************/

    @Override
    public Long append(final byte[] key, final byte[] value) {
        return execOp(key, OpName.APPEND, client -> client.append(key, value));
    }

    @Override
    public Long decr(final byte[] key) {
        return execOp(key, OpName.DECR, client -> client.decr(key));
    }

    @Override
    public Long decrBy(final byte[] key, final long delta) {
        return execOp(key, OpName.DECRBY, client -> client.decrBy(key, delta));
    }

    @Override
    public Long del(final byte[] key) {
        return execOp(key, OpName.DEL, client -> client.del(key));
    }

    @Override
    public Long unlink(byte[] key) {
        return execOp(key, OpName.UNLINK, client -> client.unlink(key));
    }

    public byte[] dump(final byte[] key) {
        return execOp(key, OpName.DUMP, client -> client.dump(key));
    }

    @Override
    public String restore(byte[] key, int ttl, byte[] serializedValue) {
        return execOp(key, OpName.RESTORE, client -> client.restore(key, ttl, serializedValue));
    }

    @Override
    public String restoreReplace(byte[] key, int ttl, byte[] serializedValue) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Boolean exists(final byte[] key) {
        return execOp(key, OpName.EXISTS, client -> client.exists(key));
    }

    @Override
    public Long expire(final byte[] key, final int seconds) {
        return execOp(key, OpName.EXPIRE, client -> client.expire(key, seconds));
    }

    @Override
    public Long expireAt(final byte[] key, final long unixTime) {
        return execOp(key, OpName.EXPIREAT, client -> client.expireAt(key, unixTime));
    }

    @Override
    public byte[] get(final byte[] key) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.GET, client -> client.get(key));
        } else {
            return execCompressOp(key, OpName.GET, (client, state, op) -> op.decompressValue(client.get(key), state));
        }
    }

    @Override
    public Boolean getbit(final byte[] key, final long offset) {
        return execOp(key, OpName.GETBIT, client -> client.getbit(key, offset));
    }

    @Override
    public byte[] getrange(final byte[] key, final long startOffset, final long endOffset) {
        return execOp(key, OpName.GETRANGE, client -> client.getrange(key, startOffset, endOffset));
    }

    @Override
    public byte[] getSet(final byte[] key, final byte[] value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.GETSET, client -> client.getSet(key, value));
        } else {
            return execCompressOp(
              key,
              OpName.GETSET,
              (client, state, op) -> op.decompressValue(client.getSet(key, op.compressValue(value, state)), state)
            );
        }
    }

    @Override
    public Long hdel(final byte[] key, final byte[]... fields) {
        return execOp(key, OpName.HDEL, client -> client.hdel(key, fields));
    }

    @Override
    public Boolean hexists(final byte[] key, final byte[] field) {
        return execOp(key, OpName.HEXISTS, client -> client.hexists(key, field));
    }

    @Override
    public byte[] hget(final byte[] key, final byte[] field) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HGET, client -> client.hget(key, field));
        } else {
            return execCompressOp(
              key,
              OpName.HGET,
              (client, state, op) -> op.decompressValue(client.hget(key, field), state)
            );
        }
    }

    @Override
    public Map<byte[], byte[]> hgetAll(final byte[] key) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HGETALL, client -> client.hgetAll(key));
        } else {
            return execCompressOp(
              key,
              OpName.HGETALL,
              (client, state, op) -> CollectionUtils.transform(client.hgetAll(key),
                                                               (key1, val) -> op.decompressValue(val, state))
            );
        }
    }

    @Override
    public Long hincrBy(final byte[] key, final byte[] field, final long value) {
        return execOp(key, OpName.HINCRBY, client -> client.hincrBy(key, field, value));
    }

    /* not supported by RedisPipeline 2.7.3 */
    public Double hincrByFloat(final byte[] key, final byte[] field, final double value) {
        return execOp(key, OpName.HINCRBYFLOAT, client -> client.hincrByFloat(key, field, value));
    }

    @Override
    public Long hsetnx(final byte[] key, final byte[] field, final byte[] value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HSETNX, client -> client.hsetnx(key, field, value));
        } else {
            return execCompressOp(
              key,
              OpName.HSETNX,
              (client, state, op) -> client.hsetnx(key, field, op.compressValue(value, state))
            );
        }
    }

    @Override
    public Set<byte[]> hkeys(final byte[] key) {
        return execOp(key, OpName.HKEYS, client -> client.hkeys(key));
    }

    @Override
    public ScanResult<Map.Entry<byte[], byte[]>> hscan(final byte[] key, final byte[] cursor) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HSCAN, client -> client.hscan(key, cursor));
        } else {
            return execCompressOp(
              key,
              OpName.HSCAN,
              (client, state, op) ->
                new ScanResult<>(cursor, new ArrayList(CollectionUtils.transform(
                  client.hscan(key, cursor).getResult(),
                  entry -> {
                      entry.setValue(op.decompressValue(entry.getValue(), state));
                      return entry;
                  })))
            );
        }
    }

    @Override
    public Long hlen(final byte[] key) {
        return execOp(key, OpName.HLEN, client -> client.hlen(key));
    }

    @Override
    public List<byte[]> hmget(final byte[] key, final byte[]... fields) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HMGET, client -> client.hmget(key, fields));
        } else {
            return execCompressOp(
              key,
              OpName.HMGET,
              (client, state, op) ->
                new ArrayList<>(CollectionUtils.transform(client.hmget(key, fields),
                                                          s -> op.decompressValue(s, state)))
            );
        }
    }

    @Override
    public String hmset(final byte[] key, final Map<byte[], byte[]> hash) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HMSET, client -> client.hmset(key, hash));
        } else {
            return execCompressOp(
              key,
              OpName.HMSET,
              (client, state, op) ->
                client.hmset(key, CollectionUtils.transform(hash, (key1, val) -> op.compressValue(val, state)))
            );
        }
    }

    @Override
    public Long hset(final byte[] key, final byte[] field, final byte[] value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HSET, client -> client.hset(key, field, value));
        } else {
            return execCompressOp(
              key,
              OpName.HSET,
              (client, state, op) -> client.hset(key, field, op.compressValue(value, state))
            );
        }
    }

    @Override
    public Long hset(byte[] key, Map<byte[], byte[]> hash) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HSET, client -> client.hset(key, hash));
        } else {
            throw new UnsupportedOperationException("not yet implemented");
        }
    }

    @Override
    public List<byte[]> hvals(final byte[] key) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HVALS, client -> client.hvals(key));
        } else {
            return execCompressOp(
              key,
              OpName.HVALS,
              (client, state, op) ->
                new ArrayList<>(CollectionUtils.transform(client.hvals(key), s -> op.decompressValue(s, state)))
            );
        }
    }

    @Override
    public Long incr(final byte[] key) {
        return execOp(key, OpName.INCR, client -> client.incr(key));
    }

    @Override
    public Long incrBy(final byte[] key, final long delta) {
        return execOp(key, OpName.INCRBY, client -> client.incrBy(key, delta));
    }

    public Double incrByFloat(final byte[] key, final double increment) {
        return execOp(key, OpName.INCRBYFLOAT, client -> client.incrByFloat(key, increment));
    }

    @Override
    public byte[] lindex(final byte[] key, final long index) {
        return execOp(key, OpName.LINDEX, client -> client.lindex(key, index));
    }

    @Override
    public Long linsert(final byte[] key, final ListPosition where, final byte[] pivot, final byte[] value) {
        return execOp(key, OpName.LINSERT, client -> client.linsert(key, where, pivot, value));
    }

    @Override
    public Long llen(final byte[] key) {
        return execOp(key, OpName.LLEN, client -> client.llen(key));
    }

    @Override
    public byte[] lpop(final byte[] key) {
        return execOp(key, OpName.LPOP, client -> client.lpop(key));
    }

    @Override
    public Long lpush(final byte[] key, final byte[]... values) {
        return execOp(key, OpName.LPUSH, client -> client.lpush(key, values));
    }

    @Override
    public Long lpushx(final byte[] key, final byte[]... values) {
        return execOp(key, OpName.LPUSHX, client -> client.lpushx(key, values));
    }

    @Override
    public List<byte[]> lrange(final byte[] key, final long start, final long end) {
        return execOp(key, OpName.LRANGE, client -> client.lrange(key, start, end));
    }

    @Override
    public Long lrem(final byte[] key, final long count, final byte[] value) {
        return execOp(key, OpName.LREM, client -> client.lrem(key, count, value));
    }

    @Override
    public String lset(final byte[] key, final long index, final byte[] value) {
        return execOp(key, OpName.LSET, client -> client.lset(key, index, value));
    }

    @Override
    public String ltrim(final byte[] key, final long start, final long end) {
        return execOp(key, OpName.LTRIM, client -> client.ltrim(key, start, end));
    }

    @Override
    public Long persist(final byte[] key) {
        return execOp(key, OpName.PERSIST, client -> client.persist(key));
    }

    @Override
    public Long pexpireAt(final byte[] key, final long millisecondsTimestamp) {
        return execOp(key, OpName.PEXPIREAT, client -> client.pexpireAt(key, millisecondsTimestamp));
    }

    @Override
    public Long pttl(final byte[] key) {
        return execOp(key, OpName.PTTL, client -> client.pttl(key));
    }

    @Override
    public Long touch(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String rename(byte[] oldkey, byte[] newkey) {
        return execOp(oldkey, OpName.RENAME, client -> client.rename(oldkey, newkey));
    }

    @Override
    public Long renamenx(byte[] oldkey, byte[] newkey) {
        return execOp(oldkey, OpName.RENAMENX, client -> client.renamenx(oldkey, newkey));
    }

    public String restore(final byte[] key, final Integer ttl, final byte[] serializedValue) {
        return execOp(key, OpName.RESTORE, client -> client.restore(key, ttl, serializedValue));
    }

    @Override
    public byte[] rpop(final byte[] key) {
        return execOp(key, OpName.RPOP, client -> client.rpop(key));
    }

    @Override
    public byte[] rpoplpush(final byte[] srckey, final byte[] dstkey) {
        return execOp(srckey, OpName.RPOPLPUSH, client -> client.rpoplpush(srckey, dstkey));
    }

    @Override
    public Long rpush(final byte[] key, final byte[]... values) {
        return execOp(key, OpName.RPUSH, client -> client.rpush(key, values));
    }

    @Override
    public Long rpushx(final byte[] key, final byte[]... values) {
        return execOp(key, OpName.RPUSHX, client -> client.rpushx(key, values));
    }

    @Override
    public Long sadd(final byte[] key, final byte[]... members) {
        return execOp(key, OpName.SADD, client -> client.sadd(key, members));
    }

    @Override
    public Long scard(final byte[] key) {
        return execOp(key, OpName.SCARD, client -> client.scard(key));
    }

    @Override
    public Set<byte[]> sdiff(final byte[]... keys) {
        return execOp(keys[0], OpName.SDIFF, client -> client.sdiff(keys));
    }

    @Override
    public Long sdiffstore(final byte[] dstkey, final byte[]... keys) {
        return execOp(dstkey, OpName.SDIFFSTORE, client -> client.sdiffstore(dstkey, keys));
    }

    @Override
    public String set(final byte[] key, final byte[] value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.SET, client -> client.set(key, value));
        } else {
            return execCompressOp(
              key,
              OpName.SET,
              (client, state, op) -> client.set(key, op.compressValue(value, state))
            );
        }
    }

    @Deprecated
    /**
     * use {@link set(byte[], byte[], SetParams)} instead
     */
    public String set(final byte[] key, final byte[] value, final String nxxx, final String expx, final long time) {
        SetParams setParams = SetParams.setParams();
        if (nxxx.equalsIgnoreCase("NX")) {
            setParams.nx();
        } else if (nxxx.equalsIgnoreCase("XX")) {
            setParams.xx();
        }
        if (expx.equalsIgnoreCase("EX")) {
            setParams.ex((int) time);
        } else if (expx.equalsIgnoreCase("PX")) {
            setParams.px(time);
        }

        return d_set(key, value, setParams);
    }

    public String set(final byte[] key, final byte[] value, final SetParams setParams) {
        return d_set(key, value, setParams);
    }

    public String d_set(final byte[] key, final byte[] value, final SetParams setParams) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy())
            return execOp(key, OpName.SET, client -> client.set(key, value, setParams));
        else {
            return execCompressOp(
              key,
              OpName.SET,
              (client, state, op) -> client.set(key, op.compressValue(value, state), setParams)
            );
        }
    }

    @Override
    public Boolean setbit(final byte[] key, final long offset, final boolean value) {
        return execOp(key, OpName.SETBIT, client -> client.setbit(key, offset, value));
    }

    @Override
    public Boolean setbit(final byte[] key, final long offset, final byte[] value) {
        return execOp(key, OpName.SETBIT, client -> client.setbit(key, offset, value));
    }

    @Override
    public String setex(final byte[] key, final int seconds, final byte[] value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.SETEX, client -> client.setex(key, seconds, value));
        } else {
            return execCompressOp(
              key,
              OpName.SETEX,
              (client, state, op) -> client.setex(key, seconds, op.compressValue(value, state))
            );
        }
    }

    @Override
    public String psetex(final byte[] key, final long milliseconds, final byte[] value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.PSETEX, client -> client.psetex(key, milliseconds, value));
        } else {
            return execCompressOp(
              key,
              OpName.PSETEX,
              (client, state, op) -> client.psetex(key, milliseconds, op.compressValue(value, state))
            );
        }
    }

    @Override
    public Long setnx(final byte[] key, final byte[] value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.SETNX, client -> client.setnx(key, value));
        } else {
            return execCompressOp(
              key,
              OpName.SETNX,
              (client, state, op) -> client.setnx(key, op.compressValue(value, state))
            );
        }
    }

    @Override
    public Long setrange(final byte[] key, final long offset, final byte[] value) {
        return execOp(key, OpName.SETRANGE, client -> client.setrange(key, offset, value));
    }

    @Override
    public Boolean sismember(final byte[] key, final byte[] member) {
        return execOp(key, OpName.SISMEMBER, client -> client.sismember(key, member));
    }

    @Override
    public Set<byte[]> smembers(final byte[] key) {
        return execOp(key, OpName.SMEMBERS, client -> client.smembers(key));
    }

    public Long smove(final byte[] srckey, final byte[] dstkey, final byte[] member) {
        return execOp(srckey, OpName.SMOVE, client -> client.smove(srckey, dstkey, member));
    }
    @Override
    public List<byte[]> sort(byte[] key) {
        return execOp(key, OpName.SORT, client -> client.sort(key));
    }

    @Override
    public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
        return execOp(key, OpName.SORT, client -> client.sort(key, sortingParameters));
    }

    @Override
    public byte[] spop(final byte[] key) {
        return execOp(key, OpName.SPOP, client -> client.spop(key));
    }

    @Override
    public Set<byte[]> spop(byte[] key, long count) {
        return execOp(key, OpName.SPOP, client -> client.spop(key, count));
    }

    @Override
    public byte[] srandmember(final byte[] key) {
        return execOp(key, OpName.SRANDMEMBER, client -> client.srandmember(key));
    }

    @Override
    public List<byte[]> srandmember(byte[] key, int count) {
        return execOp(key, OpName.SRANDMEMBER, client -> client.srandmember(key, count));
    }

    @Override
    public Long srem(final byte[] key, final byte[]... members) {
        return execOp(key, OpName.SREM, client -> client.srem(key, members));
    }

    @Override
    public ScanResult<byte[]> sscan(final byte[] key, final byte[] cursor) {
        return execOp(key, OpName.SSCAN, client -> client.sscan(key, cursor));
    }

    @Override
    public ScanResult<byte[]> sscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        return execOp(key, OpName.SSCAN, client -> client.sscan(key, cursor, params));
    }

    @Override
    public Long strlen(final byte[] key) {
        return execOp(key, OpName.STRLEN, client -> client.strlen(key));
    }

    @Override
    public byte[] substr(byte[] key, int start, int end) {
        return execOp(key, OpName.SUBSTR, client -> client.substr(key, start, end));
    }

    @Override
    public Long ttl(final byte[] key) {
        return execOp(key, OpName.TTL, client -> client.ttl(key));
    }

    @Override
    public String type(final byte[] key) {
        return execOp(key, OpName.TYPE, client -> client.type(key));
    }

    @Override
    public Long zadd(byte[] key, double score, byte[] member) {
        return execOp(key, OpName.ZADD, client -> client.zadd(key, score, member));
    }

    @Override
    public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
        return execOp(key, OpName.ZADD, client -> client.zadd(key, scoreMembers));
    }

    @Override
    public Long zadd(byte[] key, double score, byte[] member, ZAddParams params) {
        return execOp(key, OpName.ZADD, client -> client.zadd(key, score, member, params));
    }

    @Override
    public Long zcard(final byte[] key) {
        return execOp(key, OpName.ZCARD, client -> client.zcard(key));
    }

    @Override
    public Long zcount(final byte[] key, final double min, final double max) {
        return execOp(key, OpName.ZCOUNT, client -> client.zcount(key, min, max));
    }

    @Override
    public Long zcount(byte[] key, byte[] min, byte[] max) {
        return execOp(key, OpName.ZCOUNT, client -> client.zcount(key, min, max));
    }

    @Override
    public Double zincrby(final byte[] key, final double score, final byte[] member) {
        return execOp(key, OpName.ZINCRBY, client -> client.zincrby(key, score, member));
    }

    @Override
    public Set<byte[]> zrange(byte[] key, long start, long end) {
        return execOp(key, OpName.ZRANGE, client -> client.zrange(key, start, end));
    }

    @Override
    public Long zrank(final byte[] key, final byte[] member) {
        return execOp(key, OpName.ZRANK, client -> client.zrank(key, member));
    }

    @Override
    public Long zrem(byte[] key, byte[]... member) {
        return execOp(key, OpName.ZREM, client -> client.zrem(key, member));
    }

    @Override
    public Long zremrangeByRank(final byte[] key, final long start, final long end) {
        return execOp(key, OpName.ZREMRANGEBYRANK, client -> client.zremrangeByRank(key, start, end));
    }

    @Override
    public Long zremrangeByScore(final byte[] key, final double start, final double end) {
        return execOp(key, OpName.ZREMRANGEBYSCORE, client -> client.zremrangeByScore(key, start, end));
    }

    @Override
    public Set<byte[]> zrevrange(byte[] key, long start, long end) {
        return execOp(key, OpName.ZREVRANGE, client -> client.zrevrange(key, start, end));
    }

    @Override
    public Long zrevrank(final byte[] key, final byte[] member) {
        return execOp(key, OpName.ZREVRANK, client -> client.zrevrank(key, member));
    }

    @Override
    public Set<Tuple> zrangeWithScores(byte[] key, long start, long end) {
        return execOp(key, OpName.ZRANGEWITHSCORES, client -> client.zrangeWithScores(key, start, end));
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(byte[] key, long start, long end) {
        return execOp(key, OpName.ZREVRANGEWITHSCORES, client -> client.zrevrangeWithScores(key, start, end));
    }

    @Override
    public Double zscore(final byte[] key, final byte[] member) {
        return execOp(key, OpName.ZSCORE, client -> client.zscore(key, member));
    }

    @Override
    public ScanResult<Tuple> zscan(final byte[] key, final byte[] cursor) {
        return execOp(key, OpName.ZSCAN, client -> client.zscan(key, cursor));
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        return execOp(key, OpName.ZRANGEBYSCORE, client -> client.zrangeByScore(key, min, max));
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max) {
        return execOp(key, OpName.ZRANGEBYSCORE, client -> client.zrangeByScore(key, min, max));
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        return execOp(key, OpName.ZRANGEBYSCORE, client -> client.zrangeByScore(key, min, max, offset, count));
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
        return execOp(key, OpName.ZRANGEBYSCORE, client -> client.zrangeByScore(key, min, max, offset, count));
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
        return execOp(key, OpName.ZREVRANGEBYSCORE, client -> client.zrevrangeByScore(key, max, min));
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
        return execOp(key, OpName.ZREVRANGEBYSCORE, client -> client.zrevrangeByScore(key, max, min, offset, count));
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
        return execOp(key, OpName.ZREVRANGEBYSCORE, client -> client.zrevrangeByScore(key, max, min));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        return execOp(key, OpName.ZRANGEBYSCOREWITHSCORES, client -> client.zrangeByScoreWithScores(key, min, max));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        return execOp(key, OpName.ZREVRANGEBYSCOREWITHSCORES, client -> client.zrevrangeByScoreWithScores(key, max, min));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        return execOp(key, OpName.ZRANGEBYSCOREWITHSCORES, client -> client.zrangeByScoreWithScores(key, min, max, offset, count));
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
        return execOp(key, OpName.ZREVRANGEBYSCORE, client -> client.zrevrangeByScore(key, max, min, offset, count));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
        return execOp(key, OpName.ZRANGEBYSCOREWITHSCORES, client -> client.zrangeByScoreWithScores(key, min, max));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
        return execOp(key, OpName.ZREVRANGEBYSCOREWITHSCORES, client -> client.zrevrangeByScoreWithScores(key, max, min));
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset, int count) {
        return execOp(key, OpName.ZRANGEBYSCOREWITHSCORES, client -> client.zrangeByScoreWithScores(key, min, max, offset, count));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        return execOp(key, OpName.ZREVRANGEBYSCOREWITHSCORES, client -> client.zrevrangeByScoreWithScores(key, max, min, offset, count));
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset, int count) {
        return execOp(key, OpName.ZREVRANGEBYSCOREWITHSCORES, client -> client.zrevrangeByScoreWithScores(key, max, min, offset, count));
    }

    @Override
    public Long zremrangeByScore(byte[] key, byte[] start, byte[] end) {
        return execOp(key, OpName.ZREMRANGEBYSCORE, client -> client.zremrangeByScore(key, start, end));
    }

    @Override
    public Long zlexcount(byte[] key, byte[] min, byte[] max) {
        return execOp(key, OpName.ZLEXCOUNT, client -> client.zlexcount(key, min, max));
    }

    @Override
    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max) {
        return execOp(key, OpName.ZRANGEBYLEX, client -> client.zrangeByLex(key, min, max));
    }

    @Override
    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max, int offset, int count) {
        return execOp(key, OpName.ZRANGEBYLEX, client -> client.zrangeByLex(key, min, max, offset, count));
    }

    @Override
    public Long zremrangeByLex(byte[] key, byte[] min, byte[] max) {
        return execOp(key, OpName.ZREMRANGEBYLEX, client -> client.zremrangeByLex(key, min, max));
    }

    @Override
    public byte[] echo(byte[] string) {
        return execOp(string, OpName.ECHO, client -> client.echo(string));
    }

    @Override
    public Long move(byte[] key, int dbIndex) {
        return execOp(key, OpName.MOVE, client -> client.move(key, dbIndex));
    }

    @Override
    public Long bitcount(byte[] key) {
        return execOp(key, OpName.BITCOUNT, client -> client.bitcount(key));
    }

    @Override
    public Long pfadd(byte[] key, byte[]... elements) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public long pfcount(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long bitcount(byte[] key, long start, long end) {
        return execOp(key, OpName.BITCOUNT, client -> client.bitcount(key, start, end));
    }

    /**
     * MULTI-KEY COMMANDS
     */

    @Override
    public List<byte[]> blpop(int timeout, byte[]... keys) {
        return execMultiOp(keys, OpName.BLPOP, client -> client.blpop(timeout, keys));
    }

    @Override
    public List<byte[]> brpop(int timeout, byte[]... keys) {
        return execMultiOp(keys, OpName.BRPOP, client -> client.brpop(timeout, keys));
    }

    @Override
    public List<byte[]> blpop(byte[]... keys) {
        return execMultiOp(keys, OpName.BLPOP, client -> client.blpop(keys));
    }

    @Override
    public List<byte[]> brpop(byte[]... keys) {
        return execMultiOp(keys, OpName.BRPOP, client -> client.brpop(keys));
    }

    @Override
    public Set<byte[]> keys(final byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long pexpire(byte[] key, long milliseconds) {
        return execOp(key, OpName.PEXPIRE, client -> client.pexpire(key, milliseconds));
    }

    /**
     * Get values for all the keys provided. Returns a list of string values
     * corresponding to individual keys. If one of the key is missing, the
     * return list has null as its corresponding value.
     *
     * @param keys: variable list of keys to query
     * @return list of string values
     * @see <a href="http://redis.io/commands/MGET">mget</a>
     */
    @Override
    public List<byte[]> mget(byte[]... keys) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execMultiOp(keys, OpName.MGET, client -> client.mget(keys));
        } else {
            return execCompressMultiOp(
              keys,
              OpName.MGET,
              (client, state, op) -> new ArrayList<>(CollectionUtils.transform(client.mget(keys),
                                                                               s -> op.decompressValue(state, s)))
            );
        }
    }

    @Override
    public Long exists(byte[]... keys) {
        return execMultiOp(keys, OpName.EXISTS, client -> client.exists(keys));
    }

    @Override
    public Long del(byte[]... keys) {
        return execMultiOp(keys, OpName.DEL, client -> client.del(keys));
    }

    @Override
    public Long unlink(byte[]... keys) {
        return execMultiOp(keys, OpName.UNLINK, client -> client.unlink(keys));
    }

    @Override
    public Long msetnx(byte[]... keysvalues) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execMultiOp(keysvalues, OpName.MSETNX, client -> client.msetnx(keysvalues));
        } else {
            return execCompressMultiOp(
              keysvalues,
              OpName.MSETNX,
              (client, state, op) -> client.msetnx(op.compressMultiKeyValue(state, keysvalues))
            );
        }
    }

    @Override
    public String mset(byte[]... keysvalues) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(keysvalues[0], OpName.MSET, client -> client.mset(keysvalues));
        } else {
            return execCompressMultiOp(
              keysvalues,
              OpName.MSET,
              (client, state, op) -> client.mset(op.compressMultiKeyValue(state, keysvalues))
            );
        }
    }

    @Override
    public Set<byte[]> sinter(byte[]... keys) {
        return execMultiOp(keys, OpName.SINTER, client -> client.sinter(keys));
    }

    public Long sinterstore(final byte[] dstkey, final byte[]... keys) {
        return execOp(dstkey, OpName.SINTERSTORE, client -> client.sinterstore(dstkey, keys));
    }

    @Override
    public Long sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
        return execOp(key, OpName.SORT, client -> client.sort(key, sortingParameters, dstkey));
    }

    @Override
    public Long sort(byte[] key, byte[] dstkey) {
        return execOp(key, OpName.SORT, client -> client.sort(key, dstkey));
    }

    @Override
    public Set<byte[]> sunion(byte[]... keys) {
        return execMultiOp(keys, OpName.SUNION, client -> client.sunion(keys));
    }

    @Override
    public Long sunionstore(byte[] dstkey, byte[]... keys) {
        return execOp(dstkey, OpName.SUNIONSTORE, client -> client.sunionstore(dstkey, keys));
    }

    @Override
    public String watch(byte[]... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zinterstore(byte[] dstkey, byte[]... sets) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zinterstore(byte[] dstkey, ZParams params, byte[]... sets) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min) {
        return execOp(key, OpName.ZREVRANGEBYLEX, client -> client.zrevrangeByLex(key, max, min));
    }

    @Override
    public Set<byte[]> zrevrangeByLex(byte[] key, byte[] max, byte[] min, int offset, int count) {
        return execOp(key, OpName.ZREVRANGEBYLEX, client -> client.zrevrangeByLex(key, max, min, offset, count));
    }

    @Override
    public Long zunionstore(byte[] dstkey, byte[]... sets) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zunionstore(byte[] dstkey, ZParams params, byte[]... sets) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public byte[] brpoplpush(byte[] source, byte[] destination, int timeout) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long publish(byte[] channel, byte[] message) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void subscribe(BinaryJedisPubSub jedisPubSub, byte[]... channels) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public void psubscribe(BinaryJedisPubSub jedisPubSub, byte[]... patterns) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public byte[] randomBinaryKey() {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long bitop(BitOP op, byte[] destKey, byte[]... srcKeys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String pfmerge(byte[] destkey, byte[]... sourcekeys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long pfcount(byte[]... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long touch(byte[]... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }


    @Override
    public Long geoadd(byte[] arg0, Map<byte[], GeoCoordinate> arg1) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long geoadd(byte[] arg0, double arg1, double arg2, byte[] arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Double geodist(byte[] arg0, byte[] arg1, byte[] arg2) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Double geodist(byte[] arg0, byte[] arg1, byte[] arg2, GeoUnit arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<byte[]> geohash(byte[] arg0, byte[]... arg1) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoCoordinate> geopos(byte[] arg0, byte[]... arg1) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadius(byte[] arg0, double arg1, double arg2, double arg3, GeoUnit arg4) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadiusReadonly(byte[] key, double longitude, double latitude, double radius, GeoUnit unit) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadius(byte[] arg0, double arg1, double arg2, double arg3, GeoUnit arg4,
                                             GeoRadiusParam arg5) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadiusReadonly(byte[] key, double longitude, double latitude, double radius, GeoUnit unit, GeoRadiusParam param) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(byte[] arg0, byte[] arg1, double arg2, GeoUnit arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMemberReadonly(byte[] key, byte[] member, double radius, GeoUnit unit) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(byte[] arg0, byte[] arg1, double arg2, GeoUnit arg3,
                                                     GeoRadiusParam arg4) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMemberReadonly(byte[] key, byte[] member, double radius, GeoUnit unit, GeoRadiusParam param) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<Long> bitfield(byte[] key, byte[]... arguments) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long hstrlen(byte[] key, byte[] field) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public ScanResult<Entry<byte[], byte[]>> hscan(byte[] arg0, byte[] arg1, ScanParams arg2) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zadd(byte[] arg0, Map<byte[], Double> arg1, ZAddParams arg2) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Double zincrby(byte[] arg0, double arg1, byte[] arg2, ZIncrByParams arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public ScanResult<Tuple> zscan(byte[] arg0, byte[] arg1, ScanParams arg2) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    /******************* End Jedis Binary Commands **************/


    private boolean validHashtag(final String hashtag) {
        return !Strings.isNullOrEmpty(hashtag) && hashtag.length() == 2;
    }

    private String ehashDataKey(String key) {
        String hashtag = connPool.getConfiguration().getHashtag();
        if (!validHashtag(hashtag)) {
            throw new IllegalStateException("hashtags not set");
        }

        return new StringBuilder(hashtag)
                .insert(1, key)
                .toString();
    }

    @VisibleForTesting
    String ehashMetadataKey(String key) {
        final String hashtag = connPool.getConfiguration().getHashtag();
        if (!validHashtag(hashtag)) {
            throw new IllegalStateException("hashtags not set");
        }

        return new StringBuilder(hashtag)
                .insert(0, DYNO_EXIPREHASH_METADATA_KEYPREFIX)
                .insert(DYNO_EXIPREHASH_METADATA_KEYPREFIX.length() + 1, key)
                .toString();
    }

    private double timeInEpochSeconds(long ttl) {
        final long timeSinceEpoch = System.currentTimeMillis() / 1000L;

        return timeSinceEpoch + ttl;
    }

    @AllArgsConstructor
    @Getter
    private class EHMetadataUpdateResult {
        private final String dataKey;
        private final String metadataKey;
        private final Set<String> expiredFields;
        private final Response<Long> hdelResponse;
        private final Response<Long> zremResponse;
    }

    private EHMetadataUpdateResult ehPurgeExpiredFields(DynoJedisPipeline pipeline, String key) {
        final String metadataKey = ehashMetadataKey(key);
        final String dataKey = ehashDataKey(key);
        final double now = timeInEpochSeconds(0);

        // get expired fields
        final Set<String> expiredFields = this.zrangeByScore(metadataKey, 0, now);

        Response<Long> hdelResponse = null;
        Response<Long> zremResponse = null;
        if (expiredFields.size() > 0) {
            hdelResponse = pipeline.hdel(dataKey, expiredFields.toArray(new String[0]));
            zremResponse = pipeline.zremrangeByScore(metadataKey, 0, now);
        }

        return new EHMetadataUpdateResult(dataKey, metadataKey, expiredFields, hdelResponse, zremResponse);
    }

    private void ehVerifyMetadataUpdate(EHMetadataUpdateResult ehMetadataUpdateResult) {
        if (ehMetadataUpdateResult.expiredFields.size() > 0 && ehMetadataUpdateResult.hdelResponse != null &&
                (ehMetadataUpdateResult.expiredFields.size() != ehMetadataUpdateResult.hdelResponse.get())) {
            // If requested field is not in in the expired fields list, correctness of this request is not affected.
            Logger.debug("Expire hash:{} inconsistent with metadata:{}. Failed to delete expired fields from hash",
                    ehMetadataUpdateResult.dataKey, ehMetadataUpdateResult.metadataKey);
        }

        // if fields were not deleted from metadata, correctness is not affected.
        if (ehMetadataUpdateResult.expiredFields.size() > 0 && ehMetadataUpdateResult.zremResponse != null &&
                ehMetadataUpdateResult.expiredFields.size() != ehMetadataUpdateResult.zremResponse.get()) {
            Logger.debug("Expire hash:{} inconsistent with metadata:{}. Failed to delete expired fields from metadata",
                    ehMetadataUpdateResult.dataKey, ehMetadataUpdateResult.metadataKey);
        }
    }

    @Override
    public Long ehset(final String key, final String field, final String value, final long ttl)
            throws UnsupportedOperationException, DynoException {
        final DynoJedisPipeline pipeline = this.pipelined();
        final Response<Long> zResponse = pipeline.zadd(ehashMetadataKey(key), timeInEpochSeconds(ttl), field,
                ZAddParams.zAddParams().ch());
        final Response<Long> hResponse = pipeline.hset(ehashDataKey(key), field, value);
        pipeline.sync();

        return hResponse.get();
    }

    public Long ehsetnx(final String key, final String field, final String value, final long ttl) {
        final String ehashDataKey = ehashDataKey(key);
        final String ehashMetadataKey = ehashMetadataKey(key);
        final DynoJedisPipeline pipeline = this.pipelined();
        final Response<Long> zResponse = pipeline.zadd(ehashMetadataKey, timeInEpochSeconds(ttl), field,
                ZAddParams.zAddParams().ch());
        final Response<Long> hResponse = pipeline.hsetnx(ehashDataKey, field, value);
        pipeline.sync();

        // If metadata operation failed, remove the data and throw exception
        if (!zResponse.get().equals(hResponse.get())) {
            hdel(ehashDataKey, field);
            zrem(ehashMetadataKey, field);
            throw new DynoException("Metadata inconsistent with data for expireHash: " + ehashDataKey);
        }
        return hResponse.get();
    }

    @Override
    public String ehget(final String key, final String field)
            throws UnsupportedOperationException, DynoException {
        final String dataKey = ehashDataKey(key);
        final DynoJedisPipeline pipeline = this.pipelined();

        EHMetadataUpdateResult ehMetadataUpdateResult = ehPurgeExpiredFields(pipeline, key);
        final Response<String> getResponse = pipeline.hget(dataKey, field);
        pipeline.sync();

        // verify if all expired fields were removed from data and metadata
        ehVerifyMetadataUpdate(ehMetadataUpdateResult);

        // Return failure if the requested field was expired and was not removed from the data
        if (ehMetadataUpdateResult.expiredFields.size() > 0 && ehMetadataUpdateResult.hdelResponse != null &&
                (ehMetadataUpdateResult.expiredFields.size() != ehMetadataUpdateResult.hdelResponse.get()) &&
                ehMetadataUpdateResult.expiredFields.contains(field)) {
            throw new DynoException("Failed to update expire hash metadata");
        }

        return getResponse.get();
    }

    @Override
    public Long ehdel(final String key, final String... fields) {
        final DynoJedisPipeline pipeline = this.pipelined();
        final Response<Long> zResponse = pipeline.zrem(ehashMetadataKey(key), fields);
        final Response<Long> hResponse = pipeline.hdel(ehashDataKey(key), fields);
        pipeline.sync();

        if (zResponse.get().compareTo(hResponse.get()) != 0) {
            Logger.error("Operation: {} - data: {} and metadata: {} field count mismatch",
                    OpName.EHDEL, hResponse.get(), zResponse.get());
        }

        return hResponse.get();
    }

    @Override
    public Boolean ehexists(final String key, final String field) {
        final String dataKey = ehashDataKey(key);
        final DynoJedisPipeline pipeline = this.pipelined();

        EHMetadataUpdateResult ehMetadataUpdateResult = ehPurgeExpiredFields(pipeline, key);
        final Response<Boolean> existsResponse = pipeline.hexists(dataKey, field);
        pipeline.sync();

        // verify if all expired fields were removed from data and metadata
        ehVerifyMetadataUpdate(ehMetadataUpdateResult);

        // Return failure if the requested field was expired and was not removed from the data
        if (ehMetadataUpdateResult.expiredFields.size() > 0 && ehMetadataUpdateResult.hdelResponse != null &&
                (ehMetadataUpdateResult.expiredFields.size() != ehMetadataUpdateResult.hdelResponse.get()) &&
                ehMetadataUpdateResult.expiredFields.contains(field)) {
            throw new DynoException("Failed to update expire hash metadata");
        }

        return existsResponse.get();
    }

    @Override
    public Map<String, String> ehgetall(final String key) {
        final String dataKey = ehashDataKey(key);
        final DynoJedisPipeline pipeline = this.pipelined();

        EHMetadataUpdateResult ehMetadataUpdateResult = ehPurgeExpiredFields(pipeline, key);
        final Response<Map<String, String>> getallResponse = pipeline.hgetAll(dataKey);
        pipeline.sync();

        // verify if all expired fields were removed from data and metadata
        ehVerifyMetadataUpdate(ehMetadataUpdateResult);

        // on failure to remove all expired keys, fail
        if (ehMetadataUpdateResult.expiredFields.size() > 0 && ehMetadataUpdateResult.hdelResponse != null &&
                (ehMetadataUpdateResult.expiredFields.size() != ehMetadataUpdateResult.hdelResponse.get())) {
            throw new DynoException("Failed to expire hash fields");
        }

        return getallResponse.get();
    }

    @Override
    public Set<String> ehkeys(final String key) {
        final String dataKey = ehashDataKey(key);
        final DynoJedisPipeline pipeline = this.pipelined();

        EHMetadataUpdateResult ehMetadataUpdateResult = ehPurgeExpiredFields(pipeline, key);
        final Response<Set<String>> getkeysResponse = pipeline.hkeys(dataKey);
        pipeline.sync();

        // verify if all expired fields were removed from data and metadata
        ehVerifyMetadataUpdate(ehMetadataUpdateResult);

        // on failure to remove all expired keys, fail
        if (ehMetadataUpdateResult.expiredFields.size() > 0 && ehMetadataUpdateResult.hdelResponse != null &&
                (ehMetadataUpdateResult.expiredFields.size() != ehMetadataUpdateResult.hdelResponse.get())) {
            throw new DynoException("Failed to expire hash fields");
        }

        return getkeysResponse.get();
    }

    @Override
    public List<String> ehvals(final String key) {
        final String dataKey = ehashDataKey(key);
        final DynoJedisPipeline pipeline = this.pipelined();

        EHMetadataUpdateResult ehMetadataUpdateResult = ehPurgeExpiredFields(pipeline, key);
        final Response<List<String>> getvalsResponse = pipeline.hvals(dataKey);
        pipeline.sync();

        // verify if all expired fields were removed from data and metadata
        ehVerifyMetadataUpdate(ehMetadataUpdateResult);

        // on failure to remove all expired keys, fail
        if (ehMetadataUpdateResult.expiredFields.size() > 0 && ehMetadataUpdateResult.hdelResponse != null &&
                (ehMetadataUpdateResult.expiredFields.size() != ehMetadataUpdateResult.hdelResponse.get())) {
            throw new DynoException("Failed to expire hash fields");
        }

        return getvalsResponse.get();
    }

    @Override
    public List<String> ehmget(final String key, final String... fields) {
        final String dataKey = ehashDataKey(key);
        final DynoJedisPipeline pipeline = this.pipelined();

        EHMetadataUpdateResult ehMetadataUpdateResult = ehPurgeExpiredFields(pipeline, key);
        final Response<List<String>> mgetResponse = pipeline.hmget(dataKey, fields);
        pipeline.sync();

        // verify if all expired fields were removed from data and metadata
        ehVerifyMetadataUpdate(ehMetadataUpdateResult);

        // on failure to remove all expired keys and expired keys contains one of requested fields, fail
        if (ehMetadataUpdateResult.expiredFields.size() > 0 && ehMetadataUpdateResult.hdelResponse != null &&
                (ehMetadataUpdateResult.expiredFields.size() != ehMetadataUpdateResult.hdelResponse.get()) &&
                Arrays.stream(fields).anyMatch(ehMetadataUpdateResult.expiredFields::contains)) {
            throw new DynoException("Failed to expire hash fields");
        }

        return mgetResponse.get();
    }

    @Override
    public String ehmset(final String key, final Map<String, Pair<String, Long>> hash) {
        final DynoJedisPipeline pipeline = this.pipelined();
        Map<String, String> fields = new HashMap<>();
        Map<String, Double> metadataFields = new HashMap<>();

        hash.keySet().forEach(f -> {
            fields.put(f, hash.get(f).getLeft());
            metadataFields.put(f, timeInEpochSeconds(hash.get(f).getRight()));
        });

        final Response<Long> zResponse = pipeline.zadd(ehashMetadataKey(key), metadataFields,
                ZAddParams.zAddParams().ch());
        final Response<String> hResponse = pipeline.hmset(ehashDataKey(key), fields);
        pipeline.sync();

        return hResponse.get();
    }

    @Override
    public ScanResult<Map.Entry<String, String>> ehscan(final String key, final String cursor) {
        final String dataKey = ehashDataKey(key);

        final DynoJedisPipeline pipeline = this.pipelined();

        EHMetadataUpdateResult ehMetadataUpdateResult = ehPurgeExpiredFields(pipeline, key);

        if (ehMetadataUpdateResult.expiredFields.size() > 0) {
            pipeline.sync();
        } else {
            pipeline.discardPipelineAndReleaseConnection();
        }

        // verify if all expired fields were removed from data and metadata
        ehVerifyMetadataUpdate(ehMetadataUpdateResult);

        // on failure to remove all expired keys, fail
        if (ehMetadataUpdateResult.expiredFields.size() > 0 && ehMetadataUpdateResult.hdelResponse != null &&
                (ehMetadataUpdateResult.expiredFields.size() != ehMetadataUpdateResult.hdelResponse.get())) {
            throw new DynoException("Failed to expire hash fields");
        }

        return hscan(dataKey, cursor);
    }

    @Override
    public Long ehincrby(final String key, final String field, final long value) {
        final String dataKey = ehashDataKey(key);
        final DynoJedisPipeline pipeline = this.pipelined();

        EHMetadataUpdateResult ehMetadataUpdateResult = ehPurgeExpiredFields(pipeline, key);
        final Response<Long> incrbyResponse = pipeline.hincrBy(dataKey, field, value);
        pipeline.sync();

        // verify if all expired fields were removed from data and metadata
        ehVerifyMetadataUpdate(ehMetadataUpdateResult);

        // on failure to remove all expired keys and expired keys contains requested field, fail
        if (ehMetadataUpdateResult.expiredFields.size() > 0 && ehMetadataUpdateResult.hdelResponse != null &&
                (ehMetadataUpdateResult.expiredFields.size() != ehMetadataUpdateResult.hdelResponse.get()) &&
                ehMetadataUpdateResult.expiredFields.contains(field)) {
            throw new DynoException("Failed to expire hash fields");
        }

        return incrbyResponse.get();
    }

    @Override
    public Double ehincrbyfloat(final String key, final String field, final double value) {
        final String dataKey = ehashDataKey(key);
        final DynoJedisPipeline pipeline = this.pipelined();

        EHMetadataUpdateResult ehMetadataUpdateResult = ehPurgeExpiredFields(pipeline, key);
        final Response<Double> incrbyFloatResponse = pipeline.hincrByFloat(dataKey, field, value);
        pipeline.sync();

        // verify if all expired fields were removed from data and metadata
        ehVerifyMetadataUpdate(ehMetadataUpdateResult);

        // on failure to remove all expired keys and expired keys contains requested field, fail
        if (ehMetadataUpdateResult.expiredFields.size() > 0 && ehMetadataUpdateResult.hdelResponse != null &&
                (ehMetadataUpdateResult.expiredFields.size() != ehMetadataUpdateResult.hdelResponse.get()) &&
                ehMetadataUpdateResult.expiredFields.contains(field)) {
            throw new DynoException("Failed to expire hash fields");
        }

        return incrbyFloatResponse.get();
    }

    @Override
    public Long ehlen(final String key) {
        final String dataKey = ehashDataKey(key);
        final DynoJedisPipeline pipeline = this.pipelined();

        EHMetadataUpdateResult ehMetadataUpdateResult = ehPurgeExpiredFields(pipeline, key);
        final Response<Long> hlenResponse = pipeline.hlen(dataKey);
        pipeline.sync();

        // verify if all expired fields were removed from data and metadata
        ehVerifyMetadataUpdate(ehMetadataUpdateResult);

        // on failure to remove all expired keys, fail
        if (ehMetadataUpdateResult.expiredFields.size() > 0 && ehMetadataUpdateResult.hdelResponse != null &&
                (ehMetadataUpdateResult.expiredFields.size() != ehMetadataUpdateResult.hdelResponse.get())) {
            throw new DynoException("Failed to expire hash fields");
        }

        return hlenResponse.get();
    }

    @Override
    public String ehrename(final String oldKey, final String newKey) {
        final String dataOldKey = ehashDataKey(oldKey);
        final String dataNewKey = ehashDataKey(newKey);
        final String metadataOldKey = ehashMetadataKey(oldKey);
        final String metadataNewKey = ehashMetadataKey(newKey);
        final DynoJedisPipeline pipeline = this.pipelined();

        final Response<String> zrenameResponse = pipeline.rename(metadataOldKey, metadataNewKey);
        final Response<String> hrenameResponse = pipeline.rename(dataOldKey, dataNewKey);
        pipeline.sync();

        if (zrenameResponse.get().compareTo("OK") != 0) {
            rename(dataNewKey, dataOldKey);
            throw new DynoException("Unable to rename key: " + metadataOldKey + " to key:" + metadataNewKey);
        }

        return hrenameResponse.get();
    }

    @Override
    public Long ehrenamenx(final String oldKey, final String newKey) {
        final String dataOldKey = ehashDataKey(oldKey);
        final String dataNewKey = ehashDataKey(newKey);
        final String metadataOldKey = ehashMetadataKey(oldKey);
        final String metadataNewKey = ehashMetadataKey(newKey);
        final DynoJedisPipeline pipeline = this.pipelined();

        final Response<Long> zrenamenxResponse = pipeline.renamenx(metadataOldKey, metadataNewKey);
        final Response<Long> hrenamenxResponse = pipeline.renamenx(dataOldKey, dataNewKey);
        pipeline.sync();

        if (zrenamenxResponse.get() != 1 && hrenamenxResponse.get() == 1) {
            rename(dataNewKey, dataOldKey);
            throw new DynoException("Unable to rename key: " + metadataOldKey + " to key:" + metadataNewKey);
        }

        return hrenamenxResponse.get();
    }

    @Override
    public Long ehexpire(final String key, final int seconds) {
        final String dataKey = ehashDataKey(key);
        final String metadataKey = ehashMetadataKey(key);
        final DynoJedisPipeline pipeline = this.pipelined();

        Response<Long> metadataExpireResponse = pipeline.expire(metadataKey, seconds);
        Response<Long> dataExpireResponse = pipeline.expire(dataKey, seconds);
        pipeline.sync();

        if (metadataExpireResponse.get().compareTo(dataExpireResponse.get()) != 0) {
            throw new DynoException("Metadata and data timeout do not match");
        }

        return dataExpireResponse.get();
    }

    @Override
    public Long ehexpireat(final String key, final long timestamp) {
        final String dataKey = ehashDataKey(key);
        final String metadataKey = ehashMetadataKey(key);
        final DynoJedisPipeline pipeline = this.pipelined();

        Response<Long> metadataExpireResponse = pipeline.expireAt(metadataKey, timestamp);
        Response<Long> dataExpireResponse = pipeline.expireAt(dataKey, timestamp);
        pipeline.sync();

        if (metadataExpireResponse.get().compareTo(dataExpireResponse.get()) != 0) {
            throw new DynoException("Metadata and data timeout do not match");
        }

        return dataExpireResponse.get();
    }

    @Override
    public Long ehpexpireat(final String key, final long timestamp) {
        final String dataKey = ehashDataKey(key);
        final String metadataKey = ehashMetadataKey(key);
        final DynoJedisPipeline pipeline = this.pipelined();

        Response<Long> metadataExpireResponse = pipeline.pexpireAt(metadataKey, timestamp);
        Response<Long> dataExpireResponse = pipeline.pexpireAt(dataKey, timestamp);
        pipeline.sync();

        if (metadataExpireResponse.get().compareTo(dataExpireResponse.get()) != 0) {
            throw new DynoException("Metadata and data timeout do not match");
        }

        return dataExpireResponse.get();
    }

    @Override
    public Long ehpersist(final String key) {
        final String dataKey = ehashDataKey(key);
        final String metadataKey = ehashMetadataKey(key);
        final DynoJedisPipeline pipeline = this.pipelined();

        Response<Long> metadataPersistResponse = pipeline.persist(metadataKey);
        Response<Long> dataPersistResponse = pipeline.persist(dataKey);
        pipeline.sync();

        if (metadataPersistResponse.get().compareTo(dataPersistResponse.get()) != 0) {
            throw new DynoException("Metadata and data expiry do not match");
        }

        return dataPersistResponse.get();
    }

    @Override
    public Long ehttl(final String key) {
        return ttl(ehashDataKey(key));
    }

    @Override
    public Long ehttl(final String key, final String field) {
        double now = timeInEpochSeconds(0);
        final String metadataKey = ehashMetadataKey(key);
        final DynoJedisPipeline pipeline = this.pipelined();

        EHMetadataUpdateResult ehMetadataUpdateResult = ehPurgeExpiredFields(pipeline, key);
        final Response<Double> zscoreResponse = pipeline.zscore(metadataKey, field);
        pipeline.sync();

        // verify if all expired fields were removed from data and metadata
        ehVerifyMetadataUpdate(ehMetadataUpdateResult);

        // on failure to remove all expired keys and expired keys contains requested field, fail
        if (ehMetadataUpdateResult.expiredFields.size() > 0 && ehMetadataUpdateResult.hdelResponse != null &&
                (ehMetadataUpdateResult.expiredFields.size() != ehMetadataUpdateResult.hdelResponse.get()) &&
                ehMetadataUpdateResult.expiredFields.contains(field)) {
            throw new DynoException("Failed to expire hash fields");
        }

        if (zscoreResponse.get() > 0) {
            return zscoreResponse.get().longValue() - (long) now;
        } else {
            return zscoreResponse.get().longValue();
        }
    }

    @Override
    public Long ehpttl(final String key) {
        return pttl(ehashDataKey(key));
    }

    @Override
    public Long ehpttl(final String key, final String field) {
        return ehttl(key, field);
    }

    public void stopClient() {
        if (pipelineMonitor.get() != null) {
            pipelineMonitor.get().stop();
        }

        this.connPool.shutdown();
    }

    public DynoJedisPipeline pipelined() {
        return new DynoJedisPipeline(getConnPool(), checkAndInitPipelineMonitor(), getConnPool().getMonitor());
    }

    private DynoJedisPipelineMonitor checkAndInitPipelineMonitor() {

        if (pipelineMonitor.get() != null) {
            return pipelineMonitor.get();
        }

        int flushTimerFrequency = this.connPool.getConfiguration().getTimingCountersResetFrequencySeconds();
        DynoJedisPipelineMonitor plMonitor = new DynoJedisPipelineMonitor(appName, flushTimerFrequency);
        boolean success = pipelineMonitor.compareAndSet(null, plMonitor);
        if (success) {
            pipelineMonitor.get().init();
        }
        return pipelineMonitor.get();
    }

    public static class Builder {

        private String appName;
        private String clusterName;
        private ConnectionPoolConfigurationImpl cpConfig;
        private HostSupplier hostSupplier;
        private EurekaClient discoveryClient;
        private String dualWriteClusterName;
        private HostSupplier dualWriteHostSupplier;
        private DynoDualWriterClient.Dial dualWriteDial;
        private ConnectionPoolMonitor cpMonitor;
        private SSLSocketFactory sslSocketFactory;
        private TokenMapSupplier tokenMapSupplier;
        private TokenMapSupplier dualWriteTokenMapSupplier;
        private boolean isDatastoreClient;
        private String connectionPoolConsistency;

        public Builder() {
        }

        public Builder withApplicationName(String applicationName) {
            appName = applicationName;
            return this;
        }

        public Builder withDynomiteClusterName(String cluster) {
            clusterName = cluster;
            return this;
        }

        public Builder withCPConfig(ConnectionPoolConfigurationImpl config) {
            cpConfig = config;
            return this;
        }

        public Builder withHostSupplier(HostSupplier hSupplier) {
            hostSupplier = hSupplier;
            return this;
        }

        public Builder withTokenMapSupplier(TokenMapSupplier tokenMapSupplier) {
            this.tokenMapSupplier = tokenMapSupplier;
            return this;
        }

        @Deprecated
        public Builder withDiscoveryClient(DiscoveryClient client) {
            discoveryClient = client;
            return this;
        }

        public Builder withDiscoveryClient(EurekaClient client) {
            discoveryClient = client;
            return this;
        }

        public Builder withDualWriteClusterName(String dualWriteCluster) {
            dualWriteClusterName = dualWriteCluster;
            return this;
        }

        public Builder withDualWriteHostSupplier(HostSupplier dualWriteHostSupplier) {
            this.dualWriteHostSupplier = dualWriteHostSupplier;
            return this;
        }

        public Builder withDualWriteTokenMapSupplier(TokenMapSupplier dualWriteTokenMapSupplier) {
            this.dualWriteTokenMapSupplier = dualWriteTokenMapSupplier;
            return this;
        }

        public Builder withDualWriteDial(DynoDualWriterClient.Dial dial) {
            this.dualWriteDial = dial;
            return this;
        }

        public Builder withConnectionPoolMonitor(ConnectionPoolMonitor cpMonitor) {
            this.cpMonitor = cpMonitor;
            return this;
        }

        public Builder withSSLSocketFactory(SSLSocketFactory sslSocketFactory) {
            this.sslSocketFactory = sslSocketFactory;
            return this;
        }

        public Builder isDatastoreClient(boolean isDatastoreClient) {
            this.isDatastoreClient = isDatastoreClient;
            return this;
        }

        public Builder withConnectionPoolConsistency(String consistency) {
            this.connectionPoolConsistency = consistency;
            return this;
        }

        public DynoJedisClient build() {
            assert (appName != null);
            assert (clusterName != null);

            // Make sure that the user doesn't set isDatastoreClient and connectionPoolConsistency together.
            if (this.isDatastoreClient == true && this.connectionPoolConsistency != null) {
                throw new DynoException("Cannot set isDatastoreClient(true) and also set withConnectionPoolConsistency() together");
            }

            ArchaiusConnectionPoolConfiguration archaiusConfig = new ArchaiusConnectionPoolConfiguration(appName);
            if (cpConfig == null) {
                cpConfig = archaiusConfig;
                Logger.info("Dyno Client runtime properties: " + cpConfig.toString());
            } else {
                // Based on current requirements, we currently only want to prioritize pulling in the following FPs
                // if provided:
                // 'dualwrite.enabled', 'dualwrite.cluster', 'dualwrite.percentage'
                // TODO: Move to a clean generic userconfig + FP model.
                if (!cpConfig.isDualWriteEnabled() && archaiusConfig.isDualWriteEnabled()) {
                    // If a user sets these configs explicitly, they take precedence over the FP values.
                    if (cpConfig.getDualWriteClusterName() == null) {
                        cpConfig.setDualWriteClusterName(archaiusConfig.getDualWriteClusterName());
                    }
                    if (cpConfig.getDualWritePercentage() == 0) {
                        cpConfig.setDualWritePercentage(archaiusConfig.getDualWritePercentage());
                    }
                    cpConfig.setDualWriteEnabled(true);
                }
            }
            cpConfig.setConnectToDatastore(isDatastoreClient);

            // If a connection-pool level consistency setting was provided, add it here.
            if (this.connectionPoolConsistency != null) {
                cpConfig.setConnectionPoolConsistency(connectionPoolConsistency);
            }

            if (cpConfig.isDualWriteEnabled()) {
                return buildDynoDualWriterClient();
            } else {
                return buildDynoJedisClient();
            }
        }

        private DynoDualWriterClient buildDynoDualWriterClient() {
            ConnectionPoolConfigurationImpl shadowConfig = new ConnectionPoolConfigurationImpl(cpConfig);
            Logger.info("Dyno Client Shadow Config runtime properties: " + shadowConfig.toString());

            // Ensure that if the shadow cluster is down it will not block
            // client application startup
            shadowConfig.setFailOnStartupIfNoHosts(false);

            //Initialize the Host Supplier
            HostSupplier shadowSupplier;
            if (dualWriteHostSupplier == null) {
                if (hostSupplier != null && hostSupplier instanceof EurekaHostsSupplier) {
                    EurekaHostsSupplier eurekaSupplier = (EurekaHostsSupplier) hostSupplier;
                    shadowSupplier = EurekaHostsSupplier.newInstance(shadowConfig.getDualWriteClusterName(),
                            eurekaSupplier);
                } else if (discoveryClient != null) {
                    shadowSupplier = new EurekaHostsSupplier(shadowConfig.getDualWriteClusterName(), discoveryClient);
                } else {
                    throw new DynoConnectException("HostSupplier for DualWrite cluster is REQUIRED if you are not "
                            + "using EurekaHostsSupplier implementation or using a EurekaClient");
                }
            } else {
                shadowSupplier = dualWriteHostSupplier;
            }

            shadowConfig.withHostSupplier(shadowSupplier);

            if (dualWriteTokenMapSupplier != null)
                shadowConfig.withTokenSupplier(dualWriteTokenMapSupplier);

            String shadowAppName = shadowConfig.getName();
            DynoCPMonitor shadowCPMonitor = new DynoCPMonitor(shadowAppName);
            DynoOPMonitor shadowOPMonitor = new DynoOPMonitor(shadowAppName);

            DynoJedisUtils.updateConnectionPoolConfig(shadowConfig, shadowSupplier, dualWriteTokenMapSupplier, discoveryClient, clusterName);
            final ConnectionPool<Jedis> shadowPool = DynoJedisUtils.createConnectionPool(shadowAppName, shadowOPMonitor, shadowCPMonitor, shadowConfig,
                    sslSocketFactory);

            // Construct a connection pool with the shadow cluster settings
            DynoJedisClient shadowClient = new DynoJedisClient(shadowAppName, dualWriteClusterName, shadowPool,
                    shadowOPMonitor, shadowCPMonitor);

            // Construct an instance of our DualWriter client
            DynoOPMonitor opMonitor = new DynoOPMonitor(appName);
            ConnectionPoolMonitor cpMonitor = (this.cpMonitor == null) ? new DynoCPMonitor(appName) : this.cpMonitor;

            DynoJedisUtils.updateConnectionPoolConfig(cpConfig, dualWriteHostSupplier, dualWriteTokenMapSupplier, discoveryClient, clusterName);

            final ConnectionPool<Jedis> pool = DynoJedisUtils.createConnectionPool(appName, opMonitor, cpMonitor, cpConfig, sslSocketFactory);

            if (dualWriteDial != null) {
                if (shadowConfig.getDualWritePercentage() > 0) {
                    dualWriteDial.setRange(shadowConfig.getDualWritePercentage());
                }

                return new DynoDualWriterClient(appName, clusterName, pool, opMonitor, cpMonitor, shadowClient,
                        dualWriteDial);
            } else {
                return new DynoDualWriterClient(appName, clusterName, pool, opMonitor, cpMonitor, shadowClient);
            }
        }

        private DynoJedisClient buildDynoJedisClient() {
            DynoOPMonitor opMonitor = new DynoOPMonitor(appName);
            ConnectionPoolMonitor cpMonitor = (this.cpMonitor == null) ? new DynoCPMonitor(appName) : this.cpMonitor;

            DynoJedisUtils.updateConnectionPoolConfig(cpConfig, hostSupplier, tokenMapSupplier, discoveryClient,
                    clusterName);
            final ConnectionPool<Jedis> pool = DynoJedisUtils.createConnectionPool(appName, opMonitor, cpMonitor,
                    cpConfig, sslSocketFactory);

            return new DynoJedisClient(appName, clusterName, pool, opMonitor, cpMonitor);
        }

    }

    /**
     * Used for unit testing ONLY
     */
    /* package */ static class TestBuilder {
        private ConnectionPool cp;
        private String appName;

        public TestBuilder withAppname(String appName) {
            this.appName = appName;
            return this;
        }

        public TestBuilder withConnectionPool(ConnectionPool cp) {
            this.cp = cp;
            return this;
        }

        public DynoJedisClient build() {
            return new DynoJedisClient(appName, "TestCluster", cp, null, null);
        }

    }

}
