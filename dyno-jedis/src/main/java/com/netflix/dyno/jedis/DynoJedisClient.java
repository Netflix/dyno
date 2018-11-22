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
import com.netflix.dyno.connectionpool.*;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.connectionpool.impl.lb.HttpEndpointBasedTokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.ZipUtils;
import com.netflix.dyno.contrib.ArchaiusConnectionPoolConfiguration;
import com.netflix.dyno.contrib.DynoCPMonitor;
import com.netflix.dyno.contrib.DynoOPMonitor;
import com.netflix.dyno.contrib.EurekaHostsSupplier;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import redis.clients.jedis.*;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.params.geo.GeoRadiusParam;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.jedis.params.sortedset.ZIncrByParams;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;

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

    private abstract class BaseKeyOperation<T> implements Operation<Jedis, T> {

        private final String key;
        private final byte[] binaryKey;
        private final OpName op;

        private BaseKeyOperation(final String k, final OpName o) {
            this.key = k;
            this.binaryKey = null;
            this.op = o;
        }

        private BaseKeyOperation(final byte[] k, final OpName o) {
            this.key = null;
            this.binaryKey = k;
            this.op = o;
        }

        @Override
        public String getName() {
            return op.name();
        }

        @Override
        public String getStringKey() {
            return this.key;
        }

        public byte[] getBinaryKey() {
            return this.binaryKey;
        }

    }

    /**
     * A poor man's solution for multikey operation. This is similar to
     * basekeyoperation just that it takes a list of keys as arguments. For
     * token aware, we just use the first key in the list. Ideally we should be
     * doing a scatter gather
     */
    private abstract class MultiKeyOperation<T> implements Operation<Jedis, T> {

        private final List<String> keys;
        private final List<byte[]> binaryKeys;
        private final OpName op;

        private MultiKeyOperation(final List keys, final OpName o) {
            Object firstKey = (keys != null && keys.size() > 0) ? keys.get(0) : null;

            if(firstKey != null) {
                if (firstKey instanceof String) {//string key
                    this.keys = keys;
                    this.binaryKeys = null;
                } else if (firstKey instanceof byte[]) {//binary key
                    this.keys = null;
                    this.binaryKeys = keys;
                } else {//something went wrong here
                    this.keys = null;
                    this.binaryKeys = null;
                }
            } else {
                this.keys = null;
                this.binaryKeys = null;
            }

            this.op = o;
        }

        @Override
        public String getName() {
            return op.name();
        }

        @Override
        public String getStringKey() {
            return (this.keys != null) ? this.keys.get(0) : null;
        }

        public byte[] getBinaryKey() {
            return (binaryKeys != null) ? binaryKeys.get(0) : null;
        }

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
     * @param <T>
     *            the parameterized type
     */
    private abstract class CompressionValueOperation<T> extends BaseKeyOperation<T>
            implements CompressionOperation<Jedis, T> {

        private CompressionValueOperation(String k, OpName o) {
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
     * @param <T>
     *            the parameterized type
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
           	 
              for (int i = 0 ; i < items.size() ; i++) {
            	 /*
            	  * String... keyValues is a List of keys and values.
            	  * The value always comes second and this is the one
            	  * we want to compress. 
            	  */
             	 if(i % 2 == 0 ) {
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
                 }
             	 else {
             		 newItems.add(items.get(i));
             	 }
              }
            return (String[]) newItems.toArray();
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

    }

    public TopologyView getTopologyView() {
        return this.getConnPool();
    }

    @Override
    public Long append(final String key, final String value) {
        return d_append(key, value).getResult();
    }

    public OperationResult<Long> d_append(final String key, final String value) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.APPEND) {
            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.append(key, value);
            }
        });
    }

    @Override
    public Long decr(final String key) {
        return d_decr(key).getResult();
    }

    public OperationResult<Long> d_decr(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.DECR) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.decr(key);
            }

        });
    }

    @Override
    public Long decrBy(final String key, final long delta) {
        return d_decrBy(key, delta).getResult();
    }

    public OperationResult<Long> d_decrBy(final String key, final Long delta) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.DECRBY) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.decrBy(key, delta);
            }

        });
    }

    @Override
    public Long del(final String key) {
        return d_del(key).getResult();
    }

    public OperationResult<Long> d_del(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.DEL) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.del(key);
            }

        });
    }

    public byte[] dump(final String key) {
        return d_dump(key).getResult();
    }

    public OperationResult<byte[]> d_dump(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<byte[]>(key, OpName.DUMP) {

            @Override
            public byte[] execute(Jedis client, ConnectionContext state) {
                return client.dump(key);
            }

        });
    }

    @Override
    public Boolean exists(final String key) {
        return d_exists(key).getResult();
    }

    public OperationResult<Boolean> d_exists(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.EXISTS) {

            @Override
            public Boolean execute(Jedis client, ConnectionContext state) {
                return client.exists(key);
            }

        });
    }

    @Override
    public Long expire(final String key, final int seconds) {
        return d_expire(key, seconds).getResult();
    }

    public OperationResult<Long> d_expire(final String key, final int seconds) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.EXPIRE) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.expire(key, seconds);
            }
        });
    }

    @Override
    public Long expireAt(final String key, final long unixTime) {
        return d_expireAt(key, unixTime).getResult();
    }

    public OperationResult<Long> d_expireAt(final String key, final long unixTime) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.EXPIREAT) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.expireAt(key, unixTime);
            }

        });
    }

    @Override
    public Object eval(String script, int keyCount, String... params) { return d_eval(script, keyCount, params).getResult(); }

    public OperationResult<Object> d_eval(final String script, final int keyCount, final String... params) {
        if (keyCount == 0) {
            throw new DynoException("Need at least one key in script");
        }
        return connPool.executeWithFailover(new BaseKeyOperation<Object>(params[0], OpName.EVAL) {
            @Override
            public Object execute(Jedis client, ConnectionContext state) {
                return client.eval(script, keyCount, params);
            }
        });
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
        throw new UnsupportedOperationException("This function is Not Implemented. Please use eval instead.");
    }

    @Override
    public Object evalsha(String sha1, List<String> keys, List<String> args) {
        throw new UnsupportedOperationException("This function is Not Implemented. Please use eval instead.");
    }

    @Override
    public Object evalsha(String script) {
        throw new UnsupportedOperationException("This function is Not Implemented. Please use eval instead.");
    }

    @Override
    public Boolean scriptExists(String sha1) {
        throw new UnsupportedOperationException("This function is Not Implemented");
    }

    @Override
    public List<Boolean> scriptExists(String... sha1) {
        throw new UnsupportedOperationException("This function is Not Implemented");
    }

    @Override
    public String scriptLoad(String script) {
        throw new UnsupportedOperationException("This function is Not Implemented");
    }

    @Override
    public String get(final String key) {
        return d_get(key).getResult();
    }

    public OperationResult<String> d_get(final String key) {

        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.GET) {
                @Override
                public String execute(Jedis client, ConnectionContext state) throws DynoException {
                    return client.get(key);
                }
            });
        } else {
            return connPool.executeWithFailover(new CompressionValueOperation<String>(key, OpName.GET) {
                @Override
                public String execute(final Jedis client, final ConnectionContext state) throws DynoException {
                    return decompressValue(client.get(key), state);
                }
            });
        }
    }

    @Override
    public Boolean getbit(final String key, final long offset) {
        return d_getbit(key, offset).getResult();
    }

    public OperationResult<Boolean> d_getbit(final String key, final Long offset) {

        return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.GETBIT) {

            @Override
            public Boolean execute(Jedis client, ConnectionContext state) {
                return client.getbit(key, offset);
            }

        });
    }

    @Override
    public String getrange(final String key, final long startOffset, final long endOffset) {
        return d_getrange(key, startOffset, endOffset).getResult();
    }

    public OperationResult<String> d_getrange(final String key, final Long startOffset, final Long endOffset) {

        return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.GETRANGE) {

            @Override
            public String execute(Jedis client, ConnectionContext state) {
                return client.getrange(key, startOffset, endOffset);
            }

        });
    }

    @Override
    public String getSet(final String key, final String value) {
        return d_getSet(key, value).getResult();
    }

    public OperationResult<String> d_getSet(final String key, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.GETSET) {
                @Override
                public String execute(Jedis client, ConnectionContext state) throws DynoException {
                    return client.getSet(key, value);
                }
            });
        } else {
            return connPool.executeWithFailover(new CompressionValueOperation<String>(key, OpName.GETSET) {
                @Override
                public String execute(Jedis client, ConnectionContext state) throws DynoException {
                    return decompressValue(client.getSet(key, compressValue(value, state)), state);
                }
            });
        }
    }

    @Override
    public Long hdel(final String key, final String... fields) {
        return d_hdel(key, fields).getResult();
    }

    public OperationResult<Long> d_hdel(final String key, final String... fields) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HDEL) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.hdel(key, fields);
            }

        });
    }

    @Override
    public Boolean hexists(final String key, final String field) {
        return d_hexists(key, field).getResult();
    }

    public OperationResult<Boolean> d_hexists(final String key, final String field) {

        return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.HEXISTS) {

            @Override
            public Boolean execute(Jedis client, ConnectionContext state) {
                return client.hexists(key, field);
            }

        });
    }

    @Override
    public String hget(final String key, final String field) {
        return d_hget(key, field).getResult();
    }

    public OperationResult<String> d_hget(final String key, final String field) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.HGET) {
                @Override
                public String execute(Jedis client, ConnectionContext state) throws DynoException {
                    return client.hget(key, field);
                }
            });
        } else {
            return connPool.executeWithFailover(new CompressionValueOperation<String>(key, OpName.HGET) {
                @Override
                public String execute(final Jedis client, final ConnectionContext state) throws DynoException {
                    return decompressValue(client.hget(key, field), state);
                }
            });
        }
    }

    @Override
    public Map<String, String> hgetAll(final String key) {
        return d_hgetAll(key).getResult();
    }

    public OperationResult<Map<String, String>> d_hgetAll(final String key) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return connPool.executeWithFailover(new BaseKeyOperation<Map<String, String>>(key, OpName.HGETALL) {
                @Override
                public Map<String, String> execute(Jedis client, ConnectionContext state) throws DynoException {
                    return client.hgetAll(key);
                }
            });
        } else {
            return connPool
                    .executeWithFailover(new CompressionValueOperation<Map<String, String>>(key, OpName.HGETALL) {
                        @Override
                        public Map<String, String> execute(final Jedis client, final ConnectionContext state) {
                            return CollectionUtils.transform(client.hgetAll(key),
                                    new CollectionUtils.MapEntryTransform<String, String, String>() {
                                        @Override
                                        public String get(String key, String val) {
                                            return decompressValue(val, state);
                                        }
                                    });
                        }
                    });
        }
    }

    @Override
    public Long hincrBy(final String key, final String field, final long value) {
        return d_hincrBy(key, field, value).getResult();
    }

    public OperationResult<Long> d_hincrBy(final String key, final String field, final long value) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HINCRBY) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.hincrBy(key, field, value);
            }

        });
    }

    /* not supported by RedisPipeline 2.7.3 */
    public Double hincrByFloat(final String key, final String field, final double value) {
        return d_hincrByFloat(key, field, value).getResult();
    }

    public OperationResult<Double> d_hincrByFloat(final String key, final String field, final double value) {

        return connPool.executeWithFailover(new BaseKeyOperation<Double>(key, OpName.HINCRBYFLOAT) {

            @Override
            public Double execute(Jedis client, ConnectionContext state) {
                return client.hincrByFloat(key, field, value);
            }

        });
    }

    @Override
    public Long hsetnx(final String key, final String field, final String value) {
        return d_hsetnx(key, field, value).getResult();
    }

    public OperationResult<Long> d_hsetnx(final String key, final String field, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HSETNX) {
                @Override
                public Long execute(Jedis client, ConnectionContext state) {
                    return client.hsetnx(key, field, value);
                }

            });
        } else {
            return connPool.executeWithFailover(new CompressionValueOperation<Long>(key, OpName.HSETNX) {
                @Override
                public Long execute(final Jedis client, final ConnectionContext state) throws DynoException {
                    return client.hsetnx(key, field, compressValue(value, state));
                }
            });
        }
    }

    @Override
    public Set<String> hkeys(final String key) {
        return d_hkeys(key).getResult();
    }

    public OperationResult<Set<String>> d_hkeys(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.HKEYS) {

            @Override
            public Set<String> execute(Jedis client, ConnectionContext state) {
                return client.hkeys(key);
            }

        });
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(final String key, final int cursor) {
        throw new UnsupportedOperationException("This function is deprecated, use hscan(String, String)");
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(final String key, final String cursor) {
        return d_hscan(key, cursor).getResult();
    }

    public OperationResult<ScanResult<Map.Entry<String, String>>> d_hscan(final String key, final String cursor) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return connPool.executeWithFailover(
                    new BaseKeyOperation<ScanResult<Map.Entry<String, String>>>(key, OpName.HSCAN) {
                        @Override
                        public ScanResult<Map.Entry<String, String>> execute(Jedis client, ConnectionContext state) {
                            return client.hscan(key, cursor);
                        }
                    });
        } else {
            return connPool.executeWithFailover(
                    new CompressionValueOperation<ScanResult<Map.Entry<String, String>>>(key, OpName.HSCAN) {
                        @Override
                        public ScanResult<Map.Entry<String, String>> execute(final Jedis client,
                                final ConnectionContext state) {
                            return new ScanResult<>(cursor, new ArrayList(CollectionUtils.transform(
                                    client.hscan(key, cursor).getResult(),
                                    new CollectionUtils.Transform<Map.Entry<String, String>, Map.Entry<String, String>>() {
                                        @Override
                                        public Map.Entry<String, String> get(Map.Entry<String, String> entry) {
                                            entry.setValue(decompressValue(entry.getValue(), state));
                                            return entry;
                                        }
                                    })));
                        }
                    });
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
        return new ArrayList<>(connPool.executeWithRing((TokenRackMapper)cursor, new BaseKeyOperation<ScanResult<String>>("SCAN", OpName.SCAN) {
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
        return d_hlen(key).getResult();
    }

    public OperationResult<Long> d_hlen(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HLEN) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.hlen(key);
            }

        });
    }

    @Override
    public List<String> hmget(final String key, final String... fields) {
        return d_hmget(key, fields).getResult();
    }

    public OperationResult<List<String>> d_hmget(final String key, final String... fields) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return connPool.executeWithFailover(new BaseKeyOperation<List<String>>(key, OpName.HMGET) {
                @Override
                public List<String> execute(Jedis client, ConnectionContext state) {
                    return client.hmget(key, fields);
                }
            });
        } else {
            return connPool.executeWithFailover(new CompressionValueOperation<List<String>>(key, OpName.HMGET) {
                @Override
                public List<String> execute(final Jedis client, final ConnectionContext state) throws DynoException {
                    return new ArrayList<String>(CollectionUtils.transform(client.hmget(key, fields),
                            new CollectionUtils.Transform<String, String>() {
                                @Override
                                public String get(String s) {
                                    return decompressValue(s, state);
                                }
                            }));
                }
            });
        }
    }

    @Override
    public String hmset(final String key, final Map<String, String> hash) {
        return d_hmset(key, hash).getResult();
    }

    public OperationResult<String> d_hmset(final String key, final Map<String, String> hash) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.HMSET) {
                @Override
                public String execute(Jedis client, ConnectionContext state) {
                    return client.hmset(key, hash);
                }
            });
        } else {
            return connPool.executeWithFailover(new CompressionValueOperation<String>(key, OpName.HMSET) {
                @Override
                public String execute(final Jedis client, final ConnectionContext state) throws DynoException {
                    return client.hmset(key, CollectionUtils.transform(hash,
                            new CollectionUtils.MapEntryTransform<String, String, String>() {
                                @Override
                                public String get(String key, String val) {
                                    return compressValue(val, state);
                                }
                            }));
                }
            });
        }
    }

    @Override
    public Long hset(final String key, final String field, final String value) {
        return d_hset(key, field, value).getResult();
    }

    public OperationResult<Long> d_hset(final String key, final String field, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HSET) {
                @Override
                public Long execute(Jedis client, ConnectionContext state) {
                    return client.hset(key, field, value);
                }

            });
        } else {
            return connPool.executeWithFailover(new CompressionValueOperation<Long>(key, OpName.HSET) {
                @Override
                public Long execute(final Jedis client, final ConnectionContext state) throws DynoException {
                    return client.hset(key, field, compressValue(value, state));
                }
            });
        }
    }

    @Override
    public List<String> hvals(final String key) {
        return d_hvals(key).getResult();
    }

    public OperationResult<List<String>> d_hvals(final String key) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return connPool.executeWithFailover(new BaseKeyOperation<List<String>>(key, OpName.HVALS) {
                @Override
                public List<String> execute(Jedis client, ConnectionContext state) {
                    return client.hvals(key);
                }

            });
        } else {
            return connPool.executeWithFailover(new CompressionValueOperation<List<String>>(key, OpName.HVALS) {
                @Override
                public List<String> execute(final Jedis client, final ConnectionContext state) throws DynoException {
                    return new ArrayList<String>(CollectionUtils.transform(client.hvals(key),
                            new CollectionUtils.Transform<String, String>() {
                                @Override
                                public String get(String s) {
                                    return decompressValue(s, state);
                                }
                            }));
                }
            });
        }
    }

    @Override
    public Long incr(final String key) {
        return d_incr(key).getResult();
    }

    public OperationResult<Long> d_incr(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.INCR) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.incr(key);
            }

        });
    }

    @Override
    public Long incrBy(final String key, final long delta) {
        return d_incrBy(key, delta).getResult();
    }

    public OperationResult<Long> d_incrBy(final String key, final Long delta) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.INCRBY) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.incrBy(key, delta);
            }

        });
    }

    public Double incrByFloat(final String key, final double increment) {
        return d_incrByFloat(key, increment).getResult();
    }

    public OperationResult<Double> d_incrByFloat(final String key, final Double increment) {

        return connPool.executeWithFailover(new BaseKeyOperation<Double>(key, OpName.INCRBYFLOAT) {

            @Override
            public Double execute(Jedis client, ConnectionContext state) {
                return client.incrByFloat(key, increment);
            }

        });
    }

    @Override
    public String lindex(final String key, final long index) {
        return d_lindex(key, index).getResult();
    }

    public OperationResult<String> d_lindex(final String key, final Long index) {

        return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.LINDEX) {

            @Override
            public String execute(Jedis client, ConnectionContext state) {
                return client.lindex(key, index);
            }

        });
    }

    @Override
    public Long linsert(final String key, final LIST_POSITION where, final String pivot, final String value) {
        return d_linsert(key, where, pivot, value).getResult();
    }

    public OperationResult<Long> d_linsert(final String key, final LIST_POSITION where, final String pivot,
            final String value) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.LINSERT) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.linsert(key, where, pivot, value);
            }

        });
    }

    @Override
    public Long llen(final String key) {
        return d_llen(key).getResult();
    }

    public OperationResult<Long> d_llen(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.LLEN) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.llen(key);
            }

        });
    }

    @Override
    public String lpop(final String key) {
        return d_lpop(key).getResult();
    }

    public OperationResult<String> d_lpop(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.LPOP) {

            @Override
            public String execute(Jedis client, ConnectionContext state) {
                return client.lpop(key);
            }

        });
    }

    @Override
    public Long lpush(final String key, final String... values) {
        return d_lpush(key, values).getResult();
    }

    public OperationResult<Long> d_lpush(final String key, final String... values) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.LPUSH) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.lpush(key, values);
            }

        });
    }

    @Override
    public Long lpushx(final String key, final String... values) {
        return d_lpushx(key, values).getResult();
    }

    public OperationResult<Long> d_lpushx(final String key, final String... values) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.LPUSHX) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.lpushx(key, values);
            }

        });
    }

    @Override
    public List<String> lrange(final String key, final long start, final long end) {
        return d_lrange(key, start, end).getResult();
    }

    public OperationResult<List<String>> d_lrange(final String key, final Long start, final Long end) {

        return connPool.executeWithFailover(new BaseKeyOperation<List<String>>(key, OpName.LRANGE) {

            @Override
            public List<String> execute(Jedis client, ConnectionContext state) {
                return client.lrange(key, start, end);
            }

        });
    }

    @Override
    public Long lrem(final String key, final long count, final String value) {
        return d_lrem(key, count, value).getResult();
    }

    public OperationResult<Long> d_lrem(final String key, final Long count, final String value) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.LREM) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.lrem(key, count, value);
            }

        });
    }

    @Override
    public String lset(final String key, final long index, final String value) {
        return d_lset(key, index, value).getResult();
    }

    public OperationResult<String> d_lset(final String key, final Long index, final String value) {

        return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.LSET) {

            @Override
            public String execute(Jedis client, ConnectionContext state) {
                return client.lset(key, index, value);
            }

        });
    }

    @Override
    public String ltrim(final String key, final long start, final long end) {
        return d_ltrim(key, start, end).getResult();
    }

    public OperationResult<String> d_ltrim(final String key, final long start, final long end) {

        return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.LTRIM) {

            @Override
            public String execute(Jedis client, ConnectionContext state) {
                return client.ltrim(key, start, end);
            }

        });
    }

    @Override
    public Long persist(final String key) {
        return d_persist(key).getResult();
    }

    public OperationResult<Long> d_persist(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.PERSIST) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.persist(key);
            }

        });
    }

    public Long pexpireAt(final String key, final long millisecondsTimestamp) {
        return d_pexpireAt(key, millisecondsTimestamp).getResult();
    }

    public OperationResult<Long> d_pexpireAt(final String key, final Long millisecondsTimestamp) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.PEXPIREAT) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.pexpireAt(key, millisecondsTimestamp);
            }

        });
    }

    public Long pttl(final String key) {
        return d_pttl(key).getResult();
    }

    public OperationResult<Long> d_pttl(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.PTTL) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.pttl(key);
            }

        });
    }

    @Override
    public String rename(String oldkey, String newkey) {
        return d_rename(oldkey, newkey).getResult();
    }

    public OperationResult<String> d_rename(final String oldkey, final String newkey) {

        return connPool.executeWithFailover(new BaseKeyOperation<String>(oldkey, OpName.RENAME) {

            @Override
            public String execute(Jedis client, ConnectionContext state) {
                return client.rename(oldkey, newkey);
            }

        });
    }

    @Override
    public Long renamenx(String oldkey, String newkey) {
        return d_renamenx(oldkey, newkey).getResult();
    }

    public OperationResult<Long> d_renamenx(final String oldkey, final String newkey) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(oldkey, OpName.RENAMENX) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.renamenx(oldkey, newkey);
            }

        });
    }

    public String restore(final String key, final Integer ttl, final byte[] serializedValue) {
        return d_restore(key, ttl, serializedValue).getResult();
    }

    public OperationResult<String> d_restore(final String key, final Integer ttl, final byte[] serializedValue) {

        return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.RESTORE) {

            @Override
            public String execute(Jedis client, ConnectionContext state) {
                return client.restore(key, ttl, serializedValue);
            }

        });
    }

    public String rpop(final String key) {
        return d_rpop(key).getResult();
    }

    public OperationResult<String> d_rpop(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.RPOP) {

            @Override
            public String execute(Jedis client, ConnectionContext state) {
                return client.rpop(key);
            }

        });
    }

    public String rpoplpush(final String srckey, final String dstkey) {
        return d_rpoplpush(srckey, dstkey).getResult();
    }

    public OperationResult<String> d_rpoplpush(final String srckey, final String dstkey) {

        return connPool.executeWithFailover(new BaseKeyOperation<String>(srckey, OpName.RPOPLPUSH) {

            @Override
            public String execute(Jedis client, ConnectionContext state) {
                return client.rpoplpush(srckey, dstkey);
            }

        });
    }

    public Long rpush(final String key, final String... values) {
        return d_rpush(key, values).getResult();
    }

    public OperationResult<Long> d_rpush(final String key, final String... values) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.RPUSH) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.rpush(key, values);
            }

        });
    }

    @Override
    public Long rpushx(final String key, final String... values) {
        return d_rpushx(key, values).getResult();
    }

    public OperationResult<Long> d_rpushx(final String key, final String... values) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.RPUSHX) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.rpushx(key, values);
            }

        });
    }

    @Override
    public Long sadd(final String key, final String... members) {
        return d_sadd(key, members).getResult();
    }

    public OperationResult<Long> d_sadd(final String key, final String... members) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SADD) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.sadd(key, members);
            }

        });
    }

    @Override
    public Long scard(final String key) {
        return d_scard(key).getResult();
    }

    public OperationResult<Long> d_scard(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SCARD) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.scard(key);
            }

        });
    }

    public Set<String> sdiff(final String... keys) {
        return d_sdiff(keys).getResult();
    }

    public OperationResult<Set<String>> d_sdiff(final String... keys) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(keys[0], OpName.SDIFF) {

            @Override
            public Set<String> execute(Jedis client, ConnectionContext state) {
                return client.sdiff(keys);
            }

        });
    }

    public Long sdiffstore(final String dstkey, final String... keys) {
        return d_sdiffstore(dstkey, keys).getResult();
    }

    public OperationResult<Long> d_sdiffstore(final String dstkey, final String... keys) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(dstkey, OpName.SDIFFSTORE) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.sdiffstore(dstkey, keys);
            }

        });
    }

    @Override
    public String set(final String key, final String value) {
        return d_set(key, value).getResult();
    }

    public OperationResult<String> d_set(final String key, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.SET) {
                @Override
                public String execute(Jedis client, ConnectionContext state) throws DynoException {
                    return client.set(key, value);
                }
            });
        } else {
            return connPool.executeWithFailover(new CompressionValueOperation<String>(key, OpName.SET) {
                @Override
                public String execute(final Jedis client, final ConnectionContext state) throws DynoException {
                    return client.set(key, compressValue(value, state));
                }
            });
        }

    }

    @Override
    public String set(final String key, final String value, final String nxxx, final String expx, final long time) {
        return d_set(key, value, nxxx, expx, time).getResult();
    }

    public OperationResult<String> d_set(final String key, final String value, final String nxxx, final String expx,
            final long time) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.SET) {
                @Override
                public String execute(Jedis client, ConnectionContext state) throws DynoException {
                    return client.set(key, value, nxxx, expx, time);
                }
            });
        } else {
            return connPool.executeWithFailover(new CompressionValueOperation<String>(key, OpName.SET) {
                @Override
                public String execute(final Jedis client, final ConnectionContext state) throws DynoException {
                    return client.set(key, compressValue(value, state), nxxx, expx, time);
                }
            });
        }
    }

    @Override
    public Boolean setbit(final String key, final long offset, final boolean value) {
        return d_setbit(key, offset, value).getResult();
    }

    public OperationResult<Boolean> d_setbit(final String key, final Long offset, final Boolean value) {

        return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.SETBIT) {

            @Override
            public Boolean execute(Jedis client, ConnectionContext state) {
                return client.setbit(key, offset, value);
            }

        });
    }

    @Override
    public Boolean setbit(final String key, final long offset, final String value) {
        return d_setbit(key, offset, value).getResult();
    }

    public OperationResult<Boolean> d_setbit(final String key, final Long offset, final String value) {

        return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.SETBIT) {

            @Override
            public Boolean execute(Jedis client, ConnectionContext state) {
                return client.setbit(key, offset, value);
            }

        });
    }

    @Override
    public String setex(final String key, final int seconds, final String value) {
        return d_setex(key, seconds, value).getResult();
    }

    public OperationResult<String> d_setex(final String key, final Integer seconds, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.SETEX) {
                @Override
                public String execute(Jedis client, ConnectionContext state) throws DynoException {
                    return client.setex(key, seconds, value);
                }
            });
        } else {
            return connPool.executeWithFailover(new CompressionValueOperation<String>(key, OpName.SETEX) {
                @Override
                public String execute(final Jedis client, final ConnectionContext state) throws DynoException {
                    return client.setex(key, seconds, compressValue(value, state));
                }
            });
        }
    }
    
    @Override
    public String psetex(final String key, final long milliseconds, final String value) {
        return d_psetex(key, milliseconds, value).getResult();
    }

    public OperationResult<String> d_psetex(final String key, final long milliseconds, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.PSETEX) {
                @Override
                public String execute(Jedis client, ConnectionContext state) throws DynoException {
                    return client.psetex(key, milliseconds, value);
                }
            });
        } else {
            return connPool.executeWithFailover(new CompressionValueOperation<String>(key, OpName.PSETEX) {
                @Override
                public String execute(final Jedis client, final ConnectionContext state) throws DynoException {
                    return client.psetex(key, milliseconds, compressValue(value, state));
                }
            });
        }
    }
      



    @Override
    public Long setnx(final String key, final String value) {
        return d_setnx(key, value).getResult();
    }

    public OperationResult<Long> d_setnx(final String key, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SETNX) {
                @Override
                public Long execute(Jedis client, ConnectionContext state) throws DynoException {
                    return client.setnx(key, value);
                }
            });
        } else {
            return connPool.executeWithFailover(new CompressionValueOperation<Long>(key, OpName.SETNX) {
                @Override
                public Long execute(final Jedis client, final ConnectionContext state) {
                    return client.setnx(key, compressValue(value, state));
                }
            });
        }
    }

    @Override
    public Long setrange(final String key, final long offset, final String value) {
        return d_setrange(key, offset, value).getResult();
    }

    public OperationResult<Long> d_setrange(final String key, final Long offset, final String value) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SETRANGE) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.setrange(key, offset, value);
            }

        });
    }

    @Override
    public Boolean sismember(final String key, final String member) {
        return d_sismember(key, member).getResult();
    }

    public OperationResult<Boolean> d_sismember(final String key, final String member) {

        return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.SISMEMBER) {

            @Override
            public Boolean execute(Jedis client, ConnectionContext state) {
                return client.sismember(key, member);
            }

        });
    }

    @Override
    public Set<String> smembers(final String key) {
        return d_smembers(key).getResult();
    }

    public OperationResult<Set<String>> d_smembers(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.SMEMBERS) {

            @Override
            public Set<String> execute(Jedis client, ConnectionContext state) {
                return client.smembers(key);
            }

        });
    }

    public Long smove(final String srckey, final String dstkey, final String member) {
        return d_smove(srckey, dstkey, member).getResult();
    }

    public OperationResult<Long> d_smove(final String srckey, final String dstkey, final String member) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(srckey, OpName.SMOVE) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.smove(srckey, dstkey, member);
            }

        });
    }

    @Override
    public List<String> sort(String key) {
        return d_sort(key).getResult();
    }

    public OperationResult<List<String>> d_sort(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<List<String>>(key, OpName.SORT) {

            @Override
            public List<String> execute(Jedis client, ConnectionContext state) {
                return client.sort(key);
            }

        });
    }

    @Override
    public List<String> sort(String key, SortingParams sortingParameters) {
        return d_sort(key, sortingParameters).getResult();
    }

    public OperationResult<List<String>> d_sort(final String key, final SortingParams sortingParameters) {

        return connPool.executeWithFailover(new BaseKeyOperation<List<String>>(key, OpName.SORT) {

            @Override
            public List<String> execute(Jedis client, ConnectionContext state) {
                return client.sort(key, sortingParameters);
            }

        });
    }

    @Override
    public String spop(final String key) {
        return d_spop(key).getResult();
    }

    public OperationResult<String> d_spop(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.SPOP) {

            @Override
            public String execute(Jedis client, ConnectionContext state) {
                return client.spop(key);
            }

        });
    }

    @Override
    public Set<String> spop(String key, long count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String srandmember(final String key) {
        return d_srandmember(key).getResult();
    }

    @Override
    public List<String> srandmember(String key, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    public OperationResult<String> d_srandmember(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.SRANDMEMBER) {

            @Override
            public String execute(Jedis client, ConnectionContext state) {
                return client.srandmember(key);
            }

        });
    }

    @Override
    public Long srem(final String key, final String... members) {
        return d_srem(key, members).getResult();
    }

    public OperationResult<Long> d_srem(final String key, final String... members) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SREM) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.srem(key, members);
            }

        });
    }

    @Override
    public ScanResult<String> sscan(final String key, final int cursor) {
        return d_sscan(key, cursor).getResult();
    }

    public OperationResult<ScanResult<String>> d_sscan(final String key, final int cursor) {

        return connPool.executeWithFailover(new BaseKeyOperation<ScanResult<String>>(key, OpName.SSCAN) {

            @Override
            public ScanResult<String> execute(Jedis client, ConnectionContext state) {
                return client.sscan(key, cursor);
            }

        });
    }

    @Override
    public ScanResult<String> sscan(final String key, final String cursor) {
        return d_sscan(key, cursor).getResult();
    }

    public OperationResult<ScanResult<String>> d_sscan(final String key, final String cursor) {

        return connPool.executeWithFailover(new BaseKeyOperation<ScanResult<String>>(key, OpName.SSCAN) {

            @Override
            public ScanResult<String> execute(Jedis client, ConnectionContext state) {
                return client.sscan(key, cursor);
            }

        });
    }

    @Override
    public ScanResult<String> sscan(final String key, final String cursor, final ScanParams params) {
        return d_sscan(key, cursor, params).getResult();
    }

    public OperationResult<ScanResult<String>> d_sscan(final String key, final String cursor, final ScanParams params) {

        return connPool.executeWithFailover(new BaseKeyOperation<ScanResult<String>>(key, OpName.SSCAN) {

            @Override
            public ScanResult<String> execute(Jedis client, ConnectionContext state) {
                return client.sscan(key, cursor, params);
            }

        });
    }

    @Override
    public Long strlen(final String key) {
        return d_strlen(key).getResult();
    }

    public OperationResult<Long> d_strlen(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.STRLEN) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.strlen(key);
            }

        });
    }

    @Override
    public String substr(String key, int start, int end) {
        return d_substr(key, start, end).getResult();
    }

    public OperationResult<String> d_substr(final String key, final Integer start, final Integer end) {

        return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.SUBSTR) {

            @Override
            public String execute(Jedis client, ConnectionContext state) {
                return client.substr(key, start, end);
            }

        });
    }

    @Override
    public Long ttl(final String key) {
        return d_ttl(key).getResult();
    }

    public OperationResult<Long> d_ttl(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.TTL) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.ttl(key);
            }

        });
    }

    @Override
    public String type(final String key) {
        return d_type(key).getResult();
    }

    public OperationResult<String> d_type(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.TYPE) {

            @Override
            public String execute(Jedis client, ConnectionContext state) {
                return client.type(key);
            }

        });
    }

    @Override
    public Long zadd(String key, double score, String member) {
        return d_zadd(key, score, member).getResult();
    }

    public OperationResult<Long> d_zadd(final String key, final Double score, final String member) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZADD) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.zadd(key, score, member);
            }

        });
    }

    @Override
    public Long zadd(String key, Map<String, Double> scoreMembers) {
        return d_zadd(key, scoreMembers).getResult();
    }

    public OperationResult<Long> d_zadd(final String key, final Map<String, Double> scoreMembers) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZADD) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.zadd(key, scoreMembers);
            }

        });
    }

    @Override
    public Long zadd(String key, double score, String member, ZAddParams params) {
        return d_zadd(key, score, member, params).getResult();
    }

    public OperationResult<Long> d_zadd(final String key, final double score, final String member,
            final ZAddParams params) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZADD) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.zadd(key, score, member, params);
            }

        });
    }

    @Override
    public Long zcard(final String key) {
        return d_zcard(key).getResult();
    }

    public OperationResult<Long> d_zcard(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZCARD) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.zcard(key);
            }

        });
    }

    @Override
    public Long zcount(final String key, final double min, final double max) {
        return d_zcount(key, min, max).getResult();
    }

    public OperationResult<Long> d_zcount(final String key, final Double min, final Double max) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZCOUNT) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.zcount(key, min, max);
            }

        });
    }

    @Override
    public Long zcount(String key, String min, String max) {
        return d_zcount(key, min, max).getResult();
    }

    public OperationResult<Long> d_zcount(final String key, final String min, final String max) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZCOUNT) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.zcount(key, min, max);
            }

        });
    }

    @Override
    public Double zincrby(final String key, final double score, final String member) {
        return d_zincrby(key, score, member).getResult();
    }

    public OperationResult<Double> d_zincrby(final String key, final Double score, final String member) {

        return connPool.executeWithFailover(new BaseKeyOperation<Double>(key, OpName.ZINCRBY) {

            @Override
            public Double execute(Jedis client, ConnectionContext state) {
                return client.zincrby(key, score, member);
            }

        });
    }

    @Override
    public Set<String> zrange(String key, long start, long end) {
        return d_zrange(key, start, end).getResult();
    }

    public OperationResult<Set<String>> d_zrange(final String key, final Long start, final Long end) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZRANGE) {

            @Override
            public Set<String> execute(Jedis client, ConnectionContext state) {
                return client.zrange(key, start, end);
            }

        });
    }

    @Override
    public Long zrank(final String key, final String member) {
        return d_zrank(key, member).getResult();
    }

    public OperationResult<Long> d_zrank(final String key, final String member) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZRANK) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.zrank(key, member);
            }

        });
    }

    @Override
    public Long zrem(String key, String... member) {
        return d_zrem(key, member).getResult();
    }

    public OperationResult<Long> d_zrem(final String key, final String... member) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZREM) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.zrem(key, member);
            }

        });
    }

    @Override
    public Long zremrangeByRank(final String key, final long start, final long end) {
        return d_zremrangeByRank(key, start, end).getResult();
    }

    public OperationResult<Long> d_zremrangeByRank(final String key, final Long start, final Long end) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZREMRANGEBYRANK) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.zremrangeByRank(key, start, end);
            }

        });
    }

    @Override
    public Long zremrangeByScore(final String key, final double start, final double end) {
        return d_zremrangeByScore(key, start, end).getResult();
    }

    public OperationResult<Long> d_zremrangeByScore(final String key, final Double start, final Double end) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZREMRANGEBYSCORE) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.zremrangeByScore(key, start, end);
            }

        });
    }

    @Override
    public Set<String> zrevrange(String key, long start, long end) {
        return d_zrevrange(key, start, end).getResult();
    }

    public OperationResult<Set<String>> d_zrevrange(final String key, final Long start, final Long end) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZREVRANGE) {

            @Override
            public Set<String> execute(Jedis client, ConnectionContext state) {
                return client.zrevrange(key, start, end);
            }

        });
    }

    @Override
    public Long zrevrank(final String key, final String member) {
        return d_zrevrank(key, member).getResult();
    }

    public OperationResult<Long> d_zrevrank(final String key, final String member) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZREVRANK) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.zrevrank(key, member);
            }

        });
    }

    @Override
    public Set<Tuple> zrangeWithScores(String key, long start, long end) {
        return d_zrangeWithScores(key, start, end).getResult();
    }

    public OperationResult<Set<Tuple>> d_zrangeWithScores(final String key, final Long start, final Long end) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZRANGEWITHSCORES) {

            @Override
            public Set<Tuple> execute(Jedis client, ConnectionContext state) {
                return client.zrangeWithScores(key, start, end);
            }

        });
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        return d_zrevrangeWithScores(key, start, end).getResult();
    }

    public OperationResult<Set<Tuple>> d_zrevrangeWithScores(final String key, final Long start, final Long end) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZREVRANGEWITHSCORES) {

            @Override
            public Set<Tuple> execute(Jedis client, ConnectionContext state) {
                return client.zrevrangeWithScores(key, start, end);
            }

        });
    }

    @Override
    public Double zscore(final String key, final String member) {
        return d_zscore(key, member).getResult();
    }

    public OperationResult<Double> d_zscore(final String key, final String member) {

        return connPool.executeWithFailover(new BaseKeyOperation<Double>(key, OpName.ZSCORE) {

            @Override
            public Double execute(Jedis client, ConnectionContext state) {
                return client.zscore(key, member);
            }

        });
    }

    @Override
    public ScanResult<Tuple> zscan(final String key, final int cursor) {
        return d_zscan(key, cursor).getResult();
    }

    public OperationResult<ScanResult<Tuple>> d_zscan(final String key, final int cursor) {

        return connPool.executeWithFailover(new BaseKeyOperation<ScanResult<Tuple>>(key, OpName.ZSCAN) {
            @Override
            public ScanResult<Tuple> execute(Jedis client, ConnectionContext state) {
                return client.zscan(key, cursor);
            }

        });
    }

    @Override
    public ScanResult<Tuple> zscan(final String key, final String cursor) {
        return d_zscan(key, cursor).getResult();
    }

    public OperationResult<ScanResult<Tuple>> d_zscan(final String key, final String cursor) {

        return connPool.executeWithFailover(new BaseKeyOperation<ScanResult<Tuple>>(key, OpName.ZSCAN) {
            @Override
            public ScanResult<Tuple> execute(Jedis client, ConnectionContext state) {
                return client.zscan(key, cursor);
            }

        });
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max) {
        return d_zrangeByScore(key, min, max).getResult();
    }

    public OperationResult<Set<String>> d_zrangeByScore(final String key, final Double min, final Double max) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZRANGEBYSCORE) {

            @Override
            public Set<String> execute(Jedis client, ConnectionContext state) {
                return client.zrangeByScore(key, min, max);
            }

        });
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max) {
        return d_zrangeByScore(key, min, max).getResult();
    }

    public OperationResult<Set<String>> d_zrangeByScore(final String key, final String min, final String max) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZRANGEBYSCORE) {

            @Override
            public Set<String> execute(Jedis client, ConnectionContext state) {
                return client.zrangeByScore(key, min, max);
            }

        });
    }

    @Override
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        return d_zrangeByScore(key, min, max, offset, count).getResult();
    }

    public OperationResult<Set<String>> d_zrangeByScore(final String key, final Double min, final Double max,
            final Integer offset, final Integer count) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZRANGEBYSCORE) {

            @Override
            public Set<String> execute(Jedis client, ConnectionContext state) {
                return client.zrangeByScore(key, min, max, offset, count);
            }

        });
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min) {
        return d_zrevrangeByScore(key, max, min).getResult();
    }

    public OperationResult<Set<String>> d_zrevrangeByScore(final String key, final String max, final String min) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZREVRANGEBYSCORE) {

            @Override
            public Set<String> execute(Jedis client, ConnectionContext state) {
                return client.zrevrangeByScore(key, max, min);
            }

        });
    }

    @Override
    public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
        return d_zrangeByScore(key, min, max, offset, count).getResult();
    }

    public OperationResult<Set<String>> d_zrangeByScore(final String key, final String min, final String max,
            final Integer offset, final Integer count) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZRANGEBYSCORE) {

            @Override
            public Set<String> execute(Jedis client, ConnectionContext state) {
                return client.zrangeByScore(key, min, max, offset, count);
            }

        });
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        return d_zrevrangeByScore(key, max, min, offset, count).getResult();
    }

    public OperationResult<Set<String>> d_zrevrangeByScore(final String key, final Double max, final Double min,
            final Integer offset, final Integer count) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZREVRANGEBYSCORE) {

            @Override
            public Set<String> execute(Jedis client, ConnectionContext state) {
                return client.zrevrangeByScore(key, max, min, offset, count);
            }

        });
    }

    @Override
    public Set<String> zrevrangeByScore(String key, double max, double min) {
        return d_zrevrangeByScore(key, max, min).getResult();
    }

    public OperationResult<Set<String>> d_zrevrangeByScore(final String key, final Double max, final Double min) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZREVRANGEBYSCORE) {

            @Override
            public Set<String> execute(Jedis client, ConnectionContext state) {
                return client.zrevrangeByScore(key, max, min);
            }

        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
        return d_zrangeByScoreWithScores(key, min, max).getResult();
    }

    public OperationResult<Set<Tuple>> d_zrangeByScoreWithScores(final String key, final Double min, final Double max) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZREVRANGEBYSCORE) {

            @Override
            public Set<Tuple> execute(Jedis client, ConnectionContext state) {
                return client.zrangeByScoreWithScores(key, min, max);
            }

        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
        return d_zrevrangeByScoreWithScores(key, min, max).getResult();
    }

    public OperationResult<Set<Tuple>> d_zrevrangeByScoreWithScores(final String key, final Double max,
            final Double min) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZREVRANGEBYSCOREWITHSCORES) {

            @Override
            public Set<Tuple> execute(Jedis client, ConnectionContext state) {
                return client.zrevrangeByScoreWithScores(key, max, min);
            }

        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
        return d_zrangeByScoreWithScores(key, min, max, offset, count).getResult();
    }

    public OperationResult<Set<Tuple>> d_zrangeByScoreWithScores(final String key, final Double min, final Double max,
            final Integer offset, final Integer count) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZRANGEBYSCOREWITHSCORES) {

            @Override
            public Set<Tuple> execute(Jedis client, ConnectionContext state) {
                return client.zrangeByScoreWithScores(key, min, max, offset, count);
            }

        });
    }

    @Override
    public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        return d_zrevrangeByScore(key, max, min, offset, count).getResult();
    }

    public OperationResult<Set<String>> d_zrevrangeByScore(final String key, final String max, final String min,
            final Integer offset, final Integer count) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZREVRANGEBYSCORE) {

            @Override
            public Set<String> execute(Jedis client, ConnectionContext state) {
                return client.zrevrangeByScore(key, max, min, offset, count);
            }

        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
        return d_zrangeByScoreWithScores(key, min, max).getResult();
    }

    public OperationResult<Set<Tuple>> d_zrangeByScoreWithScores(final String key, final String min, final String max) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZRANGEBYSCOREWITHSCORES) {

            @Override
            public Set<Tuple> execute(Jedis client, ConnectionContext state) {
                return client.zrangeByScoreWithScores(key, min, max);
            }

        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
        return d_zrevrangeByScoreWithScores(key, max, min).getResult();
    }

    public OperationResult<Set<Tuple>> d_zrevrangeByScoreWithScores(final String key, final String max,
            final String min) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZREVRANGEBYSCOREWITHSCORES) {

            @Override
            public Set<Tuple> execute(Jedis client, ConnectionContext state) {
                return client.zrevrangeByScoreWithScores(key, max, min);
            }

        });
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        return d_zrangeByScoreWithScores(key, min, max, offset, count).getResult();
    }

    public OperationResult<Set<Tuple>> d_zrangeByScoreWithScores(final String key, final String min, final String max,
            final Integer offset, final Integer count) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZRANGEBYSCOREWITHSCORES) {

            @Override
            public Set<Tuple> execute(Jedis client, ConnectionContext state) {
                return client.zrangeByScoreWithScores(key, min, max, offset, count);
            }

        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
        return d_zrevrangeByScoreWithScores(key, max, min, offset, count).getResult();
    }

    public OperationResult<Set<Tuple>> d_zrevrangeByScoreWithScores(final String key, final Double max,
            final Double min, final Integer offset, final Integer count) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZREVRANGEBYSCOREWITHSCORES) {

            @Override
            public Set<Tuple> execute(Jedis client, ConnectionContext state) {
                return client.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }

        });
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        return d_zrevrangeByScoreWithScores(key, max, min, offset, count).getResult();
    }

    public OperationResult<Set<Tuple>> d_zrevrangeByScoreWithScores(final String key, final String max,
            final String min, final Integer offset, final Integer count) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZREVRANGEBYSCOREWITHSCORES) {

            @Override
            public Set<Tuple> execute(Jedis client, ConnectionContext state) {
                return client.zrevrangeByScoreWithScores(key, max, min, offset, count);
            }

        });
    }

    @Override
    public Long zremrangeByScore(String key, String start, String end) {
        return d_zremrangeByScore(key, start, end).getResult();
    }

    @Override
    public Long zlexcount(String key, String min, String max) {
        return d_zlexcount(key, min, max).getResult();
    }

    public OperationResult<Long> d_zlexcount(final String key, final String min, final String max) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZLEXCOUNT) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.zlexcount(key, min, max);
            }

        });
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max) {
        return d_zrangeByLex(key, min, max).getResult();
    }

    public OperationResult<Set<String>> d_zrangeByLex(final String key, final String min, final String max) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZRANGEBYLEX) {

            @Override
            public Set<String> execute(Jedis client, ConnectionContext state) {
                return client.zrangeByLex(key, min, max);
            }

        });
    }

    @Override
    public Set<String> zrangeByLex(String key, String min, String max, int offset, int count) {
        return d_zrangeByLex(key, min, max, offset, count).getResult();
    }

    public OperationResult<Set<String>> d_zrangeByLex(final String key, final String min, final String max,
                                                      final int offset, final int count) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZRANGEBYLEX) {

            @Override
            public Set<String> execute(Jedis client, ConnectionContext state) {
                return client.zrangeByLex(key, min, max, offset, count);
            }

        });
    }


    @Override
    public Long zremrangeByLex(String key, String min, String max) {
        return d_zremrangeByLex(key, min, max).getResult();
    }

    public OperationResult<Long> d_zremrangeByLex(final String key, final String min, final String max) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZREMRANGEBYLEX) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.zremrangeByLex(key, min, max);
            }

        });
    }


    public OperationResult<Long> d_zremrangeByScore(final String key, final String start, final String end) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZREMRANGEBYSCORE) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.zremrangeByScore(key, start, end);
            }

        });
    }

    @Override
    public List<String> blpop(String arg) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<String> blpop(int timeout, String key) {
        return d_blpop(timeout, key).getResult();
    }

    public OperationResult<List<String>> d_blpop(final int timeout, final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<List<String>>(key, OpName.BLPOP) {

            @Override
            public List<String> execute(Jedis client, ConnectionContext state) {
                return client.blpop(timeout, key);
            }

        });
    }

    @Override
    public List<String> brpop(String arg) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<String> brpop(int timeout, String key) {
        return d_brpop(timeout, key).getResult();
    }

    public OperationResult<List<String>> d_brpop(final int timeout, final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<List<String>>(key, OpName.BRPOP) {

            @Override
            public List<String> execute(Jedis client, ConnectionContext state) {
                return client.brpop(timeout, key);
            }

        });
    }

    @Override
    public String echo(String string) {
        return d_echo(string).getResult();
    }

    public OperationResult<String> d_echo(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.ECHO) {

            @Override
            public String execute(Jedis client, ConnectionContext state) {
                return client.echo(key);
            }

        });
    }

    @Override
    public Long move(String key, int dbIndex) {
        return d_move(key, dbIndex).getResult();
    }

    public OperationResult<Long> d_move(final String key, final Integer dbIndex) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.MOVE) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.move(key, dbIndex);
            }

        });
    }

    @Override
    public Long bitcount(String key) {
        return d_bitcount(key).getResult();
    }

    public OperationResult<Long> d_bitcount(final String key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.BITCOUNT) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.bitcount(key);
            }

        });
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
        return d_bitcount(key, start, end).getResult();
    }

    public OperationResult<Long> d_bitcount(final String key, final Long start, final Long end) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.BITCOUNT) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.bitcount(key, start, end);
            }

        });
    }

    /** MULTI-KEY COMMANDS */


    @Override
    public List<String> blpop(int timeout, String... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<String> brpop(int timeout, String... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<String> blpop(String... args) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<String> brpop(String... args) {
        throw new UnsupportedOperationException("not yet implemented");
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
     * @param pattern
     *            Specifies the mach set for keys
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
        throw new UnsupportedOperationException("not yet implemented");
    }

    /**
     * Get values for all the keys provided. Returns a list of string values
     * corresponding to individual keys. If one of the key is missing, the
     * return list has null as its corresponding value.
     *
     * @param keys:
     *            variable list of keys to query
     * @return list of string values
     * @see <a href="http://redis.io/commands/MGET">mget</a>
     */
    @Override
    public List<String> mget(String... keys) {
        return d_mget(keys).getResult();
    }

    public OperationResult<List<String>> d_mget(final String... keys) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {

            return connPool.executeWithFailover(new MultiKeyOperation<List<String>>(Arrays.asList(keys), OpName.MGET) {
                @Override
                public List<String> execute(Jedis client, ConnectionContext state) {
                    return client.mget(keys);
                }
            });
        } else {
            return connPool.executeWithFailover(
                    new CompressionValueMultiKeyOperation<List<String>>(Arrays.asList(keys), OpName.MGET) {
                        @Override
                        public List<String> execute(final Jedis client, final ConnectionContext state)
                                throws DynoException {
                            return new ArrayList<String>(CollectionUtils.transform(client.mget(keys),
                                    new CollectionUtils.Transform<String, String>() {
                                        @Override
                                        public String get(String s) {
                                            return decompressValue(state, s);
                                        }
                                    }));
                        }
                    });
        }
    }
    
    
    @Override
    public Long exists(String... arg0) {
        return d_exists(arg0).getResult();
    }
    
    public OperationResult<Long> d_exists(final String... arg0) {
        return connPool.executeWithFailover(new MultiKeyOperation<Long>(Arrays.asList(arg0), OpName.EXISTS) {
              @Override
              public Long execute(Jedis client, ConnectionContext state) {
                    return client.exists(arg0);
              }
        });
    }
    
    @Override
    public Long del(String... keys) {
        return d_del(keys).getResult();
    }

    public OperationResult<Long> d_del(final String... keys) {

        return connPool.executeWithFailover(new MultiKeyOperation<Long>(Arrays.asList(keys), OpName.DEL) {
            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.del(keys);
            }
        });
    }
    
    
    
    @Override
    public Long msetnx(String... keysvalues) {
        return d_msetnx(keysvalues).getResult();
    }

    public OperationResult<Long> d_msetnx(final String... keysvalues) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {

            return connPool.executeWithFailover(new MultiKeyOperation<Long>(Arrays.asList(keysvalues), OpName.MSETNX) {
                @Override
                public Long execute(Jedis client, ConnectionContext state) {                	 
                    return client.msetnx(keysvalues);
                }
            });
        } else {       	
            return connPool.executeWithFailover(new CompressionValueMultiKeyOperation<Long>(Arrays.asList(keysvalues), OpName.MSETNX) {
        		@Override
                public Long execute(final Jedis client, final ConnectionContext state) {
                    return client.msetnx(compressMultiKeyValue(state,keysvalues));
                }
            });
        }
    }
    
    @Override
    public String mset(String... keysvalues) {
        return d_mset(keysvalues).getResult();
    }

    public OperationResult<String> d_mset(final String... keysvalues) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {

            return connPool.executeWithFailover(new MultiKeyOperation<String>(Arrays.asList(keysvalues), OpName.MSET) {
                @Override
                public String execute(Jedis client, ConnectionContext state) {
                	
                    return client.mset(keysvalues);
                }
            });
        } else {
            return connPool.executeWithFailover(new CompressionValueMultiKeyOperation<String>(Arrays.asList(keysvalues), OpName.MSET) {
        		@Override
                public String execute(final Jedis client, final ConnectionContext state) {
                    return client.mset(compressMultiKeyValue(state,keysvalues));
                }
            });
        }
    }

    @Override
    public Set<String> sinter(String... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    public Long sinterstore(final String dstkey, final String... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long sort(String key, SortingParams sortingParameters, String dstkey) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long sort(String key, String dstkey) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<String> sunion(String... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long sunionstore(String dstkey, String... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String watch(String... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long del(byte[]... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long exists(byte[]... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<byte[]> blpop(int timeout, byte[]... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<byte[]> brpop(int timeout, byte[]... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<byte[]> blpop(byte[]... args) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<byte[]> brpop(byte[]... args) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> keys(byte[] pattern) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<byte[]> mget(byte[]... keys) {
        return d_mget(keys).getResult();
    }

    public OperationResult<List<byte[]>> d_mget(final byte[]... keys) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {

            return connPool.executeWithFailover(new MultiKeyOperation<List<byte[]>>(Arrays.asList(keys), OpName.MGET) {
                @Override
                public List<byte[]> execute(Jedis client, ConnectionContext state) {
                    return client.mget(keys);
                }
            });
        } else {
            return connPool.executeWithFailover(
                    new CompressionValueMultiKeyOperation<List<byte[]>>(Arrays.asList(keys), OpName.MGET) {
                        @Override
                        public List<byte[]> execute(final Jedis client, final ConnectionContext state)
                                throws DynoException {
                            return new ArrayList<>(CollectionUtils.transform(client.mget(keys),
                                    new CollectionUtils.Transform<byte[], byte[]>() {
                                        @Override
                                        public byte[] get(byte[] s) {
                                            return decompressValue(state, String.valueOf(s)).getBytes();
                                        }
                                    }));
                        }
                    });
        }
    }

    @Override
    public String mset(byte[]... keysvalues) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long msetnx(byte[]... keysvalues) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String rename(byte[] oldkey, byte[] newkey) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long renamenx(byte[] oldkey, byte[] newkey) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public byte[] rpoplpush(byte[] srckey, byte[] dstkey) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> sdiff(byte[]... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long sdiffstore(byte[] dstkey, byte[]... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> sinter(byte[]... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long sinterstore(byte[] dstkey, byte[]... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long smove(byte[] srckey, byte[] dstkey, byte[] member) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long sort(byte[] key, SortingParams sortingParameters, byte[] dstkey) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long sort(byte[] key, byte[] dstkey) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> sunion(byte[]... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long sunionstore(byte[] dstkey, byte[]... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String watch(byte[]... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String unwatch() {
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
    public Long zinterstore(String dstkey, String... sets) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zinterstore(String dstkey, ZParams params, String... sets) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min) {
        return d_zrevrangeByLex(key, max, min).getResult();
    }

    public OperationResult<Set<String>> d_zrevrangeByLex(final String key, final String max, final String min) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZREVRANGEBYLEX) {

            @Override
            public Set<String> execute(Jedis client, ConnectionContext state) {
                return client.zrangeByLex(key, max, min);
            }

        });
    }

    @Override
    public Set<String> zrevrangeByLex(String key, String max, String min, int offset, int count) {
        return d_zrevrangeByLex(key, max, min, offset, count).getResult();
    }

    public OperationResult<Set<String>> d_zrevrangeByLex(final String key, final String max, final String min,
                                                         final int offset, final int count) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZREVRANGEBYLEX) {

            @Override
            public Set<String> execute(Jedis client, ConnectionContext state) {
                return client.zrangeByLex(key, max, min, offset, count);
            }

        });
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

    /******************* Jedis Binary Commands **************/
    @Override
    public String set(final byte[] key, final byte[] value) {
        return d_set(key, value).getResult();
    }

    public OperationResult<String> d_set(final byte[] key, final byte[] value) {
        return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.SET) {
            @Override
            public String execute(Jedis client, ConnectionContext state) throws DynoException {
                return client.set(key, value);
            }
        });
    }

    @Override
    public byte[] get(final byte[] key) {
        return d_get(key).getResult();
    }

    public OperationResult<byte[]> d_get(final byte[] key) {

            return connPool.executeWithFailover(new BaseKeyOperation<byte[]>(key, OpName.GET) {
                @Override
                public byte[] execute(Jedis client, ConnectionContext state) throws DynoException {
                    return client.get(key);
                }
            });
    }

    @Override
    public String setex(final byte[] key, final int seconds, final byte[] value) {
        return d_setex(key, seconds, value).getResult();
    }

    public OperationResult<String> d_setex(final byte[] key, final Integer seconds, final byte[] value) {
        return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.SETEX) {
            @Override
            public String execute(Jedis client, ConnectionContext state) throws DynoException {
                return client.setex(key, seconds, value);
            }
        });
    }

    @Override
    public String set(final byte[] key, final byte[] value, final byte[] nxxx, final byte[] expx, final long time) {
        return d_set(key, value, nxxx, expx, time).getResult();
    }

    public OperationResult<String> d_set(final byte[] key, final byte[] value, final byte[] nxxx, final byte[] expx,
            final long time) {
            return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.SET) {
                @Override
                public String execute(Jedis client, ConnectionContext state) throws DynoException {
                    return client.set(key, value, nxxx, expx, time);
                }
            });
    }


    @Override
    public Boolean exists(final byte[] key) {
        return d_exists(key).getResult();
    }

    public OperationResult<Boolean> d_exists(final byte[] key) {
        return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.EXISTS) {

            @Override
            public Boolean execute(Jedis client, ConnectionContext state) {
                return client.exists(key);
            }
        });
    }

    @Override
    public Long persist(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String type(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long expire(final byte[] key, final int seconds) {
        return d_expire(key, seconds).getResult();
    }

    public OperationResult<Long> d_expire(final byte[] key, final int seconds) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.EXPIRE) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.expire(key, seconds);
            }
        });
    }


    @Override
    public Long pexpire(byte[] key, final long milliseconds) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long expireAt(final byte[] key, final long unixTime) {
        return d_expireAt(key, unixTime).getResult();
    }

    public OperationResult<Long> d_expireAt(final byte[] key, final long unixTime) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.EXPIREAT) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.expireAt(key, unixTime);
            }

        });
    }

    @Override
    public Long pexpireAt(byte[] key, long millisecondsTimestamp) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long ttl(final byte[] key) {
        return d_ttl(key).getResult();
    }

    public OperationResult<Long> d_ttl(final byte[] key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.TTL) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.ttl(key);
            }

        });
    }

    @Override
    public Boolean setbit(byte[] key, long offset, boolean value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Boolean setbit(byte[] key, long offset, byte[] value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Boolean getbit(byte[] key, long offset) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long setrange(byte[] key, long offset, byte[] value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public byte[] getrange(byte[] key, long startOffset, long endOffset) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public byte[] getSet(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long setnx(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long decrBy(byte[] key, long integer) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long decr(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long incrBy(byte[] key, long integer) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Double incrByFloat(byte[] key, double value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long incr(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long append(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public byte[] substr(byte[] key, int start, int end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long hset(final byte[] key, final byte[] field, final byte[] value) {
        return d_hset(key, field, value).getResult();
    }

    public OperationResult<Long> d_hset(final byte[] key, final byte[] field, final byte[] value) {
            return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HSET) {
                @Override
                public Long execute(Jedis client, ConnectionContext state) {
                    return client.hset(key, field, value);
                }
            });
    }

    @Override
    public byte[] hget(final byte[] key, final byte[] field) {
        return d_hget(key, field).getResult();
    }

    public OperationResult<byte[]> d_hget(final byte[] key, final byte[] field) {
            return connPool.executeWithFailover(new BaseKeyOperation<byte[]>(key, OpName.HGET) {
                @Override
                public byte[] execute(Jedis client, ConnectionContext state) throws DynoException {
                    return client.hget(key, field);
                }
            });       
    }

    @Override
    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String hmset(final byte[] key, final Map<byte[], byte[]> hash) {
        return d_hmset(key, hash).getResult();
    }

    public OperationResult<String> d_hmset(final byte[] key, final Map<byte[], byte[]> hash) {
            return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.HMSET) {
                @Override
                public String execute(Jedis client, ConnectionContext state) {
                    return client.hmset(key, hash);
                }
            });
    }

    @Override
    public List<byte[]> hmget(final byte[] key, final byte[]... fields) {
        return d_hmget(key, fields).getResult();
    }

    public OperationResult<List<byte[]>> d_hmget(final byte[] key, final byte[]... fields) {
            return connPool.executeWithFailover(new BaseKeyOperation<List<byte[]>>(key, OpName.HMGET) {
                @Override
                public List<byte[]> execute(Jedis client, ConnectionContext state) {
                    return client.hmget(key, fields);
                }
            });
    }

    @Override
    public Long hincrBy(byte[] key, byte[] field, long value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Double hincrByFloat(byte[] key, byte[] field, double value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Boolean hexists(byte[] key, byte[] field) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long hdel(final byte[] key, final byte[]... fields) {
        return d_hdel(key, fields).getResult();
    }

    public OperationResult<Long> d_hdel(final byte[] key, final byte[]... fields) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HDEL) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.hdel(key, fields);
            }

        });
    }

    @Override
    public Long hlen(final byte[] key) {
        return d_hlen(key).getResult();
    }

    public OperationResult<Long> d_hlen(final byte[] key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HLEN) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.hlen(key);
            }

        });
    }

    @Override
    public Set<byte[]> hkeys(final byte[] key) {
        return d_hkeys(key).getResult();
    }

    public OperationResult<Set<byte[]>> d_hkeys(final byte[] key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<byte[]>>(key, OpName.HKEYS) {

            @Override
            public Set<byte[]> execute(Jedis client, ConnectionContext state) {
                return client.hkeys(key);
            }

        });
    }

    @Override
    public Collection<byte[]> hvals(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Map<byte[], byte[]> hgetAll(final  byte[] key) {
        return d_hgetAll(key).getResult();
    }

    public OperationResult<Map< byte[],  byte[]>> d_hgetAll(final  byte[] key) {
            return connPool.executeWithFailover(new BaseKeyOperation<Map< byte[],  byte[]>>(key, OpName.HGETALL) {
                @Override
                public Map< byte[], byte[]> execute(Jedis client, ConnectionContext state) throws DynoException {
                    return client.hgetAll(key);
                }
            });
    }

    @Override
    public Long rpush(byte[] key, byte[]... args) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long lpush(byte[] key, byte[]... args) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long llen(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<byte[]> lrange(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String ltrim(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public byte[] lindex(byte[] key, long index) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String lset(byte[] key, long index, byte[] value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long lrem(byte[] key, long count, byte[] value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public byte[] lpop(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public byte[] rpop(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long sadd(final byte[] key, final byte[]... members) {
        return d_sadd(key, members).getResult();
    }

    public OperationResult<Long> d_sadd(final byte[] key, final byte[]... members) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SADD) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.sadd(key, members);
            }

        });
    }

    @Override
    public Set<byte[]> smembers(final byte[] key) {
        return d_smembers(key).getResult();
    }

    public OperationResult<Set<byte[]>> d_smembers(final byte[] key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Set<byte[]>>(key, OpName.SMEMBERS) {

            @Override
            public Set<byte[]> execute(Jedis client, ConnectionContext state) {
                return client.smembers(key);
            }
        });
    }

    @Override
    public Long srem(final byte[] key, final byte[]... members) {
        return d_srem(key, members).getResult();
    }

    public OperationResult<Long> d_srem(final byte[] key, final byte[]... members) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SREM) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.srem(key, members);
            }

        });
    }

    @Override
    public byte[] spop(final byte[] key) {
        return d_spop(key).getResult();
    }

    public OperationResult< byte[]> d_spop(final byte[] key) {

        return connPool.executeWithFailover(new BaseKeyOperation< byte[]>(key, OpName.SPOP) {

            @Override
            public  byte[] execute(Jedis client, ConnectionContext state) {
                return client.spop(key);
            }
        });
    }

    @Override
    public Set<byte[]> spop(byte[] key, long count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long scard(final byte[] key) {
        return d_scard(key).getResult();
    }

    public OperationResult<Long> d_scard(final byte[] key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SCARD) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.scard(key);
            }

        });
    }
    @Override
    public Boolean sismember(byte[] key, byte[] member) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public byte[] srandmember(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<byte[]> srandmember(final byte[] key, final int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long strlen(final byte[] key) {
        return d_strlen(key).getResult();
    }

    public OperationResult<Long> d_strlen(final  byte[] key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.STRLEN) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.strlen(key);
            }

        });
    }

    @Override
    public Long zadd(byte[] key, double score, byte[] member) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> zrange(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zrem(byte[] key, byte[]... member) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Double zincrby(byte[] key, double score, byte[] member) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zrank(byte[] key, byte[] member) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zrevrank(byte[] key, byte[] member) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> zrevrange(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<Tuple> zrangeWithScores(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zcard(final byte[] key) {
        return d_zcard(key).getResult();
    }

    public OperationResult<Long> d_zcard(final byte[] key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZCARD) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.zcard(key);
            }

        });
    }

    @Override
    public Double zscore(final byte[] key, final byte[] member) {
        return d_zscore(key, member).getResult();
    }

    public OperationResult<Double> d_zscore(final byte[] key, final byte[] member) {

        return connPool.executeWithFailover(new BaseKeyOperation<Double>(key, OpName.ZSCORE) {

            @Override
            public Double execute(Jedis client, ConnectionContext state) {
                return client.zscore(key, member);
            }

        });
    }
    
    @Override
    public List<byte[]> sort(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zcount(byte[] key, double min, double max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zcount(byte[] key, byte[] min, byte[] max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zremrangeByRank(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zremrangeByScore(byte[] key, double start, double end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zremrangeByScore(byte[] key, byte[] start, byte[] end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zlexcount(final byte[] key, final byte[] min, final byte[] max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> zrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> zrangeByLex(final byte[] key, final byte[] min, final byte[] max, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> zrevrangeByLex(final byte[] key, final byte[] max, final byte[] min) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Set<byte[]> zrevrangeByLex(final byte[] key, final byte[] max, final byte[] min, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zremrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long linsert(byte[] key, BinaryClient.LIST_POSITION where, byte[] pivot, byte[] value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long lpushx(byte[] key, byte[]... arg) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long rpushx(byte[] key, byte[]... arg) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<byte[]> blpop(byte[] arg) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<byte[]> brpop(byte[] arg) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long del(final byte[] key) {
        return d_del(key).getResult();
    }

    public OperationResult<Long> d_del(final byte[] key) {

        return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.DEL) {

            @Override
            public Long execute(Jedis client, ConnectionContext state) {
                return client.del(key);
            }

        });
    }

    @Override
    public byte[] echo(byte[] arg) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long move(byte[] key, int dbIndex) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long bitcount(final byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long bitcount(final byte[] key, long start, long end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long pfadd(final byte[] key, final byte[]... elements) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public long pfcount(final byte[] key) {
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
    public ScanResult<String> scan(int cursor) {
        throw new UnsupportedOperationException("Not supported - use dyno_scan(String, CursorBasedResult");
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
        return new CursorBasedResultImpl<>(results, ((TokenRackMapper)cursor).getTokenRackMap());
    }

    @Override
    public String pfmerge(String destkey, String... sourcekeys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public long pfcount(String... keys) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    private boolean validHashtag(final String hashtag) {
        return !Strings.isNullOrEmpty(hashtag) && hashtag.length() == 2;
    }

    @VisibleForTesting
    String ehashDataKey(String key) throws UnsupportedOperationException {
        String hashtag = connPool.getConfiguration().getHashtag();
        if (!validHashtag(hashtag)) {
            throw new UnsupportedOperationException("hashtags not set");
        }

        return new StringBuilder(hashtag)
                .insert(1, key)
                .toString();
    }

    @VisibleForTesting
    String ehashMetadataKey(String key) throws UnsupportedOperationException {
        final String hashtag = connPool.getConfiguration().getHashtag();
        if (!validHashtag(hashtag)) {
            throw new UnsupportedOperationException("hashtags not set");
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

        // If metadata operation failed, remove the data and throw exception
        if (zResponse.get() != 1) {
            d_hdel(ehashDataKey(key), field);
            throw new DynoException("Failed to set field:" + field + "; Metadata operation failed:" + zResponse.get());
        }
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
        if (zResponse.get() != hResponse.get()) {
            d_hdel(ehashDataKey, field);
            d_zrem(ehashMetadataKey, field);
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
                Arrays.asList(fields).stream().anyMatch(x -> ehMetadataUpdateResult.expiredFields.contains(x))) {
            throw new DynoException("Failed to expire hash fields");
        }

        return mgetResponse.get();
    }

    @Override
    public String ehmset(final String key, final Map<String, Pair<String, Long>> hash) {
        final DynoJedisPipeline pipeline = this.pipelined();
        Map<String, String> fields = new HashMap<>();
        Map<String, Double> metadataFields = new HashMap<>();

        hash.keySet().stream().forEach(f -> {
            fields.put(f, hash.get(f).getLeft());
            metadataFields.put(f, timeInEpochSeconds(hash.get(f).getRight()));
        });

        final Response<Long> zResponse = pipeline.zadd(ehashMetadataKey(key), metadataFields,
                ZAddParams.zAddParams().ch());
        final Response<String> hResponse = pipeline.hmset(ehashDataKey(key), fields);
        pipeline.sync();

        // If metadata operation failed, remove the data and throw exception
        if (zResponse.get() != hash.size()) {
            d_hdel(ehashDataKey(key), fields.keySet().toArray(new String[0]));
            throw new DynoException("Failed to set fields; Metadata operation failed:" + zResponse.get());
        }
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

        public DynoJedisClient build() {
            assert (appName != null);
            assert (clusterName != null);

            if (cpConfig == null) {
                cpConfig = new ArchaiusConnectionPoolConfiguration(appName);
                Logger.info("Dyno Client runtime properties: " + cpConfig.toString());
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
            setLoadBalancingStrategy(shadowConfig);
            setHashtagConnectionPool(shadowSupplier, shadowConfig);

            String shadowAppName = shadowConfig.getName();
            DynoCPMonitor shadowCPMonitor = new DynoCPMonitor(shadowAppName);
            DynoOPMonitor shadowOPMonitor = new DynoOPMonitor(shadowAppName);

            JedisConnectionFactory connFactory = new JedisConnectionFactory(shadowOPMonitor, sslSocketFactory);

            final ConnectionPoolImpl<Jedis> shadowPool = startConnectionPool(shadowAppName, connFactory, shadowConfig,
                    shadowCPMonitor);

            // Construct a connection pool with the shadow cluster settings
            DynoJedisClient shadowClient = new DynoJedisClient(shadowAppName, dualWriteClusterName, shadowPool,
                    shadowOPMonitor, shadowCPMonitor);

            // Construct an instance of our DualWriter client
            DynoOPMonitor opMonitor = new DynoOPMonitor(appName);
            ConnectionPoolMonitor cpMonitor = (this.cpMonitor == null) ? new DynoCPMonitor(appName) : this.cpMonitor;

            final ConnectionPoolImpl<Jedis> pool = createConnectionPool(appName, opMonitor, cpMonitor);

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

            final ConnectionPoolImpl<Jedis> pool = createConnectionPool(appName, opMonitor, cpMonitor);

            return new DynoJedisClient(appName, clusterName, pool, opMonitor, cpMonitor);
        }

        private ConnectionPoolImpl<Jedis> createConnectionPool(String appName, DynoOPMonitor opMonitor,
                ConnectionPoolMonitor cpMonitor) {

            if (hostSupplier == null) {
                if (discoveryClient == null) {
                    throw new DynoConnectException("HostSupplier not provided. Cannot initialize EurekaHostsSupplier "
                            + "which requires a DiscoveryClient");
                } else {
                    hostSupplier = new EurekaHostsSupplier(clusterName, discoveryClient);
                }
            }

            cpConfig.withHostSupplier(hostSupplier);
            if (tokenMapSupplier != null)
                cpConfig.withTokenSupplier(tokenMapSupplier);
            setLoadBalancingStrategy(cpConfig);
            setHashtagConnectionPool(hostSupplier, cpConfig);
            JedisConnectionFactory connFactory = new JedisConnectionFactory(opMonitor, sslSocketFactory);

            return startConnectionPool(appName, connFactory, cpConfig, cpMonitor);
        }

        private ConnectionPoolImpl<Jedis> startConnectionPool(String appName, JedisConnectionFactory connFactory,
                ConnectionPoolConfigurationImpl cpConfig, ConnectionPoolMonitor cpMonitor) {

            final ConnectionPoolImpl<Jedis> pool = new ConnectionPoolImpl<Jedis>(connFactory, cpConfig, cpMonitor);

            try {
                Logger.info("Starting connection pool for app " + appName);

                pool.start().get();

                Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                    @Override
                    public void run() {
                        pool.shutdown();
                    }
                }));
            } catch (NoAvailableHostsException e) {
                if (cpConfig.getFailOnStartupIfNoHosts()) {
                    throw new RuntimeException(e);
                }

                Logger.warn("UNABLE TO START CONNECTION POOL -- IDLING");

                pool.idle();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            return pool;
        }

        private void setLoadBalancingStrategy(ConnectionPoolConfigurationImpl config) {
            if (ConnectionPoolConfiguration.LoadBalancingStrategy.TokenAware == config.getLoadBalancingStrategy()) {
                if (config.getTokenSupplier() == null) {
                    Logger.warn(
                            "TOKEN AWARE selected and no token supplier found, using default HttpEndpointBasedTokenMapSupplier()");
                    config.withTokenSupplier(new HttpEndpointBasedTokenMapSupplier());
                }

                if (config.getLocalRack() == null && config.localZoneAffinity()) {
                    String warningMessage = "DynoJedisClient for app=[" + config.getName()
                            + "] is configured for local rack affinity "
                            + "but cannot determine the local rack! DISABLING rack affinity for this instance. "
                            + "To make the client aware of the local rack either use "
                            + "ConnectionPoolConfigurationImpl.setLocalRack() when constructing the client "
                            + "instance or ensure EC2_AVAILABILTY_ZONE is set as an environment variable, e.g. "
                            + "run with -DLOCAL_RACK=us-east-1c";
                    config.setLocalZoneAffinity(false);
                    Logger.warn(warningMessage);
                }
            }
        }

        /**
         * Set the hash to the connection pool if is provided by Dynomite
         * @param hostSupplier
         * @param config
         */
        private void setHashtagConnectionPool(HostSupplier hostSupplier, ConnectionPoolConfigurationImpl config) {
            // Find the hosts from host supplier
            List<Host> hosts = (List<Host>) hostSupplier.getHosts();
            Collections.sort(hosts);
            // Convert the arraylist to set

            // Take the token map supplier (aka the token topology from
            // Dynomite)
            TokenMapSupplier tokenMapSupplier = config.getTokenSupplier();

            // Create a list of host/Tokens
            List<HostToken> hostTokens;
            if (tokenMapSupplier != null) {
                Set<Host> hostSet = new HashSet<Host>(hosts);
                hostTokens = tokenMapSupplier.getTokens(hostSet);
                /* Dyno cannot reach the TokenMapSupplier endpoint, 
                 * therefore no nodes can be retrieved.
                 */
                if (hostTokens.isEmpty()) {
                    throw new DynoConnectException("No hosts in the TokenMapSupplier");
                }
            } else {
                throw new DynoConnectException("TokenMapSupplier not provided");
            }
            
            String hashtag = hostTokens.get(0).getHost().getHashtag();
            short numHosts = 0;
            // Update inner state with the host tokens.
            for (HostToken hToken : hostTokens) {
                /**
                 * Checking hashtag consistency from all Dynomite hosts. If
                 * hashtags are not consistent, we need to throw an exception.
                 */
                String hashtagNew = hToken.getHost().getHashtag();

                if (hashtag != null && !hashtag.equals(hashtagNew)) {
                    Logger.error("Hashtag mismatch across hosts");
                    throw new RuntimeException("Hashtags are different across hosts");
                } // addressing case hashtag = null, hashtag = {} ...
                else if (numHosts > 0 && hashtag == null && hashtagNew != null) {
                    Logger.error("Hashtag mismatch across hosts");
                    throw new RuntimeException("Hashtags are different across hosts");

                }
                hashtag = hashtagNew;
                numHosts++;
            }

            if (hashtag != null) {
                config.withHashtag(hashtag);
            }
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

    @Override
    public ScanResult<String> scan(String arg0, ScanParams arg1) {
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
    public List<GeoRadiusResponse> georadius(byte[] arg0, double arg1, double arg2, double arg3, GeoUnit arg4,
            GeoRadiusParam arg5) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(byte[] arg0, byte[] arg1, double arg2, GeoUnit arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(byte[] arg0, byte[] arg1, double arg2, GeoUnit arg3,
            GeoRadiusParam arg4) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public ScanResult<Entry<byte[], byte[]>> hscan(byte[] arg0, byte[] arg1) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public ScanResult<Entry<byte[], byte[]>> hscan(byte[] arg0, byte[] arg1, ScanParams arg2) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String set(byte[] arg0, byte[] arg1, byte[] arg2) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public ScanResult<byte[]> sscan(byte[] arg0, byte[] arg1) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public ScanResult<byte[]> sscan(byte[] arg0, byte[] arg1, ScanParams arg2) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zadd(byte[] arg0, Map<byte[], Double> arg1, ZAddParams arg2) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Long zadd(byte[] arg0, double arg1, byte[] arg2, ZAddParams arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }
    
    @Override
    public Double zincrby(byte[] arg0, double arg1, byte[] arg2, ZIncrByParams arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public ScanResult<Tuple> zscan(byte[] arg0, byte[] arg1) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public ScanResult<Tuple> zscan(byte[] arg0, byte[] arg1, ScanParams arg2) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<byte[]> bitfield(byte[] key, byte[]... arguments) {
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
    public List<GeoRadiusResponse> georadius(String arg0, double arg1, double arg2, double arg3, GeoUnit arg4,
            GeoRadiusParam arg5) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String arg0, String arg1, double arg2, GeoUnit arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String arg0, String arg1, double arg2, GeoUnit arg3,
            GeoRadiusParam arg4) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public List<Long> bitfield(String key, String... arguments) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public ScanResult<Entry<String, String>> hscan(String arg0, String arg1, ScanParams arg2) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public String set(String arg0, String arg1, String arg2) {
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

}
