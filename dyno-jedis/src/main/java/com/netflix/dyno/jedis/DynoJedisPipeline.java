/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.jedis;

import com.google.common.base.Strings;
import com.netflix.dyno.connectionpool.*;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.FatalConnectionException;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl;
import com.netflix.dyno.connectionpool.impl.lb.TokenAwareSelection;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.ZipUtils;
import com.netflix.dyno.jedis.JedisConnectionFactory.JedisConnection;
import com.netflix.dyno.jedis.operation.BaseKeyOperation;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.*;
import redis.clients.jedis.commands.BinaryRedisPipeline;
import redis.clients.jedis.commands.RedisPipeline;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.params.ZIncrByParams;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.netflix.dyno.connectionpool.ConnectionPoolConfiguration.CompressionStrategy;

@NotThreadSafe
public class DynoJedisPipeline implements RedisPipeline, BinaryRedisPipeline, AutoCloseable {

    private static final Logger Logger = LoggerFactory.getLogger(DynoJedisPipeline.class);

    // ConnPool and connection to exec the pipeline
    private final ConnectionPoolImpl<Jedis> connPool;
    private volatile Connection<Jedis> connection;
    private final DynoJedisPipelineMonitor opMonitor;
    private final ConnectionPoolMonitor cpMonitor;

    // the cached pipeline
    private volatile Pipeline jedisPipeline = null;
    // the cached row key for the pipeline. all subsequent requests to pipeline
    // must be the same. this is used to check that.
    private final AtomicReference<String> theKey = new AtomicReference<String>(null);
    private final AtomicReference<String> hashtag = new AtomicReference<String>(null);
    private final AtomicReference<byte[]> theBinaryKey = new AtomicReference<byte[]>(null);
    private final AtomicReference<byte[]> hashtagBinary = new AtomicReference<byte[]>(null);
    // used for tracking errors
    private final AtomicReference<DynoException> pipelineEx = new AtomicReference<DynoException>(null);

    private static final String DynoPipeline = "DynoPipeline";

    DynoJedisPipeline(ConnectionPoolImpl<Jedis> cPool, DynoJedisPipelineMonitor operationMonitor,
                      ConnectionPoolMonitor connPoolMonitor) {
        this.connPool = cPool;
        this.opMonitor = operationMonitor;
        this.cpMonitor = connPoolMonitor;
    }

    private void pipelined(final byte[] key) {
        try {
            try {
                connection = connPool.getConnectionForOperation(new BaseOperation<Jedis, String>() {

                    @Override
                    public String getName() {
                        return DynoPipeline;
                    }

                    @Override
                    public String getStringKey() {// we do not use it in this context
                        return null;
                    }

                    @Override
                    public byte[] getBinaryKey() {
                        return key;
                    }

                });
            } catch (NoAvailableHostsException nahe) {
                cpMonitor.incOperationFailure(connection != null ? connection.getHost() : null, nahe);
                discardPipelineAndReleaseConnection();
                throw nahe;
            }
        } catch (NoAvailableHostsException nahe) {
            cpMonitor.incOperationFailure(connection != null ? connection.getHost() : null, nahe);
            discardPipelineAndReleaseConnection();
            throw nahe;
        }
        Jedis jedis = ((JedisConnection) connection).getClient();
        jedisPipeline = jedis.pipelined();
        cpMonitor.incOperationSuccess(connection.getHost(), 0);
    }

    private void pipelined(final String key) {
        try {
            try {
                connection = connPool.getConnectionForOperation(new BaseOperation<Jedis, String>() {

                    @Override
                    public String getName() {
                        return DynoPipeline;
                    }

                    @Override
                    public String getStringKey() {
                        return key;
                    }

                    @Override
                    public byte[] getBinaryKey() { // we do not use it in this context
                        return null;
                    }

                });
            } catch (NoAvailableHostsException nahe) {
                cpMonitor.incOperationFailure(connection != null ? connection.getHost() : null, nahe);
                discardPipelineAndReleaseConnection();
                throw nahe;
            }
        } catch (NoAvailableHostsException nahe) {
            cpMonitor.incOperationFailure(connection != null ? connection.getHost() : null, nahe);
            discardPipelineAndReleaseConnection();
            throw nahe;
        }
        Jedis jedis = ((JedisConnection) connection).getClient();
        jedisPipeline = jedis.pipelined();
        cpMonitor.incOperationSuccess(connection.getHost(), 0);
    }

    private void checkHashtag(final String key, final String hashtagValue) {
        if (this.hashtag.get() != null) {
            verifyHashtagValue(hashtagValue);
        } else {
            boolean success = this.hashtag.compareAndSet(null, hashtagValue);
            if (!success) {
                verifyHashtagValue(hashtagValue);
            } else {
                pipelined(key);
            }
        }

    }

    private void checkHashtag(final byte[] key, final byte[] hashtagValue) {
        if (this.hashtagBinary.get() != null) {
            verifyHashtagValue(hashtagValue);
        } else {
            boolean success = this.hashtagBinary.compareAndSet(null, hashtagValue);
            if (!success) {
                verifyHashtagValue(hashtagValue);
            } else {
                pipelined(key);
            }
        }

    }

    /**
     * Checks that a pipeline is associated with a single key.
     *
     * @param key
     */
    private void checkKey(final byte[] key) {
        String hashtag = connPool.getConfiguration().getHashtag();
        if (hashtag == null || hashtag.isEmpty()) {
            if (theBinaryKey.get() != null) {
                verifyKey(key);
            } else {
                boolean success = theBinaryKey.compareAndSet(null, key);
                if (!success) {
                    // someone already beat us to it. that's fine, just verify
                    // that the key is the same
                    verifyKey(key);
                } else {
                    pipelined(key);
                }
            }
        } else {
            /*
             * We have a identified a hashtag in the Host object. That means Dynomite has a
             * defined hashtag. Producing the hashvalue out of the hashtag and using that as
             * a reference to the pipeline
             */
            byte[] hashValue = TokenAwareSelection.getHashValue(key, hashtag);
            if (hashValue == null || hashValue.length == 0) {
                hashValue = key;
            }
            checkHashtag(key, hashValue);
        }
    }

    /**
     * Checks that a pipeline is associated with a single key. If there is a hashtag
     * defined in the first host of the connectionpool then we check that first.
     *
     * @param key
     */
    private void checkKey(final String key) {

        /*
         * Get hashtag from the first host of the active pool We cannot use the
         * connection object because as of now we have not selected a connection. A
         * connection is selected based on the key or hashtag respectively.
         */
        String hashtag = connPool.getConfiguration().getHashtag();
        if (hashtag == null || hashtag.isEmpty()) {
            if (theKey.get() != null) {
                verifyKey(key);
            } else {
                boolean success = theKey.compareAndSet(null, key);
                if (!success) {
                    // someone already beat us to it. that's fine, just verify
                    // that the key is the same
                    verifyKey(key);
                } else {
                    pipelined(key);
                }
            }
        } else {
            /*
             * We have a identified a hashtag in the Host object. That means Dynomite has a
             * defined hashtag. Producing the hashvalue out of the hashtag and using that as
             * a reference to the pipeline
             */
            String hashValue = TokenAwareSelection.getHashValue(key, hashtag);
            if (Strings.isNullOrEmpty(hashValue)) {
                hashValue = key;
            }
            checkHashtag(key, hashValue);
        }
    }

    /**
     * Verifies binary key with pipeline binary key
     */
    private void verifyKey(final byte[] key) {
        if (!Arrays.equals(theBinaryKey.get(), key)) {
            try {
                throw new RuntimeException("Must have same key for Redis Pipeline in Dynomite. This key: "
                                           + Arrays.toString(key));
            } finally {
                discardPipelineAndReleaseConnection();
            }
        }
    }

    /**
     * Verifies key with pipeline key
     */
    private void verifyKey(final String key) {

        if (!theKey.get().equals(key)) {
            try {
                throw new RuntimeException("Must have same key for Redis Pipeline in Dynomite. This key: " + key);
            } finally {
                discardPipelineAndReleaseConnection();
            }
        }
    }

    private void verifyHashtagValue(final String hashtagValue) {

        if (!this.hashtag.get().equals(hashtagValue)) {
            try {
                throw new RuntimeException(
                  "Must have same hashtag for Redis Pipeline in Dynomite. This hashvalue: " + hashtagValue);
            } finally {
                discardPipelineAndReleaseConnection();
            }
        }
    }

    private void verifyHashtagValue(final byte[] hashtagValue) {

        if (!Arrays.equals(this.hashtagBinary.get(), hashtagValue)) {
            try {
                throw new RuntimeException(
                  "Must have same hashtag for Redis Pipeline in Dynomite. This hashvalue: "
                  + Arrays.toString(hashtagValue));
            } finally {
                discardPipelineAndReleaseConnection();
            }
        }
    }

    private String decompressValue(String value) {
        try {
            if (ZipUtils.isCompressed(value)) {
                return ZipUtils.decompressFromBase64String(value);
            }
        } catch (IOException e) {
            Logger.warn("Unable to decompress value [" + value + "]");
        }

        return value;
    }

    private byte[] decompressValue(byte[] value) {
        try {
            if (ZipUtils.isCompressed(value)) {
                return ZipUtils.decompressBytesNonBase64(value);
            }
        } catch (IOException e) {
            Logger.warn("Unable to decompress byte array value [" + value + "]");
        }
        return value;
    }

    private String compressValue(String value) {
        String result = value;
        int thresholdBytes = connPool.getConfiguration().getValueCompressionThreshold();

        try {
            // prefer speed over accuracy here so rather than using
            // getBytes() to get the actual size
            // just estimate using 2 bytes per character
            if ((2 * value.length()) > thresholdBytes) {
                result = ZipUtils.compressStringToBase64String(value);
            }
        } catch (IOException e) {
            Logger.warn("UNABLE to compress [" + value + "]; sending value uncompressed");
        }

        return result;
    }

    private byte[] compressValue(byte[] value) {
        int thresholdBytes = connPool.getConfiguration().getValueCompressionThreshold();

        if (value.length > thresholdBytes) {
            try {
                return ZipUtils.compressBytesNonBase64(value);
            } catch (IOException e) {
                Logger.warn("UNABLE to compress byte array [" + value + "]; sending value uncompressed");
            }
        }

        return value;
    }

    public class DecompressPipelineResponse extends Response<String> {

        private Response<String> response;

        public DecompressPipelineResponse(Response<String> response) {
            super(BuilderFactory.STRING);
            this.response = response;
        }

        @Override
        public String get() {
            return decompressValue(response.get());
        }

    }

    public class DecompressPipelineListResponse extends Response<List<String>> {

        private Response<List<String>> response;

        public DecompressPipelineListResponse(Response<List<String>> response) {
            super(BuilderFactory.STRING_LIST);
            this.response = response;
        }

        @Override
        public List<String> get() {
            return response.get().stream().map(val -> decompressValue(val)).collect(Collectors.toList());
        }
    }

    public class DecompressPipelineBinaryResponse extends Response<byte[]> {

        private Response<byte[]> response;

        public DecompressPipelineBinaryResponse(Response<byte[]> response) {
            super(BuilderFactory.BYTE_ARRAY);
            this.response = response;
        }

        @Override
        public byte[] get() {
            return decompressValue(response.get());
        }

    }

    public class DecompressPipelineBinaryListResponse extends Response<List<byte[]>> {

        private Response<List<byte[]>> response;

        public DecompressPipelineBinaryListResponse(Response<List<byte[]>> response) {
            super(BuilderFactory.BYTE_ARRAY_LIST);
            this.response = response;
        }

        @Override
        public List<byte[]> get() {
            return response.get().stream().map(val -> decompressValue(val)).collect(Collectors.toList());
        }

    }

    protected <T> Response<T> execOp(String key, OpName opName, Supplier<Response<T>> fn) {
        checkKey(key);
        try {
            opMonitor.recordOperation(opName.name());
            return fn.get();
        } catch (JedisConnectionException ex) {
            DynoException e = new FatalConnectionException(ex).setAttempt(1);
            pipelineEx.set(e);
            cpMonitor.incOperationFailure(connection.getHost(), e);
            throw ex;
        }
    }

    protected <T> Response<T> execOp(byte[] key, OpName opName, Supplier<Response<T>> fn) {
        checkKey(key);
        try {
            opMonitor.recordOperation(opName.name());
            return fn.get();
        } catch (JedisConnectionException ex) {
            DynoException e = new FatalConnectionException(ex).setAttempt(1);
            pipelineEx.set(e);
            cpMonitor.incOperationFailure(connection.getHost(), e);
            throw ex;
        }
    }

    protected String compress(String val) {
        return compressValue(val);
    }

    protected Map<String, String> compressMap(Map<String, String> val) {
        Map<String, String> result = new HashMap<>();
        val.entrySet().stream().forEach(entry -> result.put(entry.getKey(), compressValue(entry.getValue())));
        return result;
    }

    protected Response<String> decompress(Response<String> val) {
        return new DecompressPipelineResponse(val);
    }

    protected Response<List<String>> decompressList(Response<List<String>> val) {
        return new DecompressPipelineListResponse(val);
    }

    protected byte[] compressBin(byte[] val) {
        return compressValue(val);
    }

    protected Map<byte[], byte[]> compressBinMap(Map<byte[], byte[]> val) {
        Map<byte[], byte[]> result = new HashMap<>();
        val.entrySet().stream().forEach(entry -> result.put(entry.getKey(), compressValue(entry.getValue())));
        return result;
    }

    protected Response<byte[]> decompressBin(Response<byte[]> val) {
        return new DecompressPipelineBinaryResponse(val);
    }

    protected Response<List<byte[]>> decompressBinList(Response<List<byte[]>> val) {
        return new DecompressPipelineBinaryListResponse(val);
    }

    /******************* Jedis String Commands **************/


    @Override
    public Response<Long> append(final String key, final String value) {
        return execOp(key, OpName.APPEND, () -> jedisPipeline.append(key, value));
    }

    @Override
    public Response<List<String>> blpop(final String arg) {
        return execOp(arg, OpName.BLPOP, () -> jedisPipeline.blpop(arg));
    }

    @Override
    public Response<List<String>> brpop(final String arg) {
        return execOp(arg, OpName.BRPOP, () -> jedisPipeline.brpop(arg));
    }

    @Override
    public Response<Long> decr(final String key) {
        return execOp(key, OpName.DECR, () -> jedisPipeline.decr(key));
    }

    @Override
    public Response<Long> decrBy(final String key, final long integer) {
        return execOp(key, OpName.DECRBY, () -> jedisPipeline.decrBy(key, integer));
    }

    @Override
    public Response<Long> del(final String key) {
        return execOp(key, OpName.DEL, () -> jedisPipeline.del(key));

    }

    @Override
    public Response<Long> unlink(String key) {
        return execOp(key, OpName.UNLINK, () -> jedisPipeline.unlink(key));
    }

    @Override
    public Response<String> echo(final String string) {
        return execOp(string, OpName.ECHO, () -> jedisPipeline.echo(string));

    }

    @Override
    public Response<Boolean> exists(final String key) {
        return execOp(key, OpName.EXISTS, () -> jedisPipeline.exists(key));
    }

    @Override
    public Response<Long> expire(final String key, final int seconds) {
        return execOp(key, OpName.EXPIRE, () -> jedisPipeline.expire(key, seconds));

    }

    @Override
    public Response<Long> pexpire(String key, long milliseconds) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> expireAt(final String key, final long unixTime) {
        return execOp(key, OpName.EXPIREAT, () -> jedisPipeline.expireAt(key, unixTime));

    }

    @Override
    public Response<Long> pexpireAt(String key, long millisecondsTimestamp) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<String> get(final String key) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.GET, () -> jedisPipeline.get(key));
        } else {
            return execOp(key, OpName.GET, () -> decompress(jedisPipeline.get(key)));
        }

    }

    @Override
    public Response<Boolean> getbit(final String key, final long offset) {
        return execOp(key, OpName.GETBIT, () -> jedisPipeline.getbit(key, offset));

    }

    @Override
    public Response<String> getrange(final String key, final long startOffset, final long endOffset) {
        return execOp(key, OpName.GETRANGE, () -> jedisPipeline.getrange(key, startOffset, endOffset));

    }

    @Override
    public Response<String> getSet(final String key, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.GETSET, () -> jedisPipeline.getSet(key, value));
        } else {
            return execOp(key, OpName.GETSET, () -> decompress(jedisPipeline.getSet(key, compress(value))));
        }
    }

    @Override
    public Response<Long> hdel(final String key, final String... field) {
        return execOp(key, OpName.HDEL, () -> jedisPipeline.hdel(key, field));

    }

    @Override
    public Response<Boolean> hexists(final String key, final String field) {
        return execOp(key, OpName.HEXISTS, () -> jedisPipeline.hexists(key, field));

    }

    @Override
    public Response<String> hget(final String key, final String field) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HGET, () -> jedisPipeline.hget(key, field));
        } else {
            return execOp(key, OpName.HGET, () -> decompress(jedisPipeline.hget(key, field)));
        }

    }

    @Override
    public Response<Map<String, String>> hgetAll(final String key) {
        return execOp(key, OpName.HGETALL, () -> jedisPipeline.hgetAll(key));
    }

    @Override
    public Response<Long> hincrBy(final String key, final String field, final long value) {
        return execOp(key, OpName.HINCRBY, () -> jedisPipeline.hincrBy(key, field, value));
    }

    /* not supported by RedisPipeline 2.7.3 */
    public Response<Double> hincrByFloat(final String key, final String field, final double value) {
        return execOp(key, OpName.HINCRBYFLOAT, () -> jedisPipeline.hincrByFloat(key, field, value));
    }

    @Override
    public Response<Set<String>> hkeys(final String key) {
        return execOp(key, OpName.HKEYS, () -> jedisPipeline.hkeys(key));

    }

    public Response<ScanResult<Map.Entry<String, String>>> hscan(final String key, int cursor) {
        throw new UnsupportedOperationException("'HSCAN' cannot be called in pipeline");
    }

    @Override
    public Response<Long> hlen(final String key) {
        return execOp(key, OpName.HLEN, () -> jedisPipeline.hlen(key));

    }

    @Override
    public Response<List<String>> hmget(final String key, final String... fields) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HMGET, () -> jedisPipeline.hmget(key, fields));
        } else {
            return execOp(key, OpName.HMGET, () -> decompressList(jedisPipeline.hmget(key, fields)));
        }
    }

    @Override
    public Response<String> hmset(final String key, final Map<String, String> hash) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HMSET, () -> jedisPipeline.hmset(key, hash));
        } else {
            return execOp(key, OpName.HMSET, () -> jedisPipeline.hmset(key, compressMap(hash)));
        }
    }

    @Override
    public Response<Long> hset(final String key, final String field, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HSET, () -> jedisPipeline.hset(key, field, value));
        } else {
            return execOp(key, OpName.HSET, () -> jedisPipeline.hset(key, field, compress(value)));
        }
    }

    @Override
    public Response<Long> hset(String key, Map<String, String> hash) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> hsetnx(final String key, final String field, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HSETNX, () -> jedisPipeline.hsetnx(key, field, value));
        } else {
            return execOp(key, OpName.HSETNX, () -> jedisPipeline.hsetnx(key, field, compress(value)));
        }
    }

    @Override
    public Response<List<String>> hvals(final String key) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HVALS, () -> jedisPipeline.hvals(key));
        } else {
            return execOp(key, OpName.HVALS, () -> decompressList(jedisPipeline.hvals(key)));
        }

    }

    @Override
    public Response<Long> incr(final String key) {
        return execOp(key, OpName.INCR, () -> jedisPipeline.incr(key));

    }

    @Override
    public Response<Long> incrBy(final String key, final long integer) {
        return execOp(key, OpName.INCRBY, () -> jedisPipeline.incrBy(key, integer));

    }

    /* not supported by RedisPipeline 2.7.3 */
    public Response<Double> incrByFloat(final String key, final double increment) {
        return execOp(key, OpName.INCRBYFLOAT, () -> jedisPipeline.incrByFloat(key, increment));

    }

    @Override
    public Response<String> psetex(String key, long milliseconds, String value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<String> lindex(final String key, final long index) {
        return execOp(key, OpName.LINDEX, () -> jedisPipeline.lindex(key, index));

    }

    @Override
    public Response<Long> linsert(final String key, final ListPosition where, final String pivot, final String value) {
        return execOp(key, OpName.LINSERT, () -> jedisPipeline.linsert(key, where, pivot, value));

    }

    @Override
    public Response<Long> llen(final String key) {
        return execOp(key, OpName.LLEN, () -> jedisPipeline.llen(key));

    }

    @Override
    public Response<String> lpop(final String key) {
        return execOp(key, OpName.LPOP, () -> jedisPipeline.lpop(key));

    }

    @Override
    public Response<Long> lpush(final String key, final String... string) {
        return execOp(key, OpName.LPUSH, () -> jedisPipeline.lpush(key, string));

    }

    @Override
    public Response<Long> lpushx(final String key, final String... string) {
        return execOp(key, OpName.LPUSHX, () -> jedisPipeline.lpushx(key, string));

    }

    @Override
    public Response<List<String>> lrange(final String key, final long start, final long end) {
        return execOp(key, OpName.LRANGE, () -> jedisPipeline.lrange(key, start, end));

    }

    @Override
    public Response<Long> lrem(final String key, final long count, final String value) {
        return execOp(key, OpName.LREM, () -> jedisPipeline.lrem(key, count, value));

    }

    @Override
    public Response<String> lset(final String key, final long index, final String value) {
        return execOp(key, OpName.LSET, () -> jedisPipeline.lset(key, index, value));

    }

    @Override
    public Response<String> ltrim(final String key, final long start, final long end) {
        return execOp(key, OpName.LTRIM, () -> jedisPipeline.ltrim(key, start, end));

    }

    @Override
    public Response<Long> move(final String key, final int dbIndex) {
        return execOp(key, OpName.MOVE, () -> jedisPipeline.move(key, dbIndex));

    }

    @Override
    public Response<Long> persist(final String key) {
        return execOp(key, OpName.PERSIST, () -> jedisPipeline.persist(key));

    }

    /* not supported by RedisPipeline 2.7.3 */
    public Response<String> rename(final String oldkey, final String newkey) {
        return execOp(oldkey, OpName.RENAME, () -> jedisPipeline.rename(oldkey, newkey));

    }

    /* not supported by RedisPipeline 2.7.3 */
    public Response<Long> renamenx(final String oldkey, final String newkey) {
        return execOp(oldkey, OpName.RENAMENX, () -> jedisPipeline.renamenx(oldkey, newkey));

    }

    @Override
    public Response<String> rpop(final String key) {
        return execOp(key, OpName.RPOP, () -> jedisPipeline.rpop(key));

    }

    @Override
    public Response<Long> rpush(final String key, final String... string) {
        return execOp(key, OpName.RPUSH, () -> jedisPipeline.rpush(key, string));

    }

    @Override
    public Response<Long> rpushx(final String key, final String... string) {
        return execOp(key, OpName.RPUSHX, () -> jedisPipeline.rpushx(key, string));

    }

    @Override
    public Response<Long> sadd(final String key, final String... member) {
        return execOp(key, OpName.SADD, () -> jedisPipeline.sadd(key, member));

    }

    @Override
    public Response<Long> scard(final String key) {
        return execOp(key, OpName.SCARD, () -> jedisPipeline.scard(key));

    }

    @Override
    public Response<Boolean> sismember(final String key, final String member) {
        return execOp(key, OpName.SISMEMBER, () -> jedisPipeline.sismember(key, member));
    }

    @Override
    public Response<String> set(final String key, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.SET, () -> jedisPipeline.set(key, value));
        } else {
            return execOp(key, OpName.SET, () -> jedisPipeline.set(key, compress(value)));
        }
    }

    @Override
    public Response<Boolean> setbit(final String key, final long offset, final boolean value) {
        return execOp(key, OpName.SETBIT, () -> jedisPipeline.setbit(key, offset, value));

    }

    @Override
    public Response<String> setex(final String key, final int seconds, final String value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.SETEX, () -> jedisPipeline.setex(key, seconds, value));
        } else {
            return execOp(key, OpName.SETEX, () -> jedisPipeline.setex(key, seconds, compress(value)));
        }

    }

    @Override
    public Response<Long> setnx(final String key, final String value) {
        return execOp(key, OpName.SETNX, () -> jedisPipeline.setnx(key, value));
    }

    @Override
    public Response<Long> setrange(final String key, final long offset, final String value) {
        return execOp(key, OpName.SETRANGE, () -> jedisPipeline.setrange(key, offset, value));

    }

    @Override
    public Response<Set<String>> smembers(final String key) {
        return execOp(key, OpName.SMEMBERS, () -> jedisPipeline.smembers(key));

    }

    @Override
    public Response<List<String>> sort(final String key) {
        return execOp(key, OpName.SORT, () -> jedisPipeline.sort(key));

    }

    @Override
    public Response<List<String>> sort(final String key, final SortingParams sortingParameters) {
        return execOp(key, OpName.SORT, () -> jedisPipeline.sort(key, sortingParameters));

    }

    @Override
    public Response<String> spop(final String key) {
        return execOp(key, OpName.SPOP, () -> jedisPipeline.spop(key));

    }

    @Override
    public Response<Set<String>> spop(final String key, final long count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<String> srandmember(final String key) {
        return execOp(key, OpName.SRANDMEMBER, () -> jedisPipeline.srandmember(key));

    }

    @Override
    public Response<Long> srem(final String key, final String... member) {
        return execOp(key, OpName.SREM, () -> jedisPipeline.srem(key, member));

    }

    public Response<ScanResult<String>> sscan(final String key, final int cursor) {
        throw new UnsupportedOperationException("'SSCAN' cannot be called in pipeline");
    }

    public Response<ScanResult<String>> sscan(final String key, final String cursor) {
        throw new UnsupportedOperationException("'SSCAN' cannot be called in pipeline");
    }

    @Override
    public Response<Long> strlen(final String key) {
        return execOp(key, OpName.STRLEN, () -> jedisPipeline.strlen(key));

    }

    @Override
    public Response<String> substr(final String key, final int start, final int end) {
        return execOp(key, OpName.SUBSTR, () -> jedisPipeline.substr(key, start, end));

    }

    @Override
    public Response<Long> touch(String key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> ttl(final String key) {
        return execOp(key, OpName.TTL, () -> jedisPipeline.ttl(key));

    }

    @Override
    public Response<Long> pttl(String key) {
        return execOp(key, OpName.PTTL, () -> jedisPipeline.pttl(key));
    }

    @Override
    public Response<String> type(final String key) {
        return execOp(key, OpName.TYPE, () -> jedisPipeline.type(key));

    }

    @Override
    public Response<Long> zadd(final String key, final double score, final String member) {
        return execOp(key, OpName.ZADD, () -> jedisPipeline.zadd(key, score, member));

    }

    @Override
    public Response<Long> zadd(final String key, final Map<String, Double> scoreMembers) {
        return execOp(key, OpName.ZADD, () -> jedisPipeline.zadd(key, scoreMembers));

    }

    @Override
    public Response<Long> zcard(final String key) {
        return execOp(key, OpName.ZCARD, () -> jedisPipeline.zcard(key));

    }

    @Override
    public Response<Long> zcount(final String key, final double min, final double max) {
        return execOp(key, OpName.ZCOUNT, () -> jedisPipeline.zcount(key, min, max));

    }

    @Override
    public Response<Long> zcount(String key, String min, String max) {
        return execOp(key, OpName.ZCOUNT, () -> jedisPipeline.zcount(key, min, max));
    }

    @Override
    public Response<Double> zincrby(final String key, final double score, final String member) {
        return execOp(key, OpName.ZINCRBY, () -> jedisPipeline.zincrby(key, score, member));

    }

    @Override
    public Response<Set<String>> zrange(final String key, final long start, final long end) {
        return execOp(key, OpName.ZRANGE, () -> jedisPipeline.zrange(key, start, end));

    }

    @Override
    public Response<Set<String>> zrangeByScore(final String key, final double min, final double max) {
        return execOp(key, OpName.ZRANGEBYSCORE, () -> jedisPipeline.zrangeByScore(key, min, max));

    }

    @Override
    public Response<Set<String>> zrangeByScore(final String key, final String min, final String max) {
        return execOp(key, OpName.ZRANGEBYSCORE, () -> jedisPipeline.zrangeByScore(key, min, max));

    }

    @Override
    public Response<Set<String>> zrangeByScore(final String key, final double min, final double max, final int offset,
                                               final int count) {
        return execOp(key, OpName.ZRANGEBYSCORE, () -> jedisPipeline.zrangeByScore(key, min, max, offset, count));

    }

    @Override
    public Response<Set<String>> zrangeByScore(String key, String min, String max, int offset, int count) {
        return execOp(key, OpName.ZRANGEBYSCORE, () -> jedisPipeline.zrangeByScore(key, min, max, offset, count));
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final String key, final double min, final double max) {
        return execOp(key, OpName.ZRANGEBYSCOREWITHSCORES, () -> jedisPipeline.zrangeByScoreWithScores(key, min, max));

    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final String key, final double min, final double max,
                                                        final int offset, final int count) {
        return execOp(key, OpName.ZRANGEBYSCOREWITHSCORES, () -> jedisPipeline.zrangeByScoreWithScores(key, min, max, offset, count));

    }

    @Override
    public Response<Set<String>> zrevrangeByScore(final String key, final double max, final double min) {
        return execOp(key, OpName.ZREVRANGEBYSCORE, () -> jedisPipeline.zrevrangeByScore(key, max, min));

    }

    @Override
    public Response<Set<String>> zrevrangeByScore(final String key, final String max, final String min) {
        return execOp(key, OpName.ZREVRANGEBYSCORE, () -> jedisPipeline.zrevrangeByScore(key, max, min));

    }

    @Override
    public Response<Set<String>> zrevrangeByScore(final String key, final double max, final double min,
                                                  final int offset, final int count) {
        return execOp(key, OpName.ZREVRANGEBYSCORE, () -> jedisPipeline.zrevrangeByScore(key, max, min, offset, count));

    }

    @Override
    public Response<Set<String>> zrevrangeByScore(String key, String max, String min, int offset, int count) {
        return execOp(key, OpName.ZREVRANGEBYSCORE, () -> jedisPipeline.zrevrangeByScore(key, max, min, offset, count));
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key, final double max, final double min) {
        return execOp(key, OpName.ZREVRANGEBYSCOREWITHSCORES, () -> jedisPipeline.zrevrangeByScoreWithScores(key, max, min));

    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(String key, String max, String min) {
        return execOp(key, OpName.ZREVRANGEBYSCOREWITHSCORES, () -> jedisPipeline.zrevrangeByScoreWithScores(key, max, min));
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key, final double max, final double min,
                                                           final int offset, final int count) {
        return execOp(key, OpName.ZREVRANGEBYSCOREWITHSCORES, () -> jedisPipeline.zrevrangeByScoreWithScores(key, max, min, offset, count));

    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
        return execOp(key, OpName.ZREVRANGEBYSCOREWITHSCORES, () -> jedisPipeline.zrevrangeByScoreWithScores(key, max, min, offset, count));
    }

    @Override
    public Response<Set<Tuple>> zrangeWithScores(final String key, final long start, final long end) {
        return execOp(key, OpName.ZRANGEWITHSCORES, () -> jedisPipeline.zrangeWithScores(key, start, end));

    }

    @Override
    public Response<Long> zrank(final String key, final String member) {
        return execOp(key, OpName.ZRANK, () -> jedisPipeline.zrank(key, member));

    }

    @Override
    public Response<Long> zrem(final String key, final String... member) {
        return execOp(key, OpName.ZREM, () -> jedisPipeline.zrem(key, member));

    }

    @Override
    public Response<Long> zremrangeByRank(final String key, final long start, final long end) {
        return execOp(key, OpName.ZREMRANGEBYRANK, () -> jedisPipeline.zremrangeByRank(key, start, end));

    }

    @Override
    public Response<Long> zremrangeByScore(final String key, final double start, final double end) {
        return execOp(key, OpName.ZREMRANGEBYSCORE, () -> jedisPipeline.zremrangeByScore(key, start, end));

    }

    @Override
    public Response<Long> zremrangeByScore(String key, String min, String max) {
        return execOp(key, OpName.ZREMRANGEBYSCORE, () -> jedisPipeline.zremrangeByScore(key, min, max));
    }

    @Override
    public Response<Set<String>> zrevrange(final String key, final long start, final long end) {
        return execOp(key, OpName.ZREVRANGE, () -> jedisPipeline.zrevrange(key, start, end));

    }

    @Override
    public Response<Set<Tuple>> zrevrangeWithScores(final String key, final long start, final long end) {
        return execOp(key, OpName.ZREVRANGEWITHSCORES, () -> jedisPipeline.zrevrangeWithScores(key, start, end));

    }

    @Override
    public Response<Long> zrevrank(final String key, final String member) {
        return execOp(key, OpName.ZREVRANK, () -> jedisPipeline.zrevrank(key, member));

    }

    @Override
    public Response<Double> zscore(final String key, final String member) {
        return execOp(key, OpName.ZSCORE, () -> jedisPipeline.zscore(key, member));

    }

    public Response<ScanResult<Tuple>> zscan(final String key, final int cursor) {
        throw new UnsupportedOperationException("'ZSCAN' cannot be called in pipeline");
    }

    @Override
    public Response<Long> zlexcount(String key, String min, String max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<String>> zrangeByLex(String key, String min, String max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<String>> zrangeByLex(String key, String min, String max, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> zremrangeByLex(String key, String start, String end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> bitcount(final String key) {
        return execOp(key, OpName.BITCOUNT, () -> jedisPipeline.bitcount(key));

    }

    @Override
    public Response<Long> bitcount(final String key, final long start, final long end) {
        return execOp(key, OpName.BITCOUNT, () -> jedisPipeline.bitcount(key, start, end));

    }

    @Override
    public Response<Long> pfadd(String key, String... elements) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> pfcount(String key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<Long>> bitfield(String key, String... arguments) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> hstrlen(String key, String field) {
        return execOp(key, OpName.HSTRLEN, () -> jedisPipeline.hstrlen(key, field));
    }

    @Override
    public Response<String> restore(String key, int ttl, byte[] serializedValue) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<String> restoreReplace(String key, int ttl, byte[] serializedValue) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<byte[]> dump(String key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<String> migrate(String host, int port, String key, int destinationDB, int timeout) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<String>> zrevrangeByLex(String key, String max, String min) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<String>> zrevrangeByLex(String key, String max, String min, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> geoadd(String arg0, Map<String, GeoCoordinate> arg1) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> geoadd(String arg0, double arg1, double arg2, String arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Double> geodist(String arg0, String arg1, String arg2) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Double> geodist(String arg0, String arg1, String arg2, GeoUnit arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<String>> geohash(String arg0, String... arg1) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoCoordinate>> geopos(String arg0, String... arg1) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadius(String arg0, double arg1, double arg2, double arg3,
                                                       GeoUnit arg4) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusReadonly(String key, double longitude, double latitude, double radius, GeoUnit unit) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadius(String arg0, double arg1, double arg2, double arg3, GeoUnit arg4,
                                                       GeoRadiusParam arg5) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusReadonly(String key, double longitude, double latitude, double radius, GeoUnit unit, GeoRadiusParam param) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusByMember(String arg0, String arg1, double arg2, GeoUnit arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusByMemberReadonly(String key, String member, double radius, GeoUnit unit) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusByMember(String arg0, String arg1, double arg2, GeoUnit arg3,
                                                               GeoRadiusParam arg4) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusByMemberReadonly(String key, String member, double radius, GeoUnit unit, GeoRadiusParam param) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> bitpos(String key, boolean value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> bitpos(String key, boolean value, BitPosParams params) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<String> set(String key, String value, SetParams params) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<String>> srandmember(String key, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(String key, String min, String max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> objectRefcount(String key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<String> objectEncoding(String key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> objectIdletime(String key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> zadd(String key, Map<String, Double> members, ZAddParams params) {
        return execOp(key, OpName.ZADD, () -> jedisPipeline.zadd(key, members, params));
    }

    public Response<Long> zadd(final String key, final double score, final String member, final ZAddParams params) {
        return execOp(key, OpName.ZADD, () -> jedisPipeline.zadd(key, score, member, params));
    }

    @Override
    public Response<Double> zincrby(String arg0, double arg1, String arg2, ZIncrByParams arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }


    /******************* End Jedis String Commands **************/


    /******************* Jedis Binary Commands **************/


    @Override
    public Response<Long> append(final byte[] key, final byte[] value) {
        return execOp(key, OpName.APPEND, () -> jedisPipeline.append(key, value));
    }

    @Override
    public Response<List<byte[]>> blpop(final byte[] arg) {
        return execOp(arg, OpName.BLPOP, () -> jedisPipeline.blpop(arg));
    }

    @Override
    public Response<List<byte[]>> brpop(final byte[] arg) {
        return execOp(arg, OpName.BRPOP, () -> jedisPipeline.brpop(arg));
    }

    @Override
    public Response<Long> decr(final byte[] key) {
        return execOp(key, OpName.DECR, () -> jedisPipeline.decr(key));
    }

    @Override
    public Response<Long> decrBy(final byte[] key, final long integer) {
        return execOp(key, OpName.DECRBY, () -> jedisPipeline.decrBy(key, integer));
    }

    @Override
    public Response<Long> del(final byte[] key) {
        return execOp(key, OpName.DEL, () -> jedisPipeline.del(key));

    }

    @Override
    public Response<Long> unlink(byte[] key) {
        return execOp(key, OpName.UNLINK, () -> jedisPipeline.unlink(key));
    }

    @Override
    public Response<byte[]> echo(final byte[] string) {
        return execOp(string, OpName.ECHO, () -> jedisPipeline.echo(string));

    }

    @Override
    public Response<Boolean> exists(final byte[] key) {
        return execOp(key, OpName.EXISTS, () -> jedisPipeline.exists(key));
    }

    @Override
    public Response<Long> expire(final byte[] key, final int seconds) {
        return execOp(key, OpName.EXPIRE, () -> jedisPipeline.expire(key, seconds));

    }

    @Override
    public Response<Long> pexpire(byte[] key, long milliseconds) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> expireAt(final byte[] key, final long unixTime) {
        return execOp(key, OpName.EXPIREAT, () -> jedisPipeline.expireAt(key, unixTime));

    }

    @Override
    public Response<Long> pexpireAt(byte[] key, long millisecondsTimestamp) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<byte[]> get(final byte[] key) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.GET, () -> jedisPipeline.get(key));
        } else {
            return execOp(key, OpName.GET, () -> decompressBin(jedisPipeline.get(key)));
        }

    }

    @Override
    public Response<Boolean> getbit(final byte[] key, final long offset) {
        return execOp(key, OpName.GETBIT, () -> jedisPipeline.getbit(key, offset));

    }

    @Override
    public Response<byte[]> getrange(final byte[] key, final long startOffset, final long endOffset) {
        return execOp(key, OpName.GETRANGE, () -> jedisPipeline.getrange(key, startOffset, endOffset));

    }

    @Override
    public Response<byte[]> getSet(final byte[] key, final byte[] value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.GETSET, () -> jedisPipeline.getSet(key, value));
        } else {
            return execOp(key, OpName.GETSET, () -> decompressBin(jedisPipeline.getSet(key, compressBin(value))));
        }
    }

    @Override
    public Response<Long> hdel(final byte[] key, final byte[]... field) {
        return execOp(key, OpName.HDEL, () -> jedisPipeline.hdel(key, field));

    }

    @Override
    public Response<Boolean> hexists(final byte[] key, final byte[] field) {
        return execOp(key, OpName.HEXISTS, () -> jedisPipeline.hexists(key, field));

    }

    @Override
    public Response<byte[]> hget(final byte[] key, final byte[] field) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HGET, () -> jedisPipeline.hget(key, field));
        } else {
            return execOp(key, OpName.HGET, () -> decompressBin(jedisPipeline.hget(key, field)));
        }

    }

    @Override
    public Response<Map<byte[], byte[]>> hgetAll(final byte[] key) {
        return execOp(key, OpName.HGETALL, () -> jedisPipeline.hgetAll(key));
    }

    @Override
    public Response<Long> hincrBy(final byte[] key, final byte[] field, final long value) {
        return execOp(key, OpName.HINCRBY, () -> jedisPipeline.hincrBy(key, field, value));
    }

    /* not supported by RedisPipeline 2.7.3 */
    public Response<Double> hincrByFloat(final byte[] key, final byte[] field, final double value) {
        return execOp(key, OpName.HINCRBYFLOAT, () -> jedisPipeline.hincrByFloat(key, field, value));
    }

    @Override
    public Response<Set<byte[]>> hkeys(final byte[] key) {
        return execOp(key, OpName.HKEYS, () -> jedisPipeline.hkeys(key));

    }

    public Response<ScanResult<Map.Entry<byte[], byte[]>>> hscan(final byte[] key, int cursor) {
        throw new UnsupportedOperationException("'HSCAN' cannot be called in pipeline");
    }

    @Override
    public Response<Long> hlen(final byte[] key) {
        return execOp(key, OpName.HLEN, () -> jedisPipeline.hlen(key));

    }

    @Override
    public Response<List<byte[]>> hmget(final byte[] key, final byte[]... fields) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HMGET, () -> jedisPipeline.hmget(key, fields));
        } else {
            return execOp(key, OpName.HMGET, () -> decompressBinList(jedisPipeline.hmget(key, fields)));
        }
    }

    @Override
    public Response<String> hmset(final byte[] key, final Map<byte[], byte[]> hash) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HMSET, () -> jedisPipeline.hmset(key, hash));
        } else {
            return execOp(key, OpName.HMSET, () -> jedisPipeline.hmset(key, compressBinMap(hash)));
        }
    }

    @Override
    public Response<Long> hset(final byte[] key, final byte[] field, final byte[] value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HSET, () -> jedisPipeline.hset(key, field, value));
        } else {
            return execOp(key, OpName.HSET, () -> jedisPipeline.hset(key, field, compressBin(value)));
        }
    }

    @Override
    public Response<Long> hset(byte[] key, Map<byte[], byte[]> hash) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> hsetnx(final byte[] key, final byte[] field, final byte[] value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HSETNX, () -> jedisPipeline.hsetnx(key, field, value));
        } else {
            return execOp(key, OpName.HSETNX, () -> jedisPipeline.hsetnx(key, field, compressBin(value)));
        }
    }

    @Override
    public Response<List<byte[]>> hvals(final byte[] key) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.HVALS, () -> jedisPipeline.hvals(key));
        } else {
            return execOp(key, OpName.HVALS, () -> decompressBinList(jedisPipeline.hvals(key)));
        }

    }

    @Override
    public Response<Long> incr(final byte[] key) {
        return execOp(key, OpName.INCR, () -> jedisPipeline.incr(key));

    }

    @Override
    public Response<Long> incrBy(final byte[] key, final long integer) {
        return execOp(key, OpName.INCRBY, () -> jedisPipeline.incrBy(key, integer));

    }

    /* not supported by RedisPipeline 2.7.3 */
    public Response<Double> incrByFloat(final byte[] key, final double increment) {
        return execOp(key, OpName.INCRBYFLOAT, () -> jedisPipeline.incrByFloat(key, increment));

    }

    @Override
    public Response<String> psetex(byte[] key, long milliseconds, byte[] value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<byte[]> lindex(final byte[] key, final long index) {
        return execOp(key, OpName.LINDEX, () -> jedisPipeline.lindex(key, index));

    }

    @Override
    public Response<Long> linsert(final byte[] key, final ListPosition where, final byte[] pivot, final byte[] value) {
        return execOp(key, OpName.LINSERT, () -> jedisPipeline.linsert(key, where, pivot, value));

    }

    @Override
    public Response<Long> llen(final byte[] key) {
        return execOp(key, OpName.LLEN, () -> jedisPipeline.llen(key));

    }

    @Override
    public Response<byte[]> lpop(final byte[] key) {
        return execOp(key, OpName.LPOP, () -> jedisPipeline.lpop(key));

    }

    @Override
    public Response<Long> lpush(final byte[] key, final byte[]... string) {
        return execOp(key, OpName.LPUSH, () -> jedisPipeline.lpush(key, string));

    }

    @Override
    public Response<Long> lpushx(final byte[] key, final byte[]... string) {
        return execOp(key, OpName.LPUSHX, () -> jedisPipeline.lpushx(key, string));

    }

    @Override
    public Response<List<byte[]>> lrange(final byte[] key, final long start, final long end) {
        return execOp(key, OpName.LRANGE, () -> jedisPipeline.lrange(key, start, end));

    }

    @Override
    public Response<Long> lrem(final byte[] key, final long count, final byte[] value) {
        return execOp(key, OpName.LREM, () -> jedisPipeline.lrem(key, count, value));

    }

    @Override
    public Response<String> lset(final byte[] key, final long index, final byte[] value) {
        return execOp(key, OpName.LSET, () -> jedisPipeline.lset(key, index, value));

    }

    @Override
    public Response<String> ltrim(final byte[] key, final long start, final long end) {
        return execOp(key, OpName.LTRIM, () -> jedisPipeline.ltrim(key, start, end));

    }

    @Override
    public Response<Long> move(final byte[] key, final int dbIndex) {
        return execOp(key, OpName.MOVE, () -> jedisPipeline.move(key, dbIndex));

    }

    @Override
    public Response<Long> persist(final byte[] key) {
        return execOp(key, OpName.PERSIST, () -> jedisPipeline.persist(key));

    }

    /* not supported by RedisPipeline 2.7.3 */
    public Response<String> rename(final byte[] oldkey, final byte[] newkey) {
        return execOp(oldkey, OpName.RENAME, () -> jedisPipeline.rename(oldkey, newkey));

    }

    /* not supported by RedisPipeline 2.7.3 */
    public Response<Long> renamenx(final byte[] oldkey, final byte[] newkey) {
        return execOp(oldkey, OpName.RENAMENX, () -> jedisPipeline.renamenx(oldkey, newkey));

    }

    @Override
    public Response<byte[]> rpop(final byte[] key) {
        return execOp(key, OpName.RPOP, () -> jedisPipeline.rpop(key));

    }

    @Override
    public Response<Long> rpush(final byte[] key, final byte[]... string) {
        return execOp(key, OpName.RPUSH, () -> jedisPipeline.rpush(key, string));

    }

    @Override
    public Response<Long> rpushx(final byte[] key, final byte[]... string) {
        return execOp(key, OpName.RPUSHX, () -> jedisPipeline.rpushx(key, string));

    }

    @Override
    public Response<Long> sadd(final byte[] key, final byte[]... member) {
        return execOp(key, OpName.SADD, () -> jedisPipeline.sadd(key, member));

    }

    @Override
    public Response<Long> scard(final byte[] key) {
        return execOp(key, OpName.SCARD, () -> jedisPipeline.scard(key));

    }

    @Override
    public Response<Boolean> sismember(final byte[] key, final byte[] member) {
        return execOp(key, OpName.SISMEMBER, () -> jedisPipeline.sismember(key, member));
    }

    @Override
    public Response<String> set(final byte[] key, final byte[] value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.SET, () -> jedisPipeline.set(key, value));
        } else {
            return execOp(key, OpName.SET, () -> jedisPipeline.set(key, compressBin(value)));
        }
    }

    @Override
    public Response<Boolean> setbit(final byte[] key, final long offset, final byte[] value) {
        return execOp(key, OpName.SETBIT, () -> jedisPipeline.setbit(key, offset, value));

    }

    @Override
    public Response<String> setex(final byte[] key, final int seconds, final byte[] value) {
        if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
            return execOp(key, OpName.SETEX, () -> jedisPipeline.setex(key, seconds, value));
        } else {
            return execOp(key, OpName.SETEX, () -> jedisPipeline.setex(key, seconds, compressBin(value)));
        }

    }

    @Override
    public Response<Long> setnx(final byte[] key, final byte[] value) {
        return execOp(key, OpName.SETNX, () -> jedisPipeline.setnx(key, value));
    }

    @Override
    public Response<Long> setrange(final byte[] key, final long offset, final byte[] value) {
        return execOp(key, OpName.SETRANGE, () -> jedisPipeline.setrange(key, offset, value));

    }

    @Override
    public Response<Set<byte[]>> smembers(final byte[] key) {
        return execOp(key, OpName.SMEMBERS, () -> jedisPipeline.smembers(key));
    }

    @Override
    public Response<List<byte[]>> sort(final byte[] key) {
        return execOp(key, OpName.SORT, () -> jedisPipeline.sort(key));

    }

    @Override
    public Response<List<byte[]>> sort(final byte[] key, final SortingParams sortingParameters) {
        return execOp(key, OpName.SORT, () -> jedisPipeline.sort(key, sortingParameters));

    }

    @Override
    public Response<byte[]> spop(final byte[] key) {
        return execOp(key, OpName.SPOP, () -> jedisPipeline.spop(key));

    }

    @Override
    public Response<Set<byte[]>> spop(final byte[] key, final long count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<byte[]> srandmember(final byte[] key) {
        return execOp(key, OpName.SRANDMEMBER, () -> jedisPipeline.srandmember(key));

    }

    @Override
    public Response<Long> srem(final byte[] key, final byte[]... member) {
        return execOp(key, OpName.SREM, () -> jedisPipeline.srem(key, member));

    }

    /**
     * This method is not supported by the BinaryRedisPipeline interface.
     */
    public Response<ScanResult<byte[]>> sscan(final byte[] key, final int cursor) {
        throw new UnsupportedOperationException("'SSCAN' cannot be called in pipeline");
    }

    /**
     * This method is not supported by the BinaryRedisPipeline interface.
     */
    public Response<ScanResult<byte[]>> sscan(final byte[] key, final byte[] cursor) {
        throw new UnsupportedOperationException("'SSCAN' cannot be called in pipeline");
    }

    @Override
    public Response<Long> strlen(final byte[] key) {
        return execOp(key, OpName.STRLEN, () -> jedisPipeline.strlen(key));

    }

    @Override
    public Response<String> substr(final byte[] key, final int start, final int end) {
        return execOp(key, OpName.SUBSTR, () -> jedisPipeline.substr(key, start, end));

    }

    @Override
    public Response<Long> touch(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> ttl(final byte[] key) {
        return execOp(key, OpName.TTL, () -> jedisPipeline.ttl(key));

    }

    @Override
    public Response<Long> pttl(byte[] key) {
        return execOp(key, OpName.PTTL, () -> jedisPipeline.pttl(key));
    }

    @Override
    public Response<String> type(final byte[] key) {
        return execOp(key, OpName.TYPE, () -> jedisPipeline.type(key));

    }

    @Override
    public Response<Long> zadd(final byte[] key, final double score, final byte[] member) {
        return execOp(key, OpName.ZADD, () -> jedisPipeline.zadd(key, score, member));

    }

    @Override
    public Response<Long> zadd(final byte[] key, final Map<byte[], Double> scoreMembers) {
        return execOp(key, OpName.ZADD, () -> jedisPipeline.zadd(key, scoreMembers));

    }

    @Override
    public Response<Long> zcard(final byte[] key) {
        return execOp(key, OpName.ZCARD, () -> jedisPipeline.zcard(key));

    }

    @Override
    public Response<Long> zcount(final byte[] key, final double min, final double max) {
        return execOp(key, OpName.ZCOUNT, () -> jedisPipeline.zcount(key, min, max));

    }

    @Override
    public Response<Long> zcount(byte[] key, byte[] min, byte[] max) {
        return execOp(key, OpName.ZCOUNT, () -> jedisPipeline.zcount(key, min, max));
    }

    @Override
    public Response<Double> zincrby(final byte[] key, final double score, final byte[] member) {
        return execOp(key, OpName.ZINCRBY, () -> jedisPipeline.zincrby(key, score, member));

    }

    @Override
    public Response<Set<byte[]>> zrange(final byte[] key, final long start, final long end) {
        return execOp(key, OpName.ZRANGE, () -> jedisPipeline.zrange(key, start, end));

    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(final byte[] key, final double min, final double max) {
        return execOp(key, OpName.ZRANGEBYSCORE, () -> jedisPipeline.zrangeByScore(key, min, max));

    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(final byte[] key, final byte[] min, final byte[] max) {
        return execOp(key, OpName.ZRANGEBYSCORE, () -> jedisPipeline.zrangeByScore(key, min, max));

    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(final byte[] key, final double min, final double max, final int offset,
                                               final int count) {
        return execOp(key, OpName.ZRANGEBYSCORE, () -> jedisPipeline.zrangeByScore(key, min, max, offset, count));

    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
        return execOp(key, OpName.ZRANGEBYSCORE, () -> jedisPipeline.zrangeByScore(key, min, max, offset, count));
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final byte[] key, final double min, final double max) {
        return execOp(key, OpName.ZRANGEBYSCOREWITHSCORES, () -> jedisPipeline.zrangeByScoreWithScores(key, min, max));

    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final byte[] key, final double min, final double max,
                                                        final int offset, final int count) {
        return execOp(key, OpName.ZRANGEBYSCOREWITHSCORES, () -> jedisPipeline.zrangeByScoreWithScores(key, min, max, offset, count));

    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(final byte[] key, final double max, final double min) {
        return execOp(key, OpName.ZREVRANGEBYSCORE, () -> jedisPipeline.zrevrangeByScore(key, max, min));

    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(final byte[] key, final byte[] max, final byte[] min) {
        return execOp(key, OpName.ZREVRANGEBYSCORE, () -> jedisPipeline.zrevrangeByScore(key, max, min));

    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(final byte[] key, final double max, final double min,
                                                  final int offset, final int count) {
        return execOp(key, OpName.ZREVRANGEBYSCORE, () -> jedisPipeline.zrevrangeByScore(key, max, min, offset, count));

    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
        return execOp(key, OpName.ZREVRANGEBYSCORE, () -> jedisPipeline.zrevrangeByScore(key, max, min, offset, count));
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min) {
        return execOp(key, OpName.ZREVRANGEBYSCOREWITHSCORES, () -> jedisPipeline.zrevrangeByScoreWithScores(key, max, min));

    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
        return execOp(key, OpName.ZREVRANGEBYSCOREWITHSCORES, () -> jedisPipeline.zrevrangeByScoreWithScores(key, max, min));
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final byte[] key, final double max, final double min,
                                                           final int offset, final int count) {
        return execOp(key, OpName.ZREVRANGEBYSCOREWITHSCORES, () -> jedisPipeline.zrevrangeByScoreWithScores(key, max, min, offset, count));

    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset, int count) {
        return execOp(key, OpName.ZREVRANGEBYSCOREWITHSCORES, () -> jedisPipeline.zrevrangeByScoreWithScores(key, max, min, offset, count));
    }

    @Override
    public Response<Set<Tuple>> zrangeWithScores(final byte[] key, final long start, final long end) {
        return execOp(key, OpName.ZRANGEWITHSCORES, () -> jedisPipeline.zrangeWithScores(key, start, end));

    }

    @Override
    public Response<Long> zrank(final byte[] key, final byte[] member) {
        return execOp(key, OpName.ZRANK, () -> jedisPipeline.zrank(key, member));

    }

    @Override
    public Response<Long> zrem(final byte[] key, final byte[]... member) {
        return execOp(key, OpName.ZREM, () -> jedisPipeline.zrem(key, member));

    }

    @Override
    public Response<Long> zremrangeByRank(final byte[] key, final long start, final long end) {
        return execOp(key, OpName.ZREMRANGEBYRANK, () -> jedisPipeline.zremrangeByRank(key, start, end));

    }

    @Override
    public Response<Long> zremrangeByScore(final byte[] key, final double start, final double end) {
        return execOp(key, OpName.ZREMRANGEBYSCORE, () -> jedisPipeline.zremrangeByScore(key, start, end));

    }

    @Override
    public Response<Long> zremrangeByScore(byte[] key, byte[] min, byte[] max) {
        return execOp(key, OpName.ZREMRANGEBYSCORE, () -> jedisPipeline.zremrangeByScore(key, min, max));
    }

    @Override
    public Response<Set<byte[]>> zrevrange(final byte[] key, final long start, final long end) {
        return execOp(key, OpName.ZREVRANGE, () -> jedisPipeline.zrevrange(key, start, end));

    }

    @Override
    public Response<Set<Tuple>> zrevrangeWithScores(final byte[] key, final long start, final long end) {
        return execOp(key, OpName.ZREVRANGEWITHSCORES, () -> jedisPipeline.zrevrangeWithScores(key, start, end));

    }

    @Override
    public Response<Long> zrevrank(final byte[] key, final byte[] member) {
        return execOp(key, OpName.ZREVRANK, () -> jedisPipeline.zrevrank(key, member));

    }

    @Override
    public Response<Double> zscore(final byte[] key, final byte[] member) {
        return execOp(key, OpName.ZSCORE, () -> jedisPipeline.zscore(key, member));

    }

    /**
     * This method is not supported by the BinaryRedisPipeline interface.
     */
    public Response<ScanResult<Tuple>> zscan(final byte[] key, final int cursor) {
        throw new UnsupportedOperationException("'ZSCAN' cannot be called in pipeline");
    }

    @Override
    public Response<Long> zlexcount(byte[] key, byte[] min, byte[] max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<byte[]>> zrangeByLex(byte[] key, byte[] min, byte[] max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<byte[]>> zrangeByLex(byte[] key, byte[] min, byte[] max, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> zremrangeByLex(byte[] key, byte[] start, byte[] end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> bitcount(final byte[] key) {
        return execOp(key, OpName.BITCOUNT, () -> jedisPipeline.bitcount(key));

    }

    @Override
    public Response<Long> bitcount(final byte[] key, final long start, final long end) {
        return execOp(key, OpName.BITCOUNT, () -> jedisPipeline.bitcount(key, start, end));

    }

    @Override
    public Response<Long> pfadd(byte[] key, byte[]... elements) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> pfcount(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<Long>> bitfield(byte[] key, byte[]... arguments) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> hstrlen(byte[] key, byte[] field) {
        return execOp(key, OpName.HSTRLEN, () -> jedisPipeline.hstrlen(key, field));
    }

    @Override
    public Response<String> restore(byte[] key, int ttl, byte[] serializedValue) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<String> restoreReplace(byte[] key, int ttl, byte[] serializedValue) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<byte[]> dump(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<String> migrate(String host, int port, byte[] key, int destinationDB, int timeout) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByLex(byte[] key, byte[] max, byte[] min) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByLex(byte[] key, byte[] max, byte[] min, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> geoadd(byte[] arg0, Map<byte[], GeoCoordinate> arg1) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> geoadd(byte[] arg0, double arg1, double arg2, byte[] arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Double> geodist(byte[] arg0, byte[] arg1, byte[] arg2) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Double> geodist(byte[] arg0, byte[] arg1, byte[] arg2, GeoUnit arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<byte[]>> geohash(byte[] arg0, byte[]... arg1) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoCoordinate>> geopos(byte[] arg0, byte[]... arg1) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadius(byte[] arg0, double arg1, double arg2, double arg3,
                                                       GeoUnit arg4) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusReadonly(byte[] key, double longitude, double latitude, double radius, GeoUnit unit) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadius(byte[] arg0, double arg1, double arg2, double arg3, GeoUnit arg4,
                                                       GeoRadiusParam arg5) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusReadonly(byte[] key, double longitude, double latitude, double radius, GeoUnit unit, GeoRadiusParam param) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusByMember(byte[] arg0, byte[] arg1, double arg2, GeoUnit arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusByMemberReadonly(byte[] key, byte[] member, double radius, GeoUnit unit) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusByMember(byte[] arg0, byte[] arg1, double arg2, GeoUnit arg3,
                                                               GeoRadiusParam arg4) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusByMemberReadonly(byte[] key, byte[] member, double radius, GeoUnit unit, GeoRadiusParam param) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> bitpos(byte[] key, boolean value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> bitpos(byte[] key, boolean value, BitPosParams params) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<String> set(byte[] key, byte[] value, SetParams params) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<byte[]>> srandmember(byte[] key, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> objectRefcount(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<byte[]> objectEncoding(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> objectIdletime(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> zadd(byte[] key, Map<byte[], Double> members, ZAddParams params) {
        return execOp(key, OpName.ZADD, () -> jedisPipeline.zadd(key, members, params));
    }

    public Response<Long> zadd(final byte[] key, final double score, final byte[] member, final ZAddParams params) {
        return execOp(key, OpName.ZADD, () -> jedisPipeline.zadd(key, score, member, params));
    }

    @Override
    public Response<Double> zincrby(byte[] arg0, double arg1, byte[] arg2, ZIncrByParams arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }


    /******************* End Jedis Binary Commands **************/


    public void sync() {
        long startTime = System.nanoTime() / 1000;
        try {
            jedisPipeline.sync();
            opMonitor.recordPipelineSync();
        } catch (JedisConnectionException jce) {
            String msg = "Failed sync() to host: " + getHostInfo();
            pipelineEx.set(new FatalConnectionException(msg, jce).
              setHost(connection == null ? Host.NO_HOST : connection.getHost()));
            cpMonitor.incOperationFailure(connection == null ? null : connection.getHost(), jce);
            throw jce;
        } finally {
            long duration = System.nanoTime() / 1000 - startTime;
            opMonitor.recordLatency(duration, TimeUnit.MICROSECONDS);
            discardPipeline(false);
            releaseConnection();
        }
    }

    public List<Object> syncAndReturnAll() {
        long startTime = System.nanoTime() / 1000;
        try {
            List<Object> result = jedisPipeline.syncAndReturnAll();
            opMonitor.recordPipelineSync();
            return result;
        } catch (JedisConnectionException jce) {
            String msg = "Failed syncAndReturnAll() to host: " + getHostInfo();
            pipelineEx.set(new FatalConnectionException(msg, jce).
              setHost(connection == null ? Host.NO_HOST : connection.getHost()));
            cpMonitor.incOperationFailure(connection == null ? null : connection.getHost(), jce);
            throw jce;
        } finally {
            long duration = System.nanoTime() / 1000 - startTime;
            opMonitor.recordLatency(duration, TimeUnit.MICROSECONDS);
            discardPipeline(false);
            releaseConnection();
        }
    }

    private void discardPipeline(boolean recordLatency) {
        if (jedisPipeline != null) {
            try {
                long startTime = System.nanoTime() / 1000;
                jedisPipeline.sync();
                if (recordLatency) {
                    long duration = System.nanoTime() / 1000 - startTime;
                    opMonitor.recordLatency(duration, TimeUnit.MICROSECONDS);
                }
            } catch (Exception e) {
                Logger.warn(String.format("Failed to discard jedis pipeline, %s", getHostInfo()), e);
            } finally {
                jedisPipeline = null;
            }
        }
    }

    private void releaseConnection() {
        if (connection != null) {
            try {
                connection.getContext().reset();
                connection.getParentConnectionPool().returnConnection(connection);
                if (pipelineEx.get() != null) {
                    connPool.getHealthTracker().trackConnectionError(connection.getParentConnectionPool(),
                                                                     pipelineEx.get());
                    pipelineEx.set(null);
                }
            } catch (Exception e) {
                Logger.warn(String.format("Failed to return connection in Dyno Jedis Pipeline, %s", getHostInfo()), e);
            } finally {
                connection = null;
            }
        }
    }

    public void discardPipelineAndReleaseConnection() {
        opMonitor.recordPipelineDiscard();
        discardPipeline(true);
        releaseConnection();
    }

    @Override
    public void close() throws Exception {
        discardPipelineAndReleaseConnection();
    }

    private String getHostInfo() {
        if (connection != null && connection.getHost() != null) {
            return connection.getHost().toString();
        }

        return "unknown";
    }

}
