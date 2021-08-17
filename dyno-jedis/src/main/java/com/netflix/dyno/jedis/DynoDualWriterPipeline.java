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

import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.params.ZIncrByParams;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Dual writer for pipeline commands. This dual writer will apply mutation to both
 * the primary and shadow Dyno clusters but the response returned is only for the
 * primary dynomite cluster. Non-mutation operations are targeted only to primary
 * dynomite clusters.
 */
public class DynoDualWriterPipeline extends DynoJedisPipeline {


    // FIXME: not all write commands are shadowed


    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DynoDualWriterPipeline.class);
    private static ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ConnectionPoolImpl<Jedis> connPool;
    private final DynoJedisPipeline shadowPipeline;
    private final DynoDualWriterClient.Dial dial;

    DynoDualWriterPipeline(String appName,
                           ConnectionPoolImpl<Jedis> pool,
                           DynoJedisPipelineMonitor operationMonitor,
                           ConnectionPoolMonitor connPoolMonitor,
                           ConnectionPoolImpl<Jedis> shadowConnectionPool,
                           DynoDualWriterClient.Dial dial) {
        super(pool, operationMonitor, connPoolMonitor);
        this.connPool = pool;
        this.dial = dial;

        // use a new pipeline monitor for shadow cluster
        int flushTimerFrequency = shadowConnectionPool.getConfiguration().getTimingCountersResetFrequencySeconds();
        DynoJedisPipelineMonitor shadowOperationMonitor = new DynoJedisPipelineMonitor(appName, flushTimerFrequency);
        this.shadowPipeline = new DynoJedisPipeline(shadowConnectionPool, shadowOperationMonitor,
                shadowConnectionPool.getMonitor());
    }

    private ConnectionPoolImpl<Jedis> getConnPool() {
        return this.connPool;
    }

    /*
     * For async scheduling of Jedis commands on shadow clusters.
     */
    private <R> Future<Response<R>> writeAsync(final String key, Callable<Response<R>> func) {
        if (canSendShadowRequest(key)) {
            return executor.submit(func);
        }
        return null;
    }

    /*
     *  For async scheduling of Jedis binary commands on shadow clusters.
     */
    private <R> Future<Response<R>> writeAsync(final byte[] key, Callable<Response<R>> func) {
        if (canSendShadowRequest(key)) {
            return executor.submit(func);
        }
        return null;
    }

    /*
     * Asynchronous processing of non Jedis commands. No dial check as these operations are preceded
     * by Jedis pipeline commands that perform dial check on shadow cluster.
     */
    private <R> Future<R> scheduleAsync(Callable<R> func) {
        return executor.submit(func);
    }

    /**
     * Returns true if the connection pool
     * <li>Is NOT idle</li>
     * <li>Has active pools (the shadow cluster may disappear at any time and we don't want to bloat logs)</li>
     * <li>The key is in range in the dial</li>
     * <p>
     * The idle check is necessary since there may be active host pools however the shadow client may not be able to
     * connect to them, for example, if security groups are not configured properly.
     */
    private boolean canSendShadowRequest(String key) {
        return this.getConnPool().getConfiguration().isDualWriteEnabled() &&
                !this.getConnPool().isIdle() &&
                this.getConnPool().getActivePools().size() > 0 &&
                dial.isInRange(key);
    }

    private boolean canSendShadowRequest(byte[] key) {
        return this.getConnPool().getConfiguration().isDualWriteEnabled() &&
                !this.getConnPool().isIdle() &&
                this.getConnPool().getActivePools().size() > 0 &&
                dial.isInRange(key);
    }

    /**
     * Sync operation will wait for primary cluster and the result is returned. But,
     * on a shadow cluster the operation is asynchronous and the result is not
     * returned to client.
     */
    @Override
    public void sync() {
        scheduleAsync(() -> {
            shadowPipeline.sync();
            return null;
        });
        super.sync();
    }

    @Override
    public List<Object> syncAndReturnAll() {
        scheduleAsync(() -> {
            this.shadowPipeline.sync();
            return null;
        });
        return super.syncAndReturnAll();
    }

    @Override
    public void discardPipelineAndReleaseConnection() {
        scheduleAsync(() -> {
            this.shadowPipeline.discardPipelineAndReleaseConnection();
            return null;
        });
        super.discardPipelineAndReleaseConnection();
    }


    @Override
    public void close() throws Exception {
        this.shadowPipeline.close(); // close the shawdow pipeline synchronously
        super.close();
    }

    //-------------------------- JEDIS PIPELINE COMMANDS ----------------------------

    @Override
    public Response<Long> append(final String key, final String value) {
        writeAsync(key, () -> shadowPipeline.append(key, value));

        return DynoDualWriterPipeline.super.append(key, value);
    }

    @Override
    public Response<List<String>> blpop(final String arg) {
        writeAsync(arg, () -> shadowPipeline.blpop(arg));

        return DynoDualWriterPipeline.super.blpop(arg);
    }

    @Override
    public Response<List<String>> brpop(final String arg) {
        writeAsync(arg, () -> shadowPipeline.brpop(arg));

        return DynoDualWriterPipeline.super.brpop(arg);
    }

    @Override
    public Response<Long> decr(final String key) {
        writeAsync(key, () -> shadowPipeline.decr(key));

        return DynoDualWriterPipeline.super.decr(key);
    }

    @Override
    public Response<Long> decrBy(final String key, final long integer) {
        writeAsync(key, () -> shadowPipeline.decrBy(key, integer));

        return DynoDualWriterPipeline.super.decrBy(key, integer);
    }

    @Override
    public Response<Long> del(final String key) {
        writeAsync(key, () -> shadowPipeline.del(key));

        return DynoDualWriterPipeline.super.del(key);
    }

    @Override
    public Response<Long> expire(final String key, final int seconds) {
        writeAsync(key, () -> shadowPipeline.expire(key, seconds));

        return DynoDualWriterPipeline.super.expire(key, seconds);
    }

    @Override
    public Response<Long> pexpire(String key, long milliseconds) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> expireAt(final String key, final long unixTime) {
        writeAsync(key, () -> shadowPipeline.expireAt(key, unixTime));

        return DynoDualWriterPipeline.super.expireAt(key, unixTime);
    }

    @Override
    public Response<Long> pexpireAt(String key, long millisecondsTimestamp) {
        throw new UnsupportedOperationException("not yet implemented");
    }


    @Override
    public Response<Long> hdel(final String key, final String... field) {
        writeAsync(key, () -> shadowPipeline.hdel(key, field));

        return DynoDualWriterPipeline.super.hdel(key, field);
    }

    @Override
    public Response<Long> hincrBy(final String key, final String field, final long value) {
        writeAsync(key, () -> shadowPipeline.hincrBy(key, field, value));

        return DynoDualWriterPipeline.super.hincrBy(key, field, value);
    }

    /* not supported by RedisPipeline 2.7.3 */
    public Response<Double> hincrByFloat(final String key, final String field, final double value) {
        writeAsync(key, () -> shadowPipeline.hincrByFloat(key, field, value));

        return DynoDualWriterPipeline.super.hincrByFloat(key, field, value);
    }

    /**
     * This method is a BinaryRedisPipeline command which dyno does not yet properly
     * support, therefore the interface is not yet implemented since only a few
     * binary commands are present.
     */
    public Response<String> hmset(final byte[] key, final Map<byte[], byte[]> hash) {
        writeAsync(key, () -> shadowPipeline.hmset(key, hash));

        return DynoDualWriterPipeline.super.hmset(key, hash);
    }

    @Override
    public Response<String> hmset(final String key, final Map<String, String> hash) {
        writeAsync(key, () -> shadowPipeline.hmset(key, hash));

        return DynoDualWriterPipeline.super.hmset(key, hash);
    }

    @Override
    public Response<Long> hset(final String key, final String field, final String value) {
        writeAsync(key, () -> shadowPipeline.hset(key, field, value));

        return DynoDualWriterPipeline.super.hset(key, field, value);
    }

    /**
     * This method is a BinaryRedisPipeline command which dyno does not yet properly
     * support, therefore the interface is not yet implemented.
     */
    public Response<Long> hset(final byte[] key, final byte[] field, final byte[] value) {
        writeAsync(key, () -> shadowPipeline.hset(key, field, value));

        return DynoDualWriterPipeline.super.hset(key, field, value);
    }

    @Override
    public Response<Long> hsetnx(final String key, final String field, final String value) {
        writeAsync(key, () -> shadowPipeline.hsetnx(key, field, value));

        return DynoDualWriterPipeline.super.hsetnx(key, field, value);
    }

    @Override
    public Response<Long> incr(final String key) {
        writeAsync(key, () -> shadowPipeline.incr(key));

        return DynoDualWriterPipeline.super.incr(key);
    }

    @Override
    public Response<Long> incrBy(final String key, final long integer) {
        writeAsync(key, () -> shadowPipeline.incrBy(key, integer));

        return DynoDualWriterPipeline.super.incrBy(key, integer);
    }

    /* not supported by RedisPipeline 2.7.3 */
    public Response<Double> incrByFloat(final String key, final double increment) {
        writeAsync(key, () -> shadowPipeline.incrByFloat(key, increment));

        return DynoDualWriterPipeline.super.incrByFloat(key, increment);
    }

    @Override
    public Response<Long> linsert(final String key, final ListPosition where, final String pivot, final String value) {
        writeAsync(key, () -> shadowPipeline.linsert(key, where, pivot, value));

        return DynoDualWriterPipeline.super.linsert(key, where, pivot, value);
    }

    @Override
    public Response<String> lpop(final String key) {
        writeAsync(key, () -> shadowPipeline.lpop(key));

        return DynoDualWriterPipeline.super.lpop(key);
    }

    @Override
    public Response<Long> lpush(final String key, final String... string) {
        writeAsync(key, () -> shadowPipeline.lpush(key, string));

        return DynoDualWriterPipeline.super.lpush(key, string);
    }

    @Override
    public Response<Long> lpushx(final String key, final String... string) {
        writeAsync(key, () -> shadowPipeline.lpushx(key, string));

        return DynoDualWriterPipeline.super.lpushx(key, string);
    }

    @Override
    public Response<Long> lrem(final String key, final long count, final String value) {
        writeAsync(key, () -> shadowPipeline.lrem(key, count, value));

        return DynoDualWriterPipeline.super.lrem(key, count, value);
    }

    @Override
    public Response<String> lset(final String key, final long index, final String value) {
        writeAsync(key, () -> shadowPipeline.lset(key, index, value));

        return DynoDualWriterPipeline.super.lset(key, index, value);
    }

    @Override
    public Response<String> ltrim(final String key, final long start, final long end) {
        writeAsync(key, () -> shadowPipeline.ltrim(key, start, end));

        return DynoDualWriterPipeline.super.ltrim(key, start, end);
    }

    @Override
    public Response<Long> move(final String key, final int dbIndex) {
        writeAsync(key, () -> shadowPipeline.move(key, dbIndex));

        return DynoDualWriterPipeline.super.move(key, dbIndex);
    }

    @Override
    public Response<Long> persist(final String key) {
        writeAsync(key, () -> shadowPipeline.persist(key));

        return DynoDualWriterPipeline.super.persist(key);
    }

    /* not supported by RedisPipeline 2.7.3 */
    public Response<String> rename(final String oldkey, final String newkey) {
        writeAsync(newkey, () -> shadowPipeline.rename(oldkey, newkey));

        return DynoDualWriterPipeline.super.rename(oldkey, newkey);
    }

    /* not supported by RedisPipeline 2.7.3 */
    public Response<Long> renamenx(final String oldkey, final String newkey) {
        writeAsync(newkey, () -> shadowPipeline.renamenx(oldkey, newkey));

        return DynoDualWriterPipeline.super.renamenx(oldkey, newkey);
    }

    @Override
    public Response<String> rpop(final String key) {
        writeAsync(key, () -> shadowPipeline.rpop(key));

        return DynoDualWriterPipeline.super.rpop(key);
    }

    @Override
    public Response<Long> rpush(final String key, final String... string) {
        writeAsync(key, () -> shadowPipeline.rpush(key, string));

        return DynoDualWriterPipeline.super.rpush(key, string);
    }

    @Override
    public Response<Long> rpushx(final String key, final String... string) {
        writeAsync(key, () -> shadowPipeline.rpushx(key, string));

        return DynoDualWriterPipeline.super.rpushx(key, string);
    }

    @Override
    public Response<Long> sadd(final String key, final String... member) {
        writeAsync(key, () -> shadowPipeline.sadd(key, member));

        return DynoDualWriterPipeline.super.sadd(key, member);
    }

    @Override
    public Response<String> set(final String key, final String value) {
        writeAsync(key, () -> shadowPipeline.set(key, value));

        return DynoDualWriterPipeline.super.set(key, value);
    }

    @Override
    public Response<Boolean> setbit(final String key, final long offset, final boolean value) {
        writeAsync(key, () -> shadowPipeline.setbit(key, offset, value));

        return DynoDualWriterPipeline.super.setbit(key, offset, value);
    }

    @Override
    public Response<String> setex(final String key, final int seconds, final String value) {
        writeAsync(key, () -> shadowPipeline.setex(key, seconds, value));

        return DynoDualWriterPipeline.super.setex(key, seconds, value);
    }

    @Override
    public Response<Long> setnx(final String key, final String value) {
        writeAsync(key, () -> shadowPipeline.setnx(key, value));

        return DynoDualWriterPipeline.super.setnx(key, value);
    }

    @Override
    public Response<Long> setrange(final String key, final long offset, final String value) {
        writeAsync(key, () -> shadowPipeline.setrange(key, offset, value));

        return DynoDualWriterPipeline.super.setrange(key, offset, value);
    }

    @Override
    public Response<List<String>> sort(final String key) {
        writeAsync(key, () -> shadowPipeline.sort(key));

        return DynoDualWriterPipeline.super.sort(key);
    }

    @Override
    public Response<List<String>> sort(final String key, final SortingParams sortingParameters) {
        writeAsync(key, () -> shadowPipeline.sort(key, sortingParameters));

        return DynoDualWriterPipeline.super.sort(key, sortingParameters);
    }

    @Override
    public Response<String> spop(final String key) {
        writeAsync(key, () -> shadowPipeline.spop(key));

        return DynoDualWriterPipeline.super.spop(key);
    }

    @Override
    public Response<Set<String>> spop(final String key, final long count) {
        writeAsync(key, () -> shadowPipeline.spop(key, count));

        return DynoDualWriterPipeline.super.spop(key, count);
    }

    @Override
    public Response<Long> srem(final String key, final String... member) {
        writeAsync(key, () -> shadowPipeline.srem(key, member));

        return DynoDualWriterPipeline.super.srem(key, member);
    }

    /**
     * This method is not supported by the BinaryRedisPipeline interface.
     */
    public Response<ScanResult<String>> sscan(final String key, final int cursor) {
        throw new UnsupportedOperationException("'SSCAN' cannot be called in pipeline");
    }

    /**
     * This method is not supported by the BinaryRedisPipeline interface.
     */
    public Response<ScanResult<String>> sscan(final String key, final String cursor) {
        throw new UnsupportedOperationException("'SSCAN' cannot be called in pipeline");
    }

    @Override
    public Response<Long> zadd(final String key, final double score, final String member) {
        writeAsync(key, () -> shadowPipeline.zadd(key, score, member));

        return DynoDualWriterPipeline.super.zadd(key, score, member);
    }

    @Override
    public Response<Long> zadd(final String key, final Map<String, Double> scoreMembers) {
        writeAsync(key, () -> shadowPipeline.zadd(key, scoreMembers));

        return DynoDualWriterPipeline.super.zadd(key, scoreMembers);
    }

    @Override
    public Response<Double> zincrby(final String key, final double score, final String member) {
        writeAsync(key, () -> shadowPipeline.zincrby(key, score, member));

        return DynoDualWriterPipeline.super.zincrby(key, score, member);
    }

    @Override
    public Response<Long> zrem(final String key, final String... member) {
        writeAsync(key, () -> shadowPipeline.zrem(key, member));

        return DynoDualWriterPipeline.super.zrem(key, member);
    }

    @Override
    public Response<Long> zremrangeByRank(final String key, final long start, final long end) {
        writeAsync(key, () -> shadowPipeline.zremrangeByRank(key, start, end));

        return DynoDualWriterPipeline.super.zremrangeByRank(key, start, end);
    }

    @Override
    public Response<Long> zremrangeByScore(final String key, final double start, final double end) {
        writeAsync(key, () -> shadowPipeline.zremrangeByScore(key, start, end));

        return DynoDualWriterPipeline.super.zremrangeByScore(key, start, end);
    }

    /**
     * This method is not supported by the BinaryRedisPipeline interface.
     */
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

    /**** Binary Operations ****/
    @Override
    public Response<String> set(final byte[] key, final byte[] value) {
        writeAsync(key, () -> shadowPipeline.set(key, value));

        return DynoDualWriterPipeline.super.set(key, value);
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
    public Response<List<GeoRadiusResponse>> georadius(String arg0, double arg1, double arg2, double arg3, GeoUnit arg4,
                                                       GeoRadiusParam arg5) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusByMember(String arg0, String arg1, double arg2, GeoUnit arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusByMember(String arg0, String arg1, double arg2, GeoUnit arg3,
                                                               GeoRadiusParam arg4) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> zadd(String arg0, Map<String, Double> arg1, ZAddParams arg2) {
        throw new UnsupportedOperationException("not yet implemented");

    }

    public Response<Long> zadd(final String key, final double score, final String member, final ZAddParams params) {
        writeAsync(key, () -> shadowPipeline.zadd(key, score, member, params));

        return DynoDualWriterPipeline.super.zadd(key, score, member, params);
    }


    @Override
    public Response<Double> zincrby(String arg0, double arg1, String arg2, ZIncrByParams arg3) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> append(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<byte[]>> blpop(byte[] arg) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<byte[]>> brpop(byte[] arg) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> decr(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> decrBy(final byte[] key, final long integer) {
        writeAsync(key, () -> shadowPipeline.decrBy(key, integer));

        return DynoDualWriterPipeline.super.decrBy(key, integer);
    }

    @Override
    public Response<Long> del(final byte[] key) {
        writeAsync(key, () -> shadowPipeline.del(key));

        return DynoDualWriterPipeline.super.del(key);
    }

    @Override
    public Response<byte[]> echo(byte[] string) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> expire(byte[] key, int seconds) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> pexpire(byte[] key, long milliseconds) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> expireAt(byte[] key, long unixTime) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> pexpireAt(byte[] key, long millisecondsTimestamp) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Boolean> getbit(byte[] key, long offset) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<byte[]> getSet(final byte[] key, final byte[] value) {
        writeAsync(key, () -> shadowPipeline.getSet(key, value));

        return DynoDualWriterPipeline.super.getSet(key, value);
    }

    @Override
    public Response<byte[]> getrange(byte[] key, long startOffset, long endOffset) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> hdel(byte[] key, byte[]... field) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Boolean> hexists(byte[] key, byte[] field) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> hincrBy(byte[] key, byte[] field, long value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<byte[]>> hkeys(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> hlen(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> hsetnx(byte[] key, byte[] field, byte[] value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<byte[]>> hvals(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> incr(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> incrBy(byte[] key, long integer) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<byte[]> lindex(byte[] key, long index) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> linsert(byte[] key, ListPosition where, byte[] pivot, byte[] value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> llen(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<byte[]> lpop(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> lpush(byte[] key, byte[]... string) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> lpushx(byte[] key, byte[]... bytes) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<byte[]>> lrange(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> lrem(byte[] key, long count, byte[] value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<String> lset(byte[] key, long index, byte[] value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<String> ltrim(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> move(byte[] key, int dbIndex) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> persist(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<byte[]> rpop(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> rpush(byte[] key, byte[]... string) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> rpushx(byte[] key, byte[]... string) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> sadd(byte[] key, byte[]... member) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> scard(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Boolean> setbit(byte[] key, long offset, byte[] value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> setrange(byte[] key, long offset, byte[] value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<String> setex(final byte[] key, final int seconds, final byte[] value) {
        writeAsync(key, () -> shadowPipeline.setex(key, seconds, value));

        return DynoDualWriterPipeline.super.setex(key, seconds, value);
    }

    @Override
    public Response<Long> setnx(byte[] key, byte[] value) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<byte[]>> smembers(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Boolean> sismember(byte[] key, byte[] member) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<byte[]>> sort(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<byte[]>> sort(byte[] key, SortingParams sortingParameters) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<byte[]> spop(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<byte[]>> spop(byte[] key, long count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<byte[]> srandmember(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> srem(byte[] key, byte[]... member) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> strlen(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<String> substr(byte[] key, int start, int end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> ttl(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<String> type(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> zadd(byte[] key, double score, byte[] member) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> zadd(byte[] key, double score, byte[] member, ZAddParams params) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> zadd(byte[] key, Map<byte[], Double> scoreMembers) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> zadd(byte[] key, Map<byte[], Double> scoreMembers, ZAddParams params) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> zcard(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> zcount(byte[] key, double min, double max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Double> zincrby(byte[] key, double score, byte[] member) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Double> zincrby(byte[] key, double score, byte[] member, ZIncrByParams params) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<byte[]>> zrange(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(byte[] key, double min, double max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(byte[] key, byte[] min, byte[] max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<byte[]>> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, double min, double max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(byte[] key, double max, double min) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<Tuple>> zrangeWithScores(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> zrank(byte[] key, byte[] member) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> zrem(byte[] key, byte[]... member) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> zremrangeByRank(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> zremrangeByScore(byte[] key, double start, double end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> zremrangeByScore(byte[] key, byte[] start, byte[] end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<byte[]>> zrevrange(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<Tuple>> zrevrangeWithScores(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> zrevrank(byte[] key, byte[] member) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Double> zscore(byte[] key, byte[] member) {
        throw new UnsupportedOperationException("not yet implemented");
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
    public Response<Set<byte[]>> zrevrangeByLex(byte[] key, byte[] max, byte[] min) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Set<byte[]>> zrevrangeByLex(byte[] key, byte[] max, byte[] min, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> zremrangeByLex(byte[] key, byte[] min, byte[] max) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> bitcount(byte[] key) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> bitcount(byte[] key, long start, long end) {
        throw new UnsupportedOperationException("not yet implemented");
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
    public Response<Long> geoadd(byte[] key, double longitude, double latitude, byte[] member) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> geoadd(byte[] key, Map<byte[], GeoCoordinate> memberCoordinateMap) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Double> geodist(byte[] key, byte[] member1, byte[] member2) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Double> geodist(byte[] key, byte[] member1, byte[] member2, GeoUnit unit) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<byte[]>> geohash(byte[] key, byte[]... members) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoCoordinate>> geopos(byte[] key, byte[]... members) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadius(byte[] key, double longitude, double latitude, double radius,
                                                       GeoUnit unit) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadius(byte[] key, double longitude, double latitude, double radius,
                                                       GeoUnit unit, GeoRadiusParam param) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusByMember(byte[] key, byte[] member, double radius, GeoUnit unit) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<GeoRadiusResponse>> georadiusByMember(byte[] key, byte[] member, double radius, GeoUnit unit,
                                                               GeoRadiusParam param) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<List<Long>> bitfield(byte[] key, byte[]... elements) {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
