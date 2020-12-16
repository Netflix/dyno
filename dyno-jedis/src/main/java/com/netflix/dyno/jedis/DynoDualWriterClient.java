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

import com.netflix.dyno.connectionpool.CompressionOperation;
import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.MultiKeyCompressionOperation;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl;
import com.netflix.dyno.contrib.DynoOPMonitor;
import com.netflix.dyno.jedis.operation.BaseKeyOperation;
import com.netflix.dyno.jedis.operation.MultiKeyOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Client that provides 'dual-write' functionality. This is useful when clients wish to move from one dynomite
 * cluster to another, for example to upgrade cluster capacity.
 *
 * @author jcacciatore
 */
public class DynoDualWriterClient extends DynoJedisClient {


    // FIXME: not all write commands are shadowed


    private static final Logger logger = LoggerFactory.getLogger(DynoDualWriterClient.class);

    private static ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    private final String appName;
    private final ConnectionPool<Jedis> connPool;

    // Client used for dual-write functionality.
    private final DynoJedisClient shadowClient;

    // Used to control traffic flow to the dual-write cluster
    private final Dial dial;

    private final AtomicReference<DynoJedisPipelineMonitor> pipelineMonitor = new AtomicReference<>();

    public DynoDualWriterClient(String name, String clusterName,
                                ConnectionPool<Jedis> pool,
                                DynoOPMonitor operationMonitor,
                                ConnectionPoolMonitor connectionPoolMonitor,
                                DynoJedisClient shadowClient) {

        this(name, clusterName, pool, operationMonitor, connectionPoolMonitor, shadowClient,
                new TimestampDial(pool.getConfiguration().getDualWritePercentage()));
    }

    public DynoDualWriterClient(String name, String clusterName,
                                ConnectionPool<Jedis> pool,
                                DynoOPMonitor operationMonitor,
                                ConnectionPoolMonitor connectionPoolMonitor,
                                DynoJedisClient shadowClient,
                                Dial dial) {
        super(name, clusterName, pool, operationMonitor, connectionPoolMonitor);
        this.appName = name;
        this.connPool = pool;
        this.shadowClient = shadowClient;
        this.dial = dial;
    }

    public Dial getDial() {
        return dial;
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

    @Override
    public DynoDualWriterPipeline pipelined() {
        return new DynoDualWriterPipeline(appName, getConnPool(), checkAndInitPipelineMonitor(), getConnPool().getMonitor(),
                shadowClient.getConnPool(), dial);
    }

    private <R> Future<R> writeAsync(final String key, Callable<R> func) {
        if (sendShadowRequest(key)) {
            try {
                return executor.submit(func);
            } catch (Throwable th) {
                opMonitor.recordFailure("shadowPool_submit", th.getMessage());
            }

            // if we need to do any other processing (logging, etc) now's the time...

        }

        return null;
    }

    /**
     *  writeAsync() for binary commands
     */
    private <R> Future<R> writeAsync(final byte[] key, Callable<R> func) {
        if (sendShadowRequest(key)) {
            try {
                return executor.submit(func);
            } catch (Throwable th) {
                opMonitor.recordFailure("shadowPool_submit", th.getMessage());
            }

            // if we need to do any other processing (logging, etc) now's the time...

        }

        return null;
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
    private boolean sendShadowRequest(String key) {
        return this.getConnPool().getConfiguration().isDualWriteEnabled() &&
                !this.getConnPool().isIdle() &&
                this.getConnPool().getActivePools().size() > 0 &&
                dial.isInRange(key);
    }

    private boolean sendShadowRequest(byte[] key) {
        return this.getConnPool().getConfiguration().isDualWriteEnabled() &&
                !this.getConnPool().isIdle() &&
                this.getConnPool().getActivePools().size() > 0 &&
                dial.isInRange(key);
    }

    public interface Dial {
        /**
         * Returns true if the given value is in range, false otherwise
         */
        boolean isInRange(String key);

        boolean isInRange(byte[] key);

        void setRange(int range);
    }

    /**
     * Default Dial implementation that presumes no knowledge of the key value
     * and simply uses a timestamp to determine inclusion/exclusion
     */
    private static class TimestampDial implements Dial {

        private final AtomicInteger range = new AtomicInteger(1);

        public TimestampDial(int range) {
            this.range.set(range);
        }

        @Override
        public boolean isInRange(String key) {
            return range.get() > (System.currentTimeMillis() % 100);
        }

        @Override
        public boolean isInRange(byte[] key) {
            return range.get() > (System.currentTimeMillis() % 100);
        }

        @Override
        public void setRange(int range) {
            this.range.set(range);
        }
    }


    //----------------------------- JEDIS COMMANDS --------------------------------------


    @Override
    public Long append(final String key, final String value) {
        writeAsync(key, () -> shadowClient.append(key, value));

        return DynoDualWriterClient.super.append(key, value);
    }

    @Override
    public String hmset(final String key, final Map<String, String> hash) {
        writeAsync(key, () -> shadowClient.hmset(key, hash));

        return DynoDualWriterClient.super.hmset(key, hash);
    }

    @Override
    public Long sadd(final String key, final String... members) {
        writeAsync(key, () -> shadowClient.sadd(key, members));

        return DynoDualWriterClient.super.sadd(key, members);

    }

    @Override
    public Long hset(final String key, final String field, final String value) {
        writeAsync(key, () -> shadowClient.hset(key, field, value));

        return DynoDualWriterClient.super.hset(key, field, value);
    }

    @Override
    public String set(final String key, final String value) {
        writeAsync(key, () -> shadowClient.set(key, value));

        return DynoDualWriterClient.super.set(key, value);
    }

    @Override
    public String setex(final String key, int seconds, String value) {
        writeAsync(key, () -> shadowClient.setex(key, seconds, value));

        return DynoDualWriterClient.super.setex(key, seconds, value);
    }

    @Override
    public Long del(final String key) {
        writeAsync(key, () -> shadowClient.del(key));

        return DynoDualWriterClient.super.del(key);
    }

    @Override
    public Long expire(final String key, final int seconds) {
        writeAsync(key, () -> shadowClient.expire(key, seconds));

        return DynoDualWriterClient.super.expire(key, seconds);
    }

    @Override
    public Long expireAt(final String key, final long unixTime) {
        writeAsync(key, () -> shadowClient.expireAt(key, unixTime));

        return DynoDualWriterClient.super.expireAt(key, unixTime);
    }

    @Override
    public String getSet(final String key, final String value) {
        writeAsync(key, () -> shadowClient.getSet(key, value));

        return DynoDualWriterClient.super.getSet(key, value);
    }

    @Override
    public Long hdel(final String key, final String... fields) {
        writeAsync(key, () -> shadowClient.hdel(key, fields));

        return DynoDualWriterClient.super.hdel(key);
    }

    @Override
    public Long hincrBy(final String key, final String field, final long value) {
        writeAsync(key, () -> shadowClient.hincrBy(key, field, value));

        return DynoDualWriterClient.super.hincrBy(key, field, value);
    }

    @Override
    public Double hincrByFloat(final String key, final String field, final double value) {
        writeAsync(key, () -> shadowClient.hincrByFloat(key, field, value));

        return DynoDualWriterClient.super.hincrByFloat(key, field, value);
    }

    @Override
    public Long hsetnx(final String key, final String field, final String value) {
        writeAsync(key, () -> shadowClient.hsetnx(key, field, value));

        return DynoDualWriterClient.super.hsetnx(key, field, value);
    }

    @Override
    public Long incr(final String key) {
        writeAsync(key, () -> shadowClient.incr(key));

        return DynoDualWriterClient.super.incr(key);
    }

    @Override
    public Long incrBy(final String key, final long delta) {
        writeAsync(key, () -> shadowClient.incrBy(key, delta));

        return DynoDualWriterClient.super.incrBy(key, delta);
    }

    @Override
    public Double incrByFloat(final String key, final double increment) {
        writeAsync(key, () -> shadowClient.incrByFloat(key, increment));

        return DynoDualWriterClient.super.incrByFloat(key, increment);
    }

    @Override
    public String lpop(final String key) {
        writeAsync(key, () -> shadowClient.lpop(key));

        return DynoDualWriterClient.super.lpop(key);
    }

    @Override
    public Long lpush(final String key, final String... values) {
        writeAsync(key, () -> shadowClient.lpush(key, values));

        return DynoDualWriterClient.super.lpush(key, values);
    }

    @Override
    public Long lrem(final String key, final long count, final String value) {
        writeAsync(key, () -> shadowClient.lrem(key, count, value));

        return DynoDualWriterClient.super.lrem(key, count, value);
    }

    @Override
    public String lset(final String key, final long count, final String value) {
        writeAsync(key, () -> shadowClient.lset(key, count, value));

        return DynoDualWriterClient.super.lset(key, count, value);
    }

    @Override
    public String ltrim(final String key, final long start, final long end) {
        writeAsync(key, () -> shadowClient.ltrim(key, start, end));

        return DynoDualWriterClient.super.ltrim(key, start, end);
    }

    @Override
    public Long persist(final String key) {
        writeAsync(key, () -> shadowClient.persist(key));

        return DynoDualWriterClient.super.persist(key);
    }

    @Override
    public Long pexpireAt(final String key, final long millisecondsTimestamp) {
        writeAsync(key, () -> shadowClient.pexpireAt(key, millisecondsTimestamp));

        return DynoDualWriterClient.super.pexpireAt(key, millisecondsTimestamp);
    }

    @Override
    public Long pttl(final String key) {
        writeAsync(key, () -> shadowClient.pttl(key));

        return DynoDualWriterClient.super.pttl(key);
    }

    @Override
    public String rename(final String oldkey, final String newkey) {
        writeAsync(oldkey, () -> shadowClient.rename(oldkey, oldkey));

        return DynoDualWriterClient.super.rename(oldkey, oldkey);
    }

    @Override
    public String rpop(final String key) {
        writeAsync(key, () -> shadowClient.rpop(key));

        return DynoDualWriterClient.super.rpop(key);
    }

    @Override
    public Long scard(final String key) {
        writeAsync(key, () -> shadowClient.scard(key));

        return DynoDualWriterClient.super.scard(key);
    }

    @Override
    public Boolean setbit(final String key, final long offset, final boolean value) {
        writeAsync(key, () -> shadowClient.setbit(key, offset, value));

        return DynoDualWriterClient.super.setbit(key, offset, value);
    }

    @Override
    public Boolean setbit(final String key, final long offset, final String value) {
        writeAsync(key, () -> shadowClient.setbit(key, offset, value));

        return DynoDualWriterClient.super.setbit(key, offset, value);
    }

    @Override
    public Long setnx(final String key, final String value) {
        writeAsync(key, () -> shadowClient.setnx(key, value));

        return DynoDualWriterClient.super.setnx(key, value);
    }

    @Override
    public Long setrange(final String key, final long offset, final String value) {
        writeAsync(key, () -> shadowClient.setrange(key, offset, value));

        return DynoDualWriterClient.super.setrange(key, offset, value);
    }

    @Override
    public Set<String> smembers(final String key) {
        writeAsync(key, () -> shadowClient.smembers(key));

        return DynoDualWriterClient.super.smembers(key);
    }

    @Override
    public Long smove(final String srckey, final String dstkey, final String member) {
        writeAsync(srckey, () -> shadowClient.smove(srckey, dstkey, member));

        return DynoDualWriterClient.super.smove(srckey, dstkey, member);
    }

    @Override
    public List<String> sort(final String key) {
        writeAsync(key, () -> shadowClient.sort(key));

        return DynoDualWriterClient.super.sort(key);
    }

    @Override
    public String spop(final String key) {
        writeAsync(key, () -> shadowClient.spop(key));

        return DynoDualWriterClient.super.spop(key);
    }

    @Override
    public Long srem(final String key, final String... members) {
        writeAsync(key, () -> shadowClient.srem(key, members));

        return DynoDualWriterClient.super.srem(key, members);
    }

    @Override
    public ScanResult<String> sscan(final String key, final String cursor) {
        writeAsync(key, () -> shadowClient.sscan(key, cursor));

        return DynoDualWriterClient.super.sscan(key, cursor);
    }

    @Override
    public ScanResult<String> sscan(final String key, final String cursor, final ScanParams params) {
        writeAsync(key, () -> shadowClient.sscan(key, cursor, params));

        return DynoDualWriterClient.super.sscan(key, cursor, params);
    }

    @Override
    public Long ttl(final String key) {
        writeAsync(key, () -> shadowClient.ttl(key));

        return DynoDualWriterClient.super.ttl(key);
    }

    @Override
    public Long zadd(final String key, final double score, final String member) {
        writeAsync(key, () -> shadowClient.zadd(key, score, member));

        return DynoDualWriterClient.super.zadd(key, score, member);
    }

    @Override
    public Long zadd(final String key, final Map<String, Double> scoreMembers) {
        writeAsync(key, () -> shadowClient.zadd(key, scoreMembers));

        return DynoDualWriterClient.super.zadd(key, scoreMembers);
    }

    @Override
    public Double zincrby(final String key, final double score, final String member) {
        writeAsync(key, () -> shadowClient.zincrby(key, score, member));

        return DynoDualWriterClient.super.zincrby(key, score, member);
    }

    @Override
    public Long zrem(final String key, final String... member) {
        writeAsync(key, () -> shadowClient.zrem(key, member));

        return DynoDualWriterClient.super.zrem(key, member);
    }

    @Override
    public List<String> blpop(final int timeout, final String key) {
        writeAsync(key, () -> shadowClient.blpop(timeout, key));

        return DynoDualWriterClient.super.blpop(timeout, key);
    }

    @Override
    public List<String> brpop(final int timeout, final String key) {
        writeAsync(key, () -> shadowClient.brpop(timeout, key));

        return DynoDualWriterClient.super.brpop(timeout, key);
    }


    /******************* Jedis Dual write for binary commands **************/


    @Override
    public Long append(final byte[] key, final byte[] value) {
        writeAsync(key, () -> shadowClient.append(key, value));

        return DynoDualWriterClient.super.append(key, value);
    }

    @Override
    public String hmset(final byte[] key, final Map<byte[], byte[]> hash) {
        writeAsync(key, () -> shadowClient.hmset(key, hash));

        return DynoDualWriterClient.super.hmset(key, hash);
    }

    @Override
    public Long sadd(final byte[] key, final byte[]... members) {
        writeAsync(key, () -> shadowClient.sadd(key, members));

        return DynoDualWriterClient.super.sadd(key, members);

    }

    @Override
    public Long hset(final byte[] key, final byte[] field, final byte[] value) {
        writeAsync(key, () -> shadowClient.hset(key, field, value));

        return DynoDualWriterClient.super.hset(key, field, value);
    }

    @Override
    public String set(final byte[] key, final byte[] value) {
        writeAsync(key, () -> shadowClient.set(key, value));

        return DynoDualWriterClient.super.set(key, value);
    }

    @Override
    public String setex(final byte[] key, int seconds, byte[] value) {
        writeAsync(key, () -> shadowClient.setex(key, seconds, value));

        return DynoDualWriterClient.super.setex(key, seconds, value);
    }

    @Override
    public Long del(final byte[] key) {
        writeAsync(key, () -> shadowClient.del(key));

        return DynoDualWriterClient.super.del(key);
    }

    @Override
    public Long expire(final byte[] key, final int seconds) {
        writeAsync(key, () -> shadowClient.expire(key, seconds));

        return DynoDualWriterClient.super.expire(key, seconds);
    }

    @Override
    public Long expireAt(final byte[] key, final long unixTime) {
        writeAsync(key, () -> shadowClient.expireAt(key, unixTime));

        return DynoDualWriterClient.super.expireAt(key, unixTime);
    }

    @Override
    public byte[] getSet(final byte[] key, final byte[] value) {
        writeAsync(key, () -> shadowClient.getSet(key, value));

        return DynoDualWriterClient.super.getSet(key, value);
    }

    @Override
    public Long hdel(final byte[] key, final byte[]... fields) {
        writeAsync(key, () -> shadowClient.hdel(key, fields));

        return DynoDualWriterClient.super.hdel(key);
    }

    @Override
    public Long hincrBy(final byte[] key, final byte[] field, final long value) {
        writeAsync(key, () -> shadowClient.hincrBy(key, field, value));

        return DynoDualWriterClient.super.hincrBy(key, field, value);
    }

    @Override
    public Double hincrByFloat(final byte[] key, final byte[] field, final double value) {
        writeAsync(key, () -> shadowClient.hincrByFloat(key, field, value));

        return DynoDualWriterClient.super.hincrByFloat(key, field, value);
    }

    @Override
    public Long hsetnx(final byte[] key, final byte[] field, final byte[] value) {
        writeAsync(key, () -> shadowClient.hsetnx(key, field, value));

        return DynoDualWriterClient.super.hsetnx(key, field, value);
    }

    @Override
    public Long incr(final byte[] key) {
        writeAsync(key, () -> shadowClient.incr(key));

        return DynoDualWriterClient.super.incr(key);
    }

    @Override
    public Long incrBy(final byte[] key, final long delta) {
        writeAsync(key, () -> shadowClient.incrBy(key, delta));

        return DynoDualWriterClient.super.incrBy(key, delta);
    }

    @Override
    public Double incrByFloat(final byte[] key, final double increment) {
        writeAsync(key, () -> shadowClient.incrByFloat(key, increment));

        return DynoDualWriterClient.super.incrByFloat(key, increment);
    }

    @Override
    public byte[] lpop(final byte[] key) {
        writeAsync(key, () -> shadowClient.lpop(key));

        return DynoDualWriterClient.super.lpop(key);
    }

    @Override
    public Long lpush(final byte[] key, final byte[]... values) {
        writeAsync(key, () -> shadowClient.lpush(key, values));

        return DynoDualWriterClient.super.lpush(key, values);
    }

    @Override
    public Long lrem(final byte[] key, final long count, final byte[] value) {
        writeAsync(key, () -> shadowClient.lrem(key, count, value));

        return DynoDualWriterClient.super.lrem(key, count, value);
    }

    @Override
    public String lset(final byte[] key, final long count, final byte[] value) {
        writeAsync(key, () -> shadowClient.lset(key, count, value));

        return DynoDualWriterClient.super.lset(key, count, value);
    }

    @Override
    public String ltrim(final byte[] key, final long start, final long end) {
        writeAsync(key, () -> shadowClient.ltrim(key, start, end));

        return DynoDualWriterClient.super.ltrim(key, start, end);
    }

    @Override
    public Long persist(final byte[] key) {
        writeAsync(key, () -> shadowClient.persist(key));

        return DynoDualWriterClient.super.persist(key);
    }

    @Override
    public Long pexpireAt(final byte[] key, final long millisecondsTimestamp) {
        writeAsync(key, () -> shadowClient.pexpireAt(key, millisecondsTimestamp));

        return DynoDualWriterClient.super.pexpireAt(key, millisecondsTimestamp);
    }

    @Override
    public Long pttl(final byte[] key) {
        writeAsync(key, () -> shadowClient.pttl(key));

        return DynoDualWriterClient.super.pttl(key);
    }

    @Override
    public String rename(final byte[] oldkey, final byte[] newkey) {
        writeAsync(oldkey, () -> shadowClient.rename(oldkey, oldkey));

        return DynoDualWriterClient.super.rename(oldkey, oldkey);
    }

    @Override
    public byte[] rpop(final byte[] key) {
        writeAsync(key, () -> shadowClient.rpop(key));

        return DynoDualWriterClient.super.rpop(key);
    }

    @Override
    public Long scard(final byte[] key) {
        writeAsync(key, () -> shadowClient.scard(key));

        return DynoDualWriterClient.super.scard(key);
    }

    @Override
    public Boolean setbit(final byte[] key, final long offset, final boolean value) {
        writeAsync(key, () -> shadowClient.setbit(key, offset, value));

        return DynoDualWriterClient.super.setbit(key, offset, value);
    }

    @Override
    public Boolean setbit(final byte[] key, final long offset, final byte[] value) {
        writeAsync(key, () -> shadowClient.setbit(key, offset, value));

        return DynoDualWriterClient.super.setbit(key, offset, value);
    }

    @Override
    public Long setnx(final byte[] key, final byte[] value) {
        writeAsync(key, () -> shadowClient.setnx(key, value));

        return DynoDualWriterClient.super.setnx(key, value);
    }

    @Override
    public Long setrange(final byte[] key, final long offset, final byte[] value) {
        writeAsync(key, () -> shadowClient.setrange(key, offset, value));

        return DynoDualWriterClient.super.setrange(key, offset, value);
    }

    @Override
    public Set<byte[]> smembers(final byte[] key) {
        writeAsync(key, () -> shadowClient.smembers(key));

        return DynoDualWriterClient.super.smembers(key);
    }

    @Override
    public Long smove(final byte[] srckey, final byte[] dstkey, final byte[] member) {
        writeAsync(srckey, () -> shadowClient.smove(srckey, dstkey, member));

        return DynoDualWriterClient.super.smove(srckey, dstkey, member);
    }

    @Override
    public List<byte[]> sort(final byte[] key) {
        writeAsync(key, () -> shadowClient.sort(key));

        return DynoDualWriterClient.super.sort(key);
    }

    @Override
    public byte[] spop(final byte[] key) {
        writeAsync(key, () -> shadowClient.spop(key));

        return DynoDualWriterClient.super.spop(key);
    }

    @Override
    public Long srem(final byte[] key, final byte[]... members) {
        writeAsync(key, () -> shadowClient.srem(key, members));

        return DynoDualWriterClient.super.srem(key, members);
    }

    @Override
    public ScanResult<byte[]> sscan(final byte[] key, final byte[] cursor) {
        writeAsync(key, () -> shadowClient.sscan(key, cursor));

        return DynoDualWriterClient.super.sscan(key, cursor);
    }

    @Override
    public ScanResult<byte[]> sscan(final byte[] key, final byte[] cursor, final ScanParams params) {
        writeAsync(key, () -> shadowClient.sscan(key, cursor, params));

        return DynoDualWriterClient.super.sscan(key, cursor, params);
    }

    @Override
    public Long ttl(final byte[] key) {
        writeAsync(key, () -> shadowClient.ttl(key));

        return DynoDualWriterClient.super.ttl(key);
    }

    @Override
    public Long zadd(final byte[] key, final double score, final byte[] member) {
        writeAsync(key, () -> shadowClient.zadd(key, score, member));

        return DynoDualWriterClient.super.zadd(key, score, member);
    }

    @Override
    public Long zadd(final byte[] key, final Map<byte[], Double> scoreMembers) {
        writeAsync(key, () -> shadowClient.zadd(key, scoreMembers));

        return DynoDualWriterClient.super.zadd(key, scoreMembers);
    }

    @Override
    public Double zincrby(final byte[] key, final double score, final byte[] member) {
        writeAsync(key, () -> shadowClient.zincrby(key, score, member));

        return DynoDualWriterClient.super.zincrby(key, score, member);
    }

    @Override
    public Long zrem(final byte[] key, final byte[]... member) {
        writeAsync(key, () -> shadowClient.zrem(key, member));

        return DynoDualWriterClient.super.zrem(key, member);
    }

}
