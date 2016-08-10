/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.jedis;

import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.contrib.DynoOPMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Client that provides 'dual-write' functionality. This is useful when clients wish to move from one dynomite
 * cluster to another, for example to upgrade cluster capacity.
 *
 * @author jcacciatore
 */
public class DynoDualWriterClient extends DynoJedisClient {

    private static final Logger logger = LoggerFactory.getLogger(DynoDualWriterClient.class);

    private static ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());



    private final DynoJedisClient targetClient;
    private final Dial dial;

    public DynoDualWriterClient(String name, String clusterName,
                                ConnectionPool<Jedis> pool,
                                DynoOPMonitor operationMonitor,
                                DynoJedisClient targetClient) {

        this(name, clusterName, pool, operationMonitor, targetClient,
                new TimestampDial(pool.getConfiguration().getDualWritePercentage()));
    }

    public DynoDualWriterClient(String name, String clusterName,
                                ConnectionPool<Jedis> pool,
                                DynoOPMonitor operationMonitor,
                                DynoJedisClient targetClient,
                                Dial dial) {
        super(name, clusterName, pool, operationMonitor);
        this.targetClient = targetClient;
        this.dial = dial;
    }

    public Dial getDial() {
        return dial;
    }

    private <R> Future<OperationResult<R>> writeAsync(final String key, Callable<OperationResult<R>> func) {
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
    private <R> Future<OperationResult<R>> writeAsync(final byte[] key, Callable<OperationResult<R>> func) {
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
        return  this.getConnPool().getConfiguration().isDualWriteEnabled() &&
                !this.getConnPool().isIdle() &&
                this.getConnPool().getActivePools().size() > 0 &&
                dial.isInRange(key);
    }
    
    private boolean sendShadowRequest(byte[] key) {
        return  this.getConnPool().getConfiguration().isDualWriteEnabled() &&
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
            return range.get() >  (System.currentTimeMillis() % 100);
        }
        
        @Override
        public boolean isInRange(byte[] key) {
            return range.get() >  (System.currentTimeMillis() % 100);
        }

        @Override
        public void setRange(int range) {
            this.range.set(range);
        }
    }   
    

    //----------------------------- JEDIS COMMANDS --------------------------------------

    @Override
    public Long append(final String key, final String value) {
        writeAsync(key, new Callable<OperationResult<Long>>() {
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_append(key, value);
            }
        });

        return targetClient.append(key, value);
    }

    @Override
    public String hmset(final String key, final Map<String, String> hash) {
        writeAsync(key, new Callable<OperationResult<String>>(){
            @Override
            public OperationResult<String> call() throws Exception {
                return d_hmset(key, hash);
            }
        });

        return targetClient.hmset(key, hash);
    }

    @Override
    public Long sadd(final String key, final String... members) {
        writeAsync(key, new Callable<OperationResult<Long>>() {
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_sadd(key, members);
            }
        });

        return targetClient.sadd(key, members);
    }

    @Override
    public Long hset(final String key, final String field, final String value) {
        writeAsync(key, new Callable<OperationResult<Long>>() {
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_hset(key, field, value);
            }
        });

        return targetClient.hset(key, field, value);
    }

    @Override
    public String set(final String key, final String value) {
        writeAsync(key, new Callable<OperationResult<String>>() {
            @Override
            public OperationResult<String> call() throws Exception {
                return d_set(key, value);
            }
        });

        return targetClient.set(key, value);
    }

    @Override
    public String setex(final String key, int seconds, String value) {
        writeAsync(key, new Callable<OperationResult<String>>(){
            @Override
            public OperationResult<String> call() throws Exception {
                return d_get(key);
            }
        });

        return targetClient.setex(key, seconds, value);
    }

    @Override
    public Long del(final String key) {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_del(key);
            }
        });

        return targetClient.del(key);
    }
    
    @Override
    public Boolean exists(final String key) {
        writeAsync(key, new Callable<OperationResult<Boolean>>(){
            @Override
            public OperationResult<Boolean> call() throws Exception {
                return d_exists(key);
            }
        });

        return targetClient.exists(key);
    }
    
    @Override
    public Long expire(final String key, final int seconds) {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_expire(key, seconds);
            }
        });

        return targetClient.expire(key, seconds);
    }
    
    @Override
    public Long expireAt(final String key, final long unixTime) {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_expireAt(key, unixTime);
            }
        });

        return targetClient.expireAt(key, unixTime);
    }
    
    @Override
    public String getSet(final String key, final String value) {
        writeAsync(key, new Callable<OperationResult<String>>(){
            @Override
            public OperationResult<String> call() throws Exception {
                return d_getSet(key, value);
            }
        });

        return targetClient.getSet(key, value);
    }
    
    @Override
    public Long hdel(final String key, final String... fields) {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_hdel(key, fields);
            }
        });

        return targetClient.hdel(key);
    }
    
    @Override
    public  Long hincrBy(final String key, final String field, final long value) {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_hincrBy(key, field, value);
            }
        });

        return targetClient.hincrBy(key, field, value);
    }
    
    @Override
    public  Double hincrByFloat(final String key, final String field, final double value)  {
        writeAsync(key, new Callable<OperationResult<Double>>(){
            @Override
            public OperationResult<Double> call() throws Exception {
                return d_hincrByFloat(key, field, value);
            }
        });

        return targetClient.hincrByFloat(key, field, value);
    }
    
    @Override
    public  Long hsetnx(final String key, final String field, final String value) {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_hsetnx(key, field, value);
            }
        });

        return targetClient.hsetnx(key, field, value);
    }
    
    @Override
    public Long incr(final String key) {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_incr(key);
            }
        });

        return targetClient.incr(key);
    }
    
    @Override
    public Long incrBy(final String key, final long delta) {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_incrBy(key, delta);
            }
        });

        return targetClient.incrBy(key, delta);
    }
    
    @Override
    public Double incrByFloat(final String key, final double increment) {
        writeAsync(key, new Callable<OperationResult<Double>>(){
            @Override
            public OperationResult<Double> call() throws Exception {
                return d_incrByFloat(key, increment);
            }
        });

        return targetClient.incrByFloat(key, increment);
    }
    
    @Override
    public String lpop(final String key) {
        writeAsync(key, new Callable<OperationResult<String>>(){
            @Override
            public OperationResult<String> call() throws Exception {
                return d_lpop(key);
            }
        });

        return targetClient.lpop(key);
    }
    
    @Override
    public Long lpush(final String key, final String... values) {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_lpush(key, values);
            }
        });

        return targetClient.lpush(key, values);
    }
     
    @Override
    public Long lrem(final String key, final long count, final String value) {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_lrem(key, count, value);
            }
        });

        return targetClient.lrem(key, count, value);
    }
    
    @Override
    public String lset(final String key, final long count, final String value) {
        writeAsync(key, new Callable<OperationResult<String>>(){
            @Override
            public OperationResult<String> call() throws Exception {
                return d_lset(key, count, value);
            }
        });

        return targetClient.lset(key, count, value);
    }
    
    @Override
    public String ltrim(final String key, final long start, final long end) {
        writeAsync(key, new Callable<OperationResult<String>>(){
            @Override
            public OperationResult<String> call() throws Exception {
                return d_ltrim(key, start, end);
            }
        });

        return targetClient.ltrim(key, start, end);
    }
    
    @Override
    public Long persist(final String key) {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_persist(key);
            }
        });

        return targetClient.persist(key);
    }
    
    @Override
    public Long pexpireAt(final String key, final long millisecondsTimestamp) {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_pexpireAt(key, millisecondsTimestamp);
            }
        });

        return targetClient.pexpireAt(key, millisecondsTimestamp);
    }
    
    @Override
    public String psetex(final String key, final int milliseconds, final String value) {
        writeAsync(key, new Callable<OperationResult<String>>(){
            @Override
            public OperationResult<String> call() throws Exception {
                return d_psetex(key, milliseconds, value);
            }
        });

        return targetClient.psetex(key, milliseconds, value);
    }
    
    @Override
    public Long pttl(final String key) {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_pttl(key);
            }
        });

        return targetClient.pttl(key);
    }
    
    @Override
    public String rename(final String oldkey, final String newkey) {
        writeAsync(oldkey, new Callable<OperationResult<String>>(){
            @Override
            public OperationResult<String> call() throws Exception {
                return d_rename(oldkey, oldkey);
            }
        });

        return targetClient.rename(oldkey, oldkey);
    }
    
    @Override
    public String rpop(final String key) {
        writeAsync(key, new Callable<OperationResult<String>>(){
            @Override
            public OperationResult<String> call() throws Exception {
                return d_rpop(key);
            }
        });

        return targetClient.rpop(key);
    }
    
    @Override
    public Long scard(final String key) {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_scard(key);
            }
        });

        return targetClient.scard(key);
    }
    
    @Override
    public Boolean setbit(final String key, final long offset, final boolean value)  {
        writeAsync(key, new Callable<OperationResult<Boolean>>(){
            @Override
            public OperationResult<Boolean> call() throws Exception {
                return d_setbit(key, offset, value);
            }
        });

        return targetClient.setbit(key, offset, value);
    }
    
    @Override
    public Boolean setbit(final String key, final long offset, final String value) {
        writeAsync(key, new Callable<OperationResult<Boolean>>(){
            @Override
            public OperationResult<Boolean> call() throws Exception {
                return d_setbit(key, offset, value);
            }
        });

        return targetClient.setbit(key, offset, value);
    }
    
    @Override
    public Long setnx(final String key, final String value) {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_setnx(key, value);
            }
        });

        return targetClient.setnx(key, value);
    }
    
    @Override
    public Long setrange(final String key, final long offset, final String value) {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_setrange(key, offset, value);
            }
        });

        return targetClient.setrange(key, offset, value);
    }
    
    @Override
    public Set<String> smembers(final String key) {
        writeAsync(key, new Callable<OperationResult<Set<String>>>(){
            @Override
            public OperationResult<Set<String>> call() throws Exception {
                return d_smembers(key);
            }
        });

        return targetClient.smembers(key);
    }
    
    @Override
    public Long smove(final String srckey, final String dstkey, final String member)  {
        writeAsync(srckey, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_smove(srckey,dstkey,member);
            }
        });

        return targetClient.smove(srckey,dstkey,member);
    }
    
    @Override
    public List<String> sort(final String key) {
        writeAsync(key, new Callable<OperationResult<List<String>>>(){
            @Override
            public OperationResult<List<String>> call() throws Exception {
                return d_sort(key);
            }
        });

        return targetClient.sort(key);
    }    
    
    @Override
    public String spop(final String key) {
        writeAsync(key, new Callable<OperationResult<String>>(){
            @Override
            public OperationResult<String> call() throws Exception {
                return d_spop(key);
            }
        });

        return targetClient.spop(key);
    }
    
    @Override
    public Long srem(final String key, final String... members)  {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_srem(key,members);
            }
        });

        return targetClient.srem(key,members);
    }
    
    @Override
    public ScanResult<String> sscan(final String key, final String cursor)  {
        writeAsync(key, new Callable<OperationResult<ScanResult<String>>>(){
            @Override
            public OperationResult<ScanResult<String>> call() throws Exception {
                return d_sscan(key,cursor);
            }
        });

        return targetClient.sscan(key,cursor);
    }
    
    @Override
    public ScanResult<String> sscan(final String key, final String cursor, final ScanParams params)  {
        writeAsync(key, new Callable<OperationResult<ScanResult<String>>>(){
            @Override
            public OperationResult<ScanResult<String>> call() throws Exception {
                return d_sscan(key,cursor,params);
            }
        });

        return targetClient.sscan(key,cursor,params);
    }
       
    @Override
    public Long ttl(final String key) {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_ttl(key);
            }
        });

        return targetClient.ttl(key);
    }   
    
    @Override
    public Long zadd(final String key, final double score, final String member)  {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_zadd(key, score, member);
            }
        });

        return targetClient.zadd(key, score, member);
    }
    
    @Override
    public Long zadd(final String key, final Map<String, Double> scoreMembers)  {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_zadd(key, scoreMembers);
            }
        });

        return targetClient.zadd(key, scoreMembers);
    }
    
    @Override
    public Double zincrby(final String key, final double score, final String member)  {
        writeAsync(key, new Callable<OperationResult<Double>>(){
            @Override
            public OperationResult<Double> call() throws Exception {
                return d_zincrby(key, score, member);
            }
        });

        return targetClient.zincrby(key, score, member);
    }
    
    @Override
    public Long zrem(final String key, final String... member)  {
        writeAsync(key, new Callable<OperationResult<Long>>(){
            @Override
            public OperationResult<Long> call() throws Exception {
                return d_zrem(key, member);
            }
        });

        return targetClient.zrem(key, member);
    }
    
    @Override
    public List<String> blpop(final int timeout, final String key)  {
        writeAsync(key, new Callable<OperationResult<List<String>>>(){
            @Override
            public OperationResult<List<String>> call() throws Exception {
                return d_blpop(timeout, key);
            }
        });

        return targetClient.blpop(timeout, key);
    }
    
    @Override
    public List<String> brpop(final int timeout, final String key)  {
        writeAsync(key, new Callable<OperationResult<List<String>>>(){
            @Override
            public OperationResult<List<String>> call() throws Exception {
                return d_brpop(timeout, key);
            }
        });

        return targetClient.brpop(timeout, key);
    }
    
    /******************* Jedis Dual write for binary commands **************/

    
    @Override
    public String set(final byte[] key, final byte[] value)  {
        writeAsync(key, new Callable<OperationResult<String>>(){
            @Override
            public OperationResult<String> call() throws Exception {
                return d_set(key, value);
            }
        });

        return targetClient.set(key, value);
    }


    
}
