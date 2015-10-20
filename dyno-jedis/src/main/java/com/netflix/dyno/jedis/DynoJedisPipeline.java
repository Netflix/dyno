package com.netflix.dyno.jedis;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.NotThreadSafe;

import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.RedisPipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.FatalConnectionException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl;
import com.netflix.dyno.jedis.JedisConnectionFactory.JedisConnection;

@NotThreadSafe
public class DynoJedisPipeline implements RedisPipeline, AutoCloseable {

    private static final Logger Logger = LoggerFactory.getLogger(DynoJedisPipeline.class);

    // ConnPool and connection to exec the pipeline
    private final ConnectionPoolImpl<Jedis> connPool;
    private volatile Connection<Jedis> connection;
    private final DynoJedisPipelineMonitor opMonitor;
    private final ConnectionPoolMonitor cpMonitor;

    // the cached pipeline
    private volatile Pipeline jedisPipeline = null;
    // the cached row key for the pipeline. all subsequent requests to pipeline must be the same. this is used to check that.
    private final AtomicReference<String> theKey = new AtomicReference<String>(null);
    // used for tracking errors
    private final AtomicReference<DynoException> pipelineEx = new AtomicReference<DynoException>(null);

    private static final String DynoPipeline = "DynoPipeline";

    DynoJedisPipeline(ConnectionPoolImpl<Jedis> cPool, DynoJedisPipelineMonitor operationMonitor, ConnectionPoolMonitor connPoolMonitor) {
        this.connPool = cPool;
        this.opMonitor = operationMonitor;
        this.cpMonitor = connPoolMonitor;
    }

    private void checkKey(final String key) {

        if (theKey.get() != null) {
            verifyKey(key);

        } else {

            boolean success = theKey.compareAndSet(null, key);
            if (!success) {
                // someone already beat us to it. that's fine, just verify that the key is the same
                verifyKey(key);
            } else {

                try {
                    connection = connPool.getConnectionForOperation(new BaseOperation<Jedis, String>() {

                        @Override
                        public String getName() {
                            return DynoPipeline;
                        }

                        @Override
                        public String getKey() {
                            return key;
                        }
                    });
                } catch (NoAvailableHostsException nahe) {
                    cpMonitor.incOperationFailure(connection != null ? connection.getHost() : null, nahe);
                    discardPipelineAndReleaseConnection();
                    throw nahe;
                }
            }

            Jedis jedis = ((JedisConnection) connection).getClient();
            jedisPipeline = jedis.pipelined();
            cpMonitor.incOperationSuccess(connection.getHost(), 0);
        }
    }

    private void verifyKey(final String key) {

        if (!theKey.get().equals(key)) {
            try {
                throw new RuntimeException("Must have same key for Redis Pipeline in Dynomite");
            } finally {
                discardPipelineAndReleaseConnection();
            }
        }
    }

    private abstract class PipelineOperation<R> {

        abstract Response<R> execute(Pipeline jedisPipeline) throws DynoException;

        Response<R> execute(final byte[] key, final OpName opName) {
            // For now simply convert the key into a String. Properly supporting this
            // functionality requires significant changes to plumb this throughout for the LB
            return execute(new String(key), opName);
        }

        Response<R> execute(final String key, final OpName opName) {

            checkKey(key);
            return executeOperation(opName);

        }

        Response<R> executeOperation(final OpName opName) {
            try {
                opMonitor.recordOperation(opName.name());
                return execute(jedisPipeline);

            } catch (JedisConnectionException ex) {
                DynoException e = new FatalConnectionException(ex).setAttempt(1);
                pipelineEx.set(e);
                cpMonitor.incOperationFailure(connection.getHost(), e);
                throw ex;
            }
        }
    }

    @Override
    public Response<Long> append(final String key, final String value) {

        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.append(key, value);
            }

        }.execute(key, OpName.APPEND);
    }

    @Override
    public Response<List<String>> blpop(final String arg) {

        return new PipelineOperation<List<String>>() {

            @Override
            Response<List<String>> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.blpop(arg);
            }
        }.execute(arg, OpName.BLPOP);

    }

    @Override
    public Response<List<String>> brpop(final String arg) {
        return new PipelineOperation<List<String>>() {

            @Override
            Response<List<String>> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.brpop(arg);
            }
        }.execute(arg, OpName.BRPOP);

    }

    @Override
    public Response<Long> decr(final String key) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.decr(key);
            }
        }.execute(key, OpName.DECR);

    }

    @Override
    public Response<Long> decrBy(final String key, final long integer) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.decrBy(key, integer);
            }
        }.execute(key, OpName.DECRBY);

    }

    @Override
    public Response<Long> del(final String key) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.del(key);
            }
        }.execute(key, OpName.DEL);

    }

    @Override
    public Response<String> echo(final String string) {
        return new PipelineOperation<String>() {

            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.echo(string);
            }
        }.execute(string, OpName.ECHO);

    }

    @Override
    public Response<Boolean> exists(final String key) {
        return new PipelineOperation<Boolean>() {

            @Override
            Response<Boolean> execute(final Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.exists(key);
            }
        }.execute(key, OpName.EXISTS);

    }

    @Override
    public Response<Long> expire(final String key, final int seconds) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(final Pipeline jedisPipeline) throws DynoException {
                long startTime = System.nanoTime() / 1000;
                try {
                    return jedisPipeline.expire(key, seconds);
                } finally {
                    long duration = System.nanoTime() / 1000 - startTime;
                    opMonitor.recordSendLatency(OpName.EXPIRE.name(), duration, TimeUnit.MICROSECONDS);
                }
            }
        }.execute(key, OpName.EXPIRE);

    }

    @Override
    public Response<Long> pexpire(String key, long milliseconds) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<Long> expireAt(final String key, final long unixTime) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(final Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.expireAt(key, unixTime);
            }
        }.execute(key, OpName.EXPIREAT);

    }

    @Override
    public Response<Long> pexpireAt(String key, long millisecondsTimestamp) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public Response<String> get(final String key) {
        return new PipelineOperation<String>() {

            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                long startTime = System.nanoTime() / 1000;
                try {
                    return jedisPipeline.get(key);
                } finally {
                    long duration = System.nanoTime() / 1000 - startTime;
                    opMonitor.recordSendLatency(OpName.GET.name(), duration, TimeUnit.MICROSECONDS);
                }
            }
        }.execute(key, OpName.GET);

    }

    @Override
    public Response<Boolean> getbit(final String key, final long offset) {
        return new PipelineOperation<Boolean>() {

            @Override
            Response<Boolean> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.getbit(key, offset);
            }
        }.execute(key, OpName.GETBIT);

    }

    @Override
    public Response<String> getrange(final String key, final long startOffset, final long endOffset) {
        return new PipelineOperation<String>() {

            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.getrange(key, startOffset, endOffset);
            }
        }.execute(key, OpName.GETRANGE);

    }

    @Override
    public Response<String> getSet(final String key, final String value) {
        return new PipelineOperation<String>() {

            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.getSet(key, value);
            }
        }.execute(key, OpName.GETSET);

    }

    @Override
    public Response<Long> hdel(final String key, final String... field) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.hdel(key, field);
            }
        }.execute(key, OpName.HDEL);

    }

    @Override
    public Response<Boolean> hexists(final String key, final String field) {
        return new PipelineOperation<Boolean>() {

            @Override
            Response<Boolean> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.hexists(key, field);
            }
        }.execute(key, OpName.HEXISTS);

    }

    @Override
    public Response<String> hget(final String key, final String field) {
        return new PipelineOperation<String>() {

            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.hget(key, field);
            }
        }.execute(key, OpName.HGET);

    }

    /**
     * This method is a BinaryRedisPipeline command which dyno does not yet properly support, therefore the
     * interface is not yet implemented.
     */
    public Response<byte[]> hget(final byte[] key, final byte[] field) {
        return new PipelineOperation<byte[]>() {

            @Override
            Response<byte[]> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.hget(key, field);
            }
        }.execute(key, OpName.HGET);

    }

    @Override
    public Response<Map<String, String>> hgetAll(final String key) {
        return new PipelineOperation<Map<String, String>>() {

            @Override
            Response<Map<String, String>> execute(Pipeline jedisPipeline) throws DynoException {
                long startTime = System.nanoTime() / 1000;
                try {
                    return jedisPipeline.hgetAll(key);
                } finally {
                    long duration = System.nanoTime() / 1000 - startTime;
                    opMonitor.recordSendLatency(OpName.HGETALL.name(), duration, TimeUnit.MICROSECONDS);
                }
            }

        }.execute(key, OpName.HGETALL);

    }

    /**
     * This method is a BinaryRedisPipeline command which dyno does not yet properly support, therefore the
     * interface is not yet implemented.
     */
    public Response<Map<byte[], byte[]>> hgetAll(final byte[] key) {
        return new PipelineOperation<Map<byte[], byte[]>>() {

            @Override
            Response<Map<byte[], byte[]>> execute(Pipeline jedisPipeline) throws DynoException {
                long startTime = System.nanoTime() / 1000;
                try {
                    return jedisPipeline.hgetAll(key);
                } finally {
                    long duration = System.nanoTime() / 1000 - startTime;
                    opMonitor.recordSendLatency(OpName.HGETALL.name(), duration, TimeUnit.MICROSECONDS);
                }
            }
        }.execute(key, OpName.HGETALL);
    }

    @Override
    public Response<Long> hincrBy(final String key, final String field, final long value) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.hincrBy(key, field, value);
            }
        }.execute(key, OpName.HINCRBY);
    }
    
    /* not supported by RedisPipeline 2.7.3 */
    public Response<Double> hincrByFloat(final String key, final String field, final double value) {
        return new PipelineOperation<Double>() {

            @Override
            Response<Double> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.hincrByFloat(key, field, value);
            }
        }.execute(key, OpName.HINCRBYFLOAT);
    }

    @Override
    public Response<Set<String>> hkeys(final String key) {
        return new PipelineOperation<Set<String>>() {

            @Override
            Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.hkeys(key);
            }
        }.execute(key, OpName.HKEYS);

    }

    @Override
    public Response<Long> hlen(final String key) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.hlen(key);
            }
        }.execute(key, OpName.HLEN);

    }

    /**
     * This method is a BinaryRedisPipeline command which dyno does not yet properly support, therefore the
     * interface is not yet implemented.
     */
    public Response<List<byte[]>> hmget(final byte[] key, final byte[]... fields) {
        return new PipelineOperation<List<byte[]>>() {

            @Override
            Response<List<byte[]>> execute(Pipeline jedisPipeline) throws DynoException {
                long startTime = System.nanoTime() / 1000;
                try {
                    return jedisPipeline.hmget(key, fields);
                } finally {
                    long duration = System.nanoTime() / 1000 - startTime;
                    opMonitor.recordSendLatency(OpName.HMGET.name(), duration, TimeUnit.MICROSECONDS);
                }
            }
        }.execute(key, OpName.HMGET);
    }

    @Override
    public Response<List<String>> hmget(final String key, final String... fields) {
        return new PipelineOperation<List<String>>() {

            @Override
            Response<List<String>> execute(Pipeline jedisPipeline) throws DynoException {
                long startTime = System.nanoTime() / 1000;
                try {
                    return jedisPipeline.hmget(key, fields);
                } finally {
                    long duration = System.nanoTime() / 1000 - startTime;
                    opMonitor.recordSendLatency(OpName.HMGET.name(), duration, TimeUnit.MICROSECONDS);
                }
            }
        }.execute(key, OpName.HMGET);

    }

    /**
     * This method is a BinaryRedisPipeline command which dyno does not yet properly support, therefore the
     * interface is not yet implemented.
     */
    public Response<String> hmset(final byte[] key, final Map<byte[], byte[]> hash) {
        return new PipelineOperation<String>() {

            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                long startTime = System.nanoTime() / 1000;
                try {
                    return jedisPipeline.hmset(key, hash);
                } finally {
                    long duration = System.nanoTime() / 1000 - startTime;
                    opMonitor.recordSendLatency(OpName.HMSET.name(), duration, TimeUnit.MICROSECONDS);
                }
            }
        }.execute(key, OpName.HMSET);
    }

    @Override
    public Response<String> hmset(final String key, final Map<String, String> hash) {
        return new PipelineOperation<String>() {

            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                long startTime = System.nanoTime() / 1000;
                try {
                    return jedisPipeline.hmset(key, hash);
                } finally {
                    long duration = System.nanoTime() / 1000 - startTime;
                    opMonitor.recordSendLatency(OpName.HMSET.name(), duration, TimeUnit.MICROSECONDS);
                }
            }
        }.execute(key, OpName.HMSET);

    }

    @Override
    public Response<Long> hset(final String key, final String field, final String value) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.hset(key, field, value);
            }
        }.execute(key, OpName.HSET);

    }

    /**
     * This method is a BinaryRedisPipeline command which dyno does not yet properly support, therefore the
     * interface is not yet implemented.
     */
    public Response<Long> hset(final byte[] key, final byte[] field, final byte[] value) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.hset(key, field, value);
            }
        }.execute(key, OpName.HSET);

    }

    @Override
    public Response<Long> hsetnx(final String key, final String field, final String value) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.hsetnx(key, field, value);
            }
        }.execute(key, OpName.HSETNX);

    }

    @Override
    public Response<List<String>> hvals(final String key) {
        return new PipelineOperation<List<String>>() {

            @Override
            Response<List<String>> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.hvals(key);
            }
        }.execute(key, OpName.HVALS);

    }

    @Override
    public Response<Long> incr(final String key) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.incr(key);
            }

        }.execute(key, OpName.INCR);

    }

    @Override
    public Response<Long> incrBy(final String key, final long integer) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.incrBy(key, integer);
            }

        }.execute(key, OpName.INCRBY);

    }
    
    /* not supported by RedisPipeline 2.7.3 */
    public Response<Double> incrByFloat(final String key, final double increment) {
        return new PipelineOperation<Double>() {

            @Override
            Response<Double> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.incrByFloat(key, increment);
            }

        }.execute(key, OpName.INCRBYFLOAT);

    }


    @Override
    public Response<String> lindex(final String key, final long index) {
        return new PipelineOperation<String>() {

            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.lindex(key, index);
            }

        }.execute(key, OpName.LINDEX);

    }

    @Override
    public Response<Long> linsert(final String key, final LIST_POSITION where, final String pivot, final String value) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return linsert(key, where, pivot, value);
            }

        }.execute(key, OpName.LINSERT);

    }

    @Override
    public Response<Long> llen(final String key) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.llen(key);
            }

        }.execute(key, OpName.LLEN);

    }

    @Override
    public Response<String> lpop(final String key) {
        return new PipelineOperation<String>() {

            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.lpop(key);
            }

        }.execute(key, OpName.LPOP);

    }

    @Override
    public Response<Long> lpush(final String key, final String... string) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.lpush(key, string);
            }

        }.execute(key, OpName.LPUSH);

    }

    @Override
    public Response<Long> lpushx(final String key, final String... string) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.lpushx(key, string);
            }

        }.execute(key, OpName.LPUSHX);

    }

    @Override
    public Response<List<String>> lrange(final String key, final long start, final long end) {
        return new PipelineOperation<List<String>>() {

            @Override
            Response<List<String>> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.lrange(key, start, end);
            }

        }.execute(key, OpName.LRANGE);

    }

    @Override
    public Response<Long> lrem(final String key, final long count, final String value) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.lrem(key, count, value);
            }

        }.execute(key, OpName.LREM);

    }

    @Override
    public Response<String> lset(final String key, final long index, final String value) {
        return new PipelineOperation<String>() {

            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.lset(key, index, value);
            }

        }.execute(key, OpName.LSET);

    }

    @Override
    public Response<String> ltrim(final String key, final long start, final long end) {
        return new PipelineOperation<String>() {

            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.ltrim(key, start, end);
            }

        }.execute(key, OpName.LTRIM);

    }

    @Override
    public Response<Long> move(final String key, final int dbIndex) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.move(key, dbIndex);
            }

        }.execute(key, OpName.MOVE);

    }

    @Override
    public Response<Long> persist(final String key) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.persist(key);
            }

        }.execute(key, OpName.PERSIST);

    }
    
    /* not supported by RedisPipeline 2.7.3 */
    public Response<String> rename(final String oldkey, final String newkey) {
        return new PipelineOperation<String>() {
        	
            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.rename(oldkey, newkey);
            }
        }.execute(oldkey, OpName.RENAME);

    }
    
    /* not supported by RedisPipeline 2.7.3 */
    public Response<Long> renamenx(final String oldkey, final String newkey) {
        return new PipelineOperation<Long>() {
        	
            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.renamenx(oldkey, newkey);
            }
        }.execute(oldkey, OpName.RENAMENX);

    }

    @Override
    public Response<String> rpop(final String key) {
        return new PipelineOperation<String>() {

            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.rpop(key);
            }

        }.execute(key, OpName.RPOP);

    }

    @Override
    public Response<Long> rpush(final String key, final String... string) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.rpush(key, string);
            }

        }.execute(key, OpName.RPUSH);

    }

    @Override
    public Response<Long> rpushx(final String key, final String... string) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.rpushx(key, string);
            }

        }.execute(key, OpName.RPUSHX);

    }

    @Override
    public Response<Long> sadd(final String key, final String... member) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.sadd(key, member);
            }

        }.execute(key, OpName.SADD);

    }

    @Override
    public Response<Long> scard(final String key) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.scard(key);
            }

        }.execute(key, OpName.SCARD);

    }

    @Override
    public Response<Boolean> sismember(final String key, final String member) {
        return new PipelineOperation<Boolean>() {

            @Override
            Response<Boolean> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.sismember(key, member);
            }

        }.execute(key, OpName.SISMEMBER);

    }

    @Override
    public Response<String> set(final String key, final String value) {
        return new PipelineOperation<String>() {

            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                long startTime = System.nanoTime() / 1000;
                try {
                    return jedisPipeline.set(key, value);
                } finally {
                    long duration = System.nanoTime() / 1000 - startTime;
                    opMonitor.recordSendLatency(OpName.SET.name(), duration, TimeUnit.MICROSECONDS);
                }
            }

        }.execute(key, OpName.SET);

    }

    @Override
    public Response<Boolean> setbit(final String key, final long offset, final boolean value) {
        return new PipelineOperation<Boolean>() {

            @Override
            Response<Boolean> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.setbit(key, offset, value);
            }

        }.execute(key, OpName.SETBIT);

    }

    @Override
    public Response<String> setex(final String key, final int seconds, final String value) {
        return new PipelineOperation<String>() {

            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.setex(key, seconds, value);
            }

        }.execute(key, OpName.SETEX);

    }

    @Override
    public Response<Long> setnx(final String key, final String value) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.setnx(key, value);
            }

        }.execute(key, OpName.SETNX);

    }

    @Override
    public Response<Long> setrange(final String key, final long offset, final String value) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.setrange(key, offset, value);
            }

        }.execute(key, OpName.SETRANGE);

    }

    @Override
    public Response<Set<String>> smembers(final String key) {
        return new PipelineOperation<Set<String>>() {

            @Override
            Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.smembers(key);
            }

        }.execute(key, OpName.SMEMBERS);

    }

    @Override
    public Response<List<String>> sort(final String key) {
        return new PipelineOperation<List<String>>() {

            @Override
            Response<List<String>> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.sort(key);
            }

        }.execute(key, OpName.SORT);

    }

    @Override
    public Response<List<String>> sort(final String key, final SortingParams sortingParameters) {
        return new PipelineOperation<List<String>>() {

            @Override
            Response<List<String>> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.sort(key, sortingParameters);
            }

        }.execute(key, OpName.SORT);

    }

    @Override
    public Response<String> spop(final String key) {
        return new PipelineOperation<String>() {

            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.spop(key);
            }

        }.execute(key, OpName.SPOP);

    }
    
    @Override
    public Response<Set<String>> spop(final String key, final long count) {
        throw new UnsupportedOperationException("not yet implemented");
    }


    @Override
    public Response<String> srandmember(final String key) {
        return new PipelineOperation<String>() {

            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.srandmember(key);
            }

        }.execute(key, OpName.SRANDMEMBER);

    }

    @Override
    public Response<Long> srem(final String key, final String... member) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.srem(key, member);
            }

        }.execute(key, OpName.SREM);

    }

    @Override
    public Response<Long> strlen(final String key) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.strlen(key);
            }

        }.execute(key, OpName.STRLEN);

    }

    @Override
    public Response<String> substr(final String key, final int start, final int end) {
        return new PipelineOperation<String>() {

            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.substr(key, start, end);
            }

        }.execute(key, OpName.SUBSTR);

    }

    @Override
    public Response<Long> ttl(final String key) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.ttl(key);
            }

        }.execute(key, OpName.TTL);

    }

    @Override
    public Response<String> type(final String key) {
        return new PipelineOperation<String>() {

            @Override
            Response<String> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.type(key);
            }

        }.execute(key, OpName.TYPE);

    }

    @Override
    public Response<Long> zadd(final String key, final double score, final String member) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zadd(key, score, member);
            }

        }.execute(key, OpName.ZADD);

    }

    @Override
    public Response<Long> zcard(final String key) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zcard(key);
            }

        }.execute(key, OpName.ZCARD);

    }

    @Override
    public Response<Long> zcount(final String key, final double min, final double max) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zcount(key, min, max);
            }

        }.execute(key, OpName.ZCOUNT);

    }

    @Override
    public Response<Double> zincrby(final String key, final double score, final String member) {
        return new PipelineOperation<Double>() {

            @Override
            Response<Double> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zincrby(key, score, member);
            }

        }.execute(key, OpName.ZINCRBY);

    }

    @Override
    public Response<Set<String>> zrange(final String key, final long start, final long end) {
        return new PipelineOperation<Set<String>>() {

            @Override
            Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zrange(key, start, end);
            }

        }.execute(key, OpName.ZRANGE);

    }

    @Override
    public Response<Set<String>> zrangeByScore(final String key, final double min, final double max) {
        return new PipelineOperation<Set<String>>() {

            @Override
            Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zrangeByScore(key, min, max);
            }

        }.execute(key, OpName.ZRANGEBYSCORE);

    }

    @Override
    public Response<Set<String>> zrangeByScore(final String key, final String min, final String max) {
        return new PipelineOperation<Set<String>>() {

            @Override
            Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zrangeByScore(key, min, max);
            }

        }.execute(key, OpName.ZRANGEBYSCORE);

    }

    @Override
    public Response<Set<String>> zrangeByScore(final String key, final double min, final double max, final int offset, final int count) {
        return new PipelineOperation<Set<String>>() {

            @Override
            Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zrangeByScore(key, min, max, offset, count);
            }

        }.execute(key, OpName.ZRANGEBYSCORE);

    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final String key, final double min, final double max) {
        return new PipelineOperation<Set<Tuple>>() {

            @Override
            Response<Set<Tuple>> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zrangeByScoreWithScores(key, min, max);
            }

        }.execute(key, OpName.ZRANGEBYSCOREWITHSCORES);

    }

    @Override
    public Response<Set<Tuple>> zrangeByScoreWithScores(final String key, final double min, final double max, final int offset, final int count) {
        return new PipelineOperation<Set<Tuple>>() {

            @Override
            Response<Set<Tuple>> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zrangeByScoreWithScores(key, min, max, offset, count);
            }

        }.execute(key, OpName.ZRANGEBYSCOREWITHSCORES);

    }

    @Override
    public Response<Set<String>> zrevrangeByScore(final String key, final double max, final double min) {
        return new PipelineOperation<Set<String>>() {

            @Override
            Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
                return zrevrangeByScore(key, max, min);
            }

        }.execute(key, OpName.ZREVRANGEBYSCORE);

    }

    @Override
    public Response<Set<String>> zrevrangeByScore(final String key, final String max, final String min) {
        return new PipelineOperation<Set<String>>() {

            @Override
            Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zrevrangeByScore(key, max, min);
            }

        }.execute(key, OpName.ZREVRANGEBYSCORE);

    }

    @Override
    public Response<Set<String>> zrevrangeByScore(final String key, final double max, final double min, final int offset, final int count) {
        return new PipelineOperation<Set<String>>() {

            @Override
            Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
                return zrevrangeByScore(key, max, min, offset, count);
            }

        }.execute(key, OpName.ZREVRANGEBYSCORE);

    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key, final double max, final double min) {
        return new PipelineOperation<Set<Tuple>>() {

            @Override
            Response<Set<Tuple>> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zrevrangeByScoreWithScores(key, max, min);
            }

        }.execute(key, OpName.ZREVRANGEBYSCOREWITHSCORES);

    }

    @Override
    public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key, final double max, final double min, final int offset, final int count) {
        return new PipelineOperation<Set<Tuple>>() {

            @Override
            Response<Set<Tuple>> execute(Pipeline jedisPipeline) throws DynoException {
                return zrevrangeByScoreWithScores(key, max, min, offset, count);
            }

        }.execute(key, OpName.ZREVRANGEBYSCOREWITHSCORES);

    }

    @Override
    public Response<Set<Tuple>> zrangeWithScores(final String key, final long start, final long end) {
        return new PipelineOperation<Set<Tuple>>() {

            @Override
            Response<Set<Tuple>> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zrangeWithScores(key, start, end);
            }

        }.execute(key, OpName.ZRANGEWITHSCORES);

    }

    @Override
    public Response<Long> zrank(final String key, final String member) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zrank(key, member);
            }

        }.execute(key, OpName.ZRANK);

    }

    @Override
    public Response<Long> zrem(final String key, final String... member) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zrem(key, member);
            }

        }.execute(key, OpName.ZREM);

    }

    @Override
    public Response<Long> zremrangeByRank(final String key, final long start, final long end) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zremrangeByRank(key, start, end);
            }

        }.execute(key, OpName.ZREMRANGEBYRANK);

    }

    @Override
    public Response<Long> zremrangeByScore(final String key, final double start, final double end) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zremrangeByScore(key, start, end);
            }

        }.execute(key, OpName.ZREMRANGEBYSCORE);

    }

    @Override
    public Response<Set<String>> zrevrange(final String key, final long start, final long end) {
        return new PipelineOperation<Set<String>>() {

            @Override
            Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zrevrange(key, start, end);
            }

        }.execute(key, OpName.ZREVRANGE);

    }

    @Override
    public Response<Set<Tuple>> zrevrangeWithScores(final String key, final long start, final long end) {
        return new PipelineOperation<Set<Tuple>>() {

            @Override
            Response<Set<Tuple>> execute(Pipeline jedisPipeline) throws DynoException {
                return zrevrangeWithScores(key, start, end);
            }

        }.execute(key, OpName.ZREVRANGEWITHSCORES);

    }

    @Override
    public Response<Long> zrevrank(final String key, final String member) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zrevrank(key, member);
            }

        }.execute(key, OpName.ZREVRANK);

    }

    @Override
    public Response<Double> zscore(final String key, final String member) {
        return new PipelineOperation<Double>() {

            @Override
            Response<Double> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.zscore(key, member);
            }

        }.execute(key, OpName.ZSCORE);

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
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.bitcount(key);
            }

        }.execute(key, OpName.BITCOUNT);

    }

    @Override
    public Response<Long> bitcount(final String key, final long start, final long end) {
        return new PipelineOperation<Long>() {

            @Override
            Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
                return jedisPipeline.bitcount(key, start, end);
            }

        }.execute(key, OpName.BITCOUNT);

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
    public Response<Set<String>> zrevrangeByLex(String key, String max, String min) {
        throw new UnsupportedOperationException("not yet implemented");
    }
    
    @Override
    public Response<Set<String>> zrevrangeByLex(String key, String max, String min, int offset, int count) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    public void sync() {
        long startTime = System.nanoTime() / 1000;
        try {
            jedisPipeline.sync();
            opMonitor.recordPipelineSync();
        } catch (JedisConnectionException jce) {
            String msg = "Failed sync() to host: " + getHostInfo();
            pipelineEx.set(new FatalConnectionException(msg, jce));
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
            pipelineEx.set(new FatalConnectionException(msg, jce));
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
        try {
            if (jedisPipeline != null) {
                long startTime = System.nanoTime() / 1000;
                jedisPipeline.sync();
                if (recordLatency) {
                    long duration = System.nanoTime() / 1000 - startTime;
                    opMonitor.recordLatency(duration, TimeUnit.MICROSECONDS);
                }
                jedisPipeline = null;
            }
        } catch (Exception e) {
            Logger.warn(String.format("Failed to discard jedis pipeline, %s", getHostInfo()), e);
        }
    }

    private void releaseConnection() {
        if (connection != null) {
            try {
                connection.getContext().reset();
                connection.getParentConnectionPool().returnConnection(connection);
                if (pipelineEx.get() != null) {
                    connPool.getCPHealthTracker().trackConnectionError(connection.getParentConnectionPool(), pipelineEx.get());
                    pipelineEx.set(null);
                }
                connection = null;
            } catch (Exception e) {
                Logger.warn(String.format("Failed to return connection in Dyno Jedis Pipeline, %s", getHostInfo()), e);
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
