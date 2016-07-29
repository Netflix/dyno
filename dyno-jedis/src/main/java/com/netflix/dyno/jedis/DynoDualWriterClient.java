package com.netflix.dyno.jedis;

import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.contrib.DynoOPMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Map;
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
        this(name, clusterName, pool, operationMonitor, targetClient, new TimestampDial());
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

    private boolean sendShadowRequest(String key) {
        return !this.getConnPool().isIdle() && dial.isInRange(key);
    }

    public interface Dial {
        /**
         * Returns true if the given value is in range, false otherwise
         */
        boolean isInRange(String key);

        void setRange(int range);
    }

    /**
     * Default Dial implementation that presumes no knowledge of the key value
     * and simply uses a timestamp to determine inclusion/exclusion
     */
    private static class TimestampDial implements Dial {

        private final AtomicInteger range = new AtomicInteger(1);

        @Override
        public boolean isInRange(String key) {
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


}
