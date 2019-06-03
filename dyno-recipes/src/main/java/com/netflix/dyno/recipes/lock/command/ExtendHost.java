package com.netflix.dyno.recipes.lock.command;

import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.jedis.OpName;
import com.netflix.dyno.jedis.operation.BaseKeyOperation;
import com.netflix.dyno.recipes.lock.LockResource;
import redis.clients.jedis.Jedis;

import java.util.concurrent.CountDownLatch;

/**
 * Instances of this class should be used to perform extend operations on an acquired lock.
 */
public class ExtendHost extends CommandHost<LockResource> {

    private static final String cmdScript = " if redis.call(\"get\",KEYS[1]) == ARGV[1] then\n" +
            "        return redis.call(\"set\",KEYS[1], ARGV[1], \"px\", ARGV[2])" +
            "    else\n" +
            "        return 0\n" +
            "    end";
    private final LockResource lockResource;
    private final String value;
    private final String randomKey;
    private final CountDownLatch latch;

    public ExtendHost(Host host, ConnectionPool pool, LockResource lockResource, CountDownLatch latch, String randomKey) {
        super(host, pool);
        this.lockResource = lockResource;
        this.value = lockResource.getResource();
        this.randomKey = randomKey;
        this.latch = latch;
    }

    @Override
    public OperationResult<LockResource> get() {
        Connection connection = getConnection();
        OperationResult<LockResource> result = connection.execute(new BaseKeyOperation<Object>(randomKey, OpName.EVAL) {
            @Override
            public LockResource execute(Jedis client, ConnectionContext state) {
                // We need to recheck randomKey in case it got removed before we get here.
                if (randomKey == null) {
                    throw new IllegalStateException("Cannot extend lock with null value for key");
                }
                String result = client.eval(cmdScript, 1, value, randomKey, String.valueOf(lockResource.getTtlMs()))
                        .toString();
                if (result.equals("OK")) {
                    lockResource.incrementLocked();
                    latch.countDown();
                }
                return lockResource;
            }
        });
        cleanConnection(connection);
        return result;
    }
}

