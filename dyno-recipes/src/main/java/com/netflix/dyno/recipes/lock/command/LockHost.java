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
import redis.clients.jedis.params.SetParams;

import java.util.concurrent.CountDownLatch;

/**
 * This class is used to acquire the lock on a host with a resource.
 */
public class LockHost extends CommandHost<LockResource> {

    private final String value;
    private final LockResource lockResource;

    private final SetParams params;
    private final String randomKey;
    private final CountDownLatch latch;

    public LockHost(Host host, ConnectionPool pool, LockResource lockResource, CountDownLatch latch, String randomKey) {
        super(host, pool);
        this.lockResource = lockResource;
        this.value = lockResource.getResource();
        this.params = SetParams.setParams().nx().px(lockResource.getTtlMs());
        this.randomKey = randomKey;
        this.latch = latch;
    }

    @Override
    public OperationResult<LockResource> get() {
        Connection connection = getConnection();
        OperationResult result = connection.execute(new BaseKeyOperation<LockResource>(value, OpName.SET) {
            @Override
            public LockResource execute(Jedis client, ConnectionContext state) {
                String result = client.set(value, randomKey, params);
                if (result != null && result.equals("OK")) {
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
