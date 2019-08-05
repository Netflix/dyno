package com.netflix.dyno.recipes.lock.command;

import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.jedis.OpName;
import com.netflix.dyno.jedis.operation.BaseKeyOperation;
import redis.clients.jedis.Jedis;

/**
 * Runs a command against the host and is used to remove the lock and checking the ttl on the resource
 */
public class CheckAndRunHost extends CommandHost<Object> {

    private static final String cmdScript = " if redis.call(\"get\",KEYS[1]) == ARGV[1] then\n" +
            "        return redis.call(\"%s\",KEYS[1])\n" +
            "    else\n" +
            "        return 0\n" +
            "    end";

    private final String resource;

    private final String randomKey;

    private final String command;

    public CheckAndRunHost(Host host, ConnectionPool pool, String command, String resource, String randomKey) {
        super(host, pool);
        this.command = String.format(cmdScript, command);
        this.resource = resource;
        this.randomKey = randomKey;
    }


    @Override
    public OperationResult<Object> get() {
        Connection connection = getConnection();
        OperationResult result = connection.execute(new BaseKeyOperation<Object>(randomKey, OpName.EVAL) {
            @Override
            public Object execute(Jedis client, ConnectionContext state) {
                if (randomKey == null) {
                    throw new IllegalStateException("Cannot extend lock with null value for key");
                }
                Object result = client.eval(command, 1, resource, randomKey);
                return result;
            }
        });
        cleanConnection(connection);
        return result;
    }
}
