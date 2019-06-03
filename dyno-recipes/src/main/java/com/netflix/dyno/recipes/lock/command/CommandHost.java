package com.netflix.dyno.recipes.lock.command;

import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.OperationResult;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * This class is used to handle the host connection startup and cleanup.
 * All non abstract subclasses should implement the supplier operation.
 * @param <T>
 */
public abstract class CommandHost<T> implements Supplier<OperationResult<T>> {
    private final Host host;
    private final ConnectionPool pool;

    public CommandHost(Host host, ConnectionPool pool) {
        this.host = host;
        this.pool = pool;
    }

    public Connection getConnection() {
        HostConnectionPool hostPool = pool.getHostPool(host);
        return hostPool.borrowConnection(pool.getConfiguration().getMaxTimeoutWhenExhausted(), TimeUnit.MILLISECONDS);
    }

    public void cleanConnection(Connection connection) {
        connection.getContext().reset();
        connection.getParentConnectionPool().returnConnection(connection);
    }
}
