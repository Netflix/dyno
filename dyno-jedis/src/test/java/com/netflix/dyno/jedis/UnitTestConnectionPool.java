package com.netflix.dyno.jedis;

import com.netflix.dyno.connectionpool.*;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.impl.ConnectionContextImpl;
import com.netflix.dyno.connectionpool.impl.OperationResultImpl;
import com.netflix.dyno.connectionpool.impl.utils.ZipUtils;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import redis.clients.jedis.Jedis;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.anyMap;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

public class UnitTestConnectionPool implements ConnectionPool<Jedis> {

    @Mock
    Jedis client;

    @Mock
    Connection<Jedis> connection;

    private final ConnectionPoolConfiguration config;

    private final ConnectionContextImpl context = new ConnectionContextImpl();

    private final OperationMonitor opMonitor;

    public UnitTestConnectionPool(ConnectionPoolConfiguration config, OperationMonitor opMonitor) {
        MockitoAnnotations.initMocks(this);

        this.config = config;
        this.opMonitor = opMonitor;

        when(client.set(anyString(), anyString())).thenAnswer(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                return (String) invocation.getArguments()[1];
            }
        });

        when(client.get(CompressionTest.VALUE_1KB)).thenReturn(CompressionTest.VALUE_1KB);

        when(client.get(CompressionTest.KEY_3KB)).thenAnswer(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                return ZipUtils.compressStringToBase64String(CompressionTest.VALUE_3KB);
            }
        });

        when(client.get(CompressionTest.KEY_1KB)).thenReturn(CompressionTest.VALUE_1KB);

        when(client.hmset(anyString(), anyMap())).thenAnswer(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                Map<String, String> map = (Map<String, String>) invocation.getArguments()[1];
                if (map != null) {
                    if (map.containsKey(CompressionTest.KEY_3KB)) {
                        if (ZipUtils.isCompressed(map.get(CompressionTest.KEY_3KB))) {
                            return "OK";
                        } else {
                            throw new RuntimeException("Value was not compressed");
                        }
                    }
                } else {
                    throw new RuntimeException("Map is NULL");
                }

                return "OK";
            }
        });

    }

    @Override
    public boolean addHost(Host host) {
        return true;
    }

    @Override
    public boolean removeHost(Host host) {
        return true;
    }

    @Override
    public boolean isHostUp(Host host) {
        return false;
    }

    @Override
    public boolean hasHost(Host host) {
        return false;
    }

    @Override
    public List<HostConnectionPool<Jedis>> getActivePools() {
        return null;
    }

    @Override
    public List<HostConnectionPool<Jedis>> getPools() {
        return null;
    }

    @Override
    public HostConnectionPool<Jedis> getHostPool(Host host) {
        return null;
    }

    @Override
    public <R> Collection<OperationResult<R>> executeWithRing(Operation<Jedis, R> op) throws DynoException {
        return null;
    }

    @Override
    public <R> ListenableFuture<OperationResult<R>> executeAsync(AsyncOperation<Jedis, R> op) throws DynoException {
        return null;
    }

    @Override
    public <R> OperationResult<R> executeWithFailover(Operation<Jedis, R> op) throws DynoException {
        try {
            R r = op.execute(client, context);
            if (context.hasMetadata("compression") || context.hasMetadata("decompression")) {
                opMonitor.recordSuccess(op.getName(), true);
            } else {
                opMonitor.recordSuccess(op.getName());
            }
            return new OperationResultImpl<R>("Test", r, null);
        } finally {
            context.reset();
        }

    }

    @Override
    public void shutdown() {

    }

    @Override
    public Future<Boolean> start() throws DynoException {
        return new Future<Boolean>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                return false;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public boolean isDone() {
                return false;
            }

            @Override
            public Boolean get() throws InterruptedException, ExecutionException {
                return true;
            }

            @Override
            public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                return true;
            }
        };
    }

    @Override
    public void idle() {

    }

    @Override
    public ConnectionPoolConfiguration getConfiguration() {
        return config;
    }

    @Override
    public HealthTracker<Jedis> getHealthTracker() {
        return null;
    }

    @Override
    public boolean isIdle() {
        return false;
    }


    @Override
    public Future<Boolean> updateHosts(Collection activeHosts, Collection inactiveHosts) {
        return null;
    }
}
