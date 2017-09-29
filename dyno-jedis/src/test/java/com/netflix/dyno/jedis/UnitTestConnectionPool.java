/**
 * Copyright 2017 Netflix, Inc.
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

import com.netflix.dyno.connectionpool.*;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.impl.ConnectionContextImpl;
import com.netflix.dyno.connectionpool.impl.OperationResultImpl;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.when;

public class UnitTestConnectionPool implements ConnectionPool<Jedis> {

    Map<String, String> redis_data;
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
        this.redis_data = new HashMap<String, String>();

        when(client.set(anyString(), anyString())).thenAnswer(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                String key = (String) invocation.getArguments()[0];
                String value = (String) invocation.getArguments()[1];
                redis_data.put(key, value);
                return "OK";
            }
        });

        when(client.get(anyString())).thenAnswer(new Answer<String>() {

            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                String key = (String) invocation.getArguments()[0];
                return redis_data.get(key);
            }
        });

        when(client.del(anyString())).thenAnswer(new Answer<Long>() {

            @Override
            public Long answer(InvocationOnMock invocation) throws Throwable {
                String key = (String) invocation.getArguments()[0];
                if (redis_data.remove(key) != null) {
                    return (long) 1;
                }
                return (long) 0;
            }
        });

        when(client.hmset(anyString(), anyMap())).thenAnswer(new Answer<String>() {
            @Override
            public String answer(InvocationOnMock invocation) throws Throwable {
                Map<String, String> map = (Map<String, String>) invocation.getArguments()[1];
                if (map != null) {
                    if (map.containsKey(CommandTest.KEY_1KB)) {
                        return "OK";
                    }
                } else {
                    throw new RuntimeException("Map is NULL");
                }

                return "OK";
            }
        });

        when(client.mget(Matchers.<String>anyVararg())).thenAnswer(new Answer<List<String>>() {
            @Override
            public List<String> answer(InvocationOnMock invocation) throws Throwable {

                // Get the keys passed
                Object[] keys = invocation.getArguments();

                List<String> values = new ArrayList<String>(10);
                for (int i = 0; i < keys.length; i++) {
                    // get the ith key, find the value in redis_data
                    // if found, return that else return nil
                    String key = (String) keys[i];
                    String value = redis_data.get(key);
                    values.add(i, value);
                }
                return values;
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
            opMonitor.recordSuccess(op.getName());
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
            public Boolean get(long timeout, TimeUnit unit)
                    throws InterruptedException, ExecutionException, TimeoutException {
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
