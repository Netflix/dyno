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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.ConnectionPoolMonitor;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.StringBasedResult;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl;

import redis.clients.jedis.BasicCommands;
import redis.clients.jedis.DebugParams;
import redis.clients.jedis.Jedis;

public class DynoBasicRedisCommands implements BasicCommands {

    private static final Logger Logger = org.slf4j.LoggerFactory.getLogger(DynoBasicRedisCommands.class);
    private final ConnectionPoolImpl<Jedis> connPool;
    protected final DynoBasicRedisCommandsMonitor rcMonitor;
    protected final ConnectionPoolMonitor cpMonitor;

    DynoBasicRedisCommands(ConnectionPoolImpl<Jedis> cPool, DynoBasicRedisCommandsMonitor redisCommandMonitor,
            ConnectionPoolMonitor connPoolMonitor) {
        this.connPool = cPool;
        this.rcMonitor = redisCommandMonitor;
        this.cpMonitor = connPoolMonitor;
    }

    private abstract class BaseCommOperation<T> implements Operation<Jedis, T> {

        private final CommName cm;

        private BaseCommOperation(final CommName cm) {
            this.cm = cm;
        }

        @Override
        public String getName() {
            return cm.name();
        }

    }
    
    public StringBasedResult<String> dyno_flushall() {
        final Map<String, String> results = new LinkedHashMap<>();

        List<OperationResult<String>> opResults = scatterFlushAll();
        for (OperationResult<String> opResult : opResults) {
            results.put(opResult.getNode().getHostAddress(), opResult.getResult());
        }

        return new StringBasedResultImpl<>(results);

    }
    
    public StringBasedResult<String> dyno_flushdb() {
        final Map<String, String> results = new LinkedHashMap<>();

        List<OperationResult<String>> opResults = scatterFlushDB();
        for (OperationResult<String> opResult : opResults) {
            results.put(opResult.getNode().getHostAddress(), opResult.getResult());
        }

        return new StringBasedResultImpl<>(results);

    }
    

    private List<OperationResult<String>> scatterFlushAll() {
        return new ArrayList<>(connPool.executeWithRing(new BaseCommOperation<String>(CommName.FLUSHALL) {
            @Override
            public String execute(final Jedis client, final ConnectionContext state) throws DynoException {

                return client.flushAll();
            }

            @Override
            public String getKey() {
                // TODO Auto-generated method stub
                return null;
            }
        }));
    }
    
    private List<OperationResult<String>> scatterFlushDB() {
        return new ArrayList<>(connPool.executeWithRing(new BaseCommOperation<String>(CommName.FLUSHDB) {
            @Override
            public String execute(final Jedis client, final ConnectionContext state) throws DynoException {

                return client.flushDB();
            }

            @Override
            public String getKey() {
                // TODO Auto-generated method stub
                return null;
            }
        }));
    }

    @Override
    public String ping() {
        throw new UnsupportedOperationException("not yet implemented");

    }

    @Override
    public String quit() {
        throw new UnsupportedOperationException("not yet implemented");

    }

    @Override
    public Long dbSize() {
        throw new UnsupportedOperationException("not yet implemented");

    }

    @Override
    public String select(int index) {
        throw new UnsupportedOperationException("not yet implemented");

    }


    @Override
    public String auth(String password) {
        throw new UnsupportedOperationException("not yet implemented");

    }

    @Override
    public String save() {
        throw new UnsupportedOperationException("not yet implemented");

    }

    @Override
    public String bgsave() {
        throw new UnsupportedOperationException("not yet implemented");

    }

    @Override
    public String bgrewriteaof() {
        throw new UnsupportedOperationException("not yet implemented");

    }

    @Override
    public Long lastsave() {
        throw new UnsupportedOperationException("not yet implemented");

    }

    @Override
    public String shutdown() {
        throw new UnsupportedOperationException("not yet implemented");

    }

    @Override
    public String info() {
        throw new UnsupportedOperationException("not yet implemented");

    }

    @Override
    public String info(String section) {
        throw new UnsupportedOperationException("not yet implemented");

    }

    @Override
    public String slaveof(String host, int port) {
        throw new UnsupportedOperationException("not yet implemented");

    }

    @Override
    public String slaveofNoOne() {
        throw new UnsupportedOperationException("not yet implemented");

    }

    @Override
    public Long getDB() {
        throw new UnsupportedOperationException("not yet implemented");

    }

    @Override
    public String debug(DebugParams params) {
        throw new UnsupportedOperationException("not yet implemented");

    }

    @Override
    public String configResetStat() {
        throw new UnsupportedOperationException("not yet implemented");

    }

    @Override
    public Long waitReplicas(int replicas, long timeout) {
        throw new UnsupportedOperationException("not yet implemented");

    }

    @Override
    public String flushDB() {
        throw new UnsupportedOperationException("Not supported - use scatterFlushDB instead");

    }
    
    @Override
    public String flushAll() {
        throw new UnsupportedOperationException("Not supported - use scatterFlushAll instead");

    }

}
