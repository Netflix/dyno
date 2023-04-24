/*******************************************************************************
 * Copyright 2011 Netflix
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.dyno.jedis;

import com.netflix.dyno.connectionpool.AsyncOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.ConnectionFactory;
import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostConnectionPool;
import com.netflix.dyno.connectionpool.ListenableFuture;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.OperationMonitor;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.exception.DynoConnectException;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.FatalConnectionException;
import com.netflix.dyno.connectionpool.impl.ConnectionContextImpl;
import com.netflix.dyno.connectionpool.impl.OperationResultImpl;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.util.Sharded;

import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;
import java.util.concurrent.TimeUnit;

public class JedisConnectionFactory implements ConnectionFactory<Jedis> {

    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(JedisConnectionFactory.class);

    private final OperationMonitor opMonitor;
    private final SSLSocketFactory sslSocketFactory;

    public JedisConnectionFactory(OperationMonitor monitor, SSLSocketFactory sslSocketFactory) {
        this.opMonitor = monitor;
        this.sslSocketFactory = sslSocketFactory;
    }

    @Override
    public Connection<Jedis> createConnection(HostConnectionPool<Jedis> pool)
            throws DynoConnectException {
        return new JedisConnection(pool);
    }

    @Override
    public Connection<Jedis> createConnectionWithDataStore(HostConnectionPool<Jedis> pool)
            throws DynoConnectException {
        return new JedisConnection(pool, true);
    }

    @Override
    public Connection<Jedis> createConnectionWithConsistencyLevel(HostConnectionPool<Jedis> pool, String consistency) {
        JedisConnection connection = new JedisConnection(pool);
        connection.setConsistencyLevel(consistency);
        return connection;
    }

    // TODO: raghu compose redisconnection with jedisconnection in it
    public class JedisConnection implements Connection<Jedis> {

        private final HostConnectionPool<Jedis> hostPool;
        private final Jedis jedisClient;
        private final ConnectionContextImpl context = new ConnectionContextImpl();
        private String consistencyLevel;

        private DynoConnectException lastDynoException;

        public JedisConnection(HostConnectionPool<Jedis> hostPool) {
            this(hostPool, false);
        }

        public JedisConnection(HostConnectionPool<Jedis> hostPool, boolean connectDataStore) {
            this.hostPool = hostPool;
            Host host = hostPool.getHost();

            int port = connectDataStore ? host.getDatastorePort() : host.getPort();

            if (sslSocketFactory == null) {
                JedisShardInfo shardInfo = new JedisShardInfo(host.getHostAddress(), port,
                        hostPool.getConnectionTimeout(), hostPool.getSocketTimeout(), Sharded.DEFAULT_WEIGHT);
                shardInfo.setPassword(host.getPassword());

                jedisClient = new Jedis(shardInfo);
            } else {
                JedisShardInfo shardInfo = new JedisShardInfo(host.getHostAddress(), port,
                        hostPool.getConnectionTimeout(), hostPool.getSocketTimeout(), Sharded.DEFAULT_WEIGHT,
                        true, sslSocketFactory, new SSLParameters(), null);
                shardInfo.setPassword(host.getPassword());

                jedisClient = new Jedis(shardInfo);
            }
        }

        public void setConsistencyLevel(String consistency) {
            this.consistencyLevel = consistency;
        }

        public boolean isConsistencyLevelProvided() {
            return this.consistencyLevel != null;
        }

        @Override
        public <R> OperationResult<R> execute(Operation<Jedis, R> op) throws DynoException {

            long startTime = System.nanoTime() / 1000;
            String opName = op.getName();

            OperationResultImpl<R> opResult = null;

            try {
                R result = op.execute(jedisClient, context);
                if (context.hasMetadata("compression") || context.hasMetadata("decompression")) {
                    opMonitor.recordSuccess(opName, true);
                } else {
                    opMonitor.recordSuccess(opName);
                }
                opResult = new OperationResultImpl<R>(opName, result, opMonitor);
                opResult.addMetadata("connectionId", String.valueOf(this.hashCode()));
                return opResult;

            } catch (JedisConnectionException ex) {
                Logger.warn("Caught JedisConnectionException: " + ex.getMessage());
                opMonitor.recordFailure(opName, ex.getMessage());
                lastDynoException = (DynoConnectException) new FatalConnectionException(ex).setAttempt(1).setHost(this.getHost());
                throw lastDynoException;

            } catch (RuntimeException ex) {
                Logger.warn("Caught RuntimeException: " + ex.getMessage());
                opMonitor.recordFailure(opName, ex.getMessage());
                lastDynoException = (DynoConnectException) new FatalConnectionException(ex).setAttempt(1).setHost(this.getHost());
                throw lastDynoException;

            } finally {
                long duration = System.nanoTime() / 1000 - startTime;
                if (opResult != null) {
                    opResult.setLatency(duration, TimeUnit.MICROSECONDS);
                }
            }
        }

        @Override
        public <R> ListenableFuture<OperationResult<R>> executeAsync(AsyncOperation<Jedis, R> op) throws DynoException {
            throw new NotImplementedException("Not Implemented");
        }

        @Override
        public void close() {
            jedisClient.quit();
            jedisClient.disconnect();
        }

        @Override
        public Host getHost() {
            return hostPool.getHost();
        }

        @Override
        public void open() throws DynoException {
            jedisClient.connect();
            if (isConsistencyLevelProvided()) {
                jedisClient.getClient().sendCommand(DynoConfigCommand.CONN_CONSISTENCY, this.consistencyLevel);
                jedisClient.getClient().getStatusCodeReply();
            }
        }

        @Override
        public DynoConnectException getLastException() {
            return lastDynoException;
        }

        @Override
        public HostConnectionPool<Jedis> getParentConnectionPool() {
            return hostPool;
        }

        @Override
        public void execPing() {
            final String result;

            try {
                result = jedisClient.ping();
            } catch (JedisConnectionException e) {
                throw new DynoConnectException("Unsuccessful ping", e);
            }

            if (result == null || result.isEmpty()) {
                throw new DynoConnectException("Unsuccessful ping, got empty result");
            }
        }

        @Override
        public ConnectionContext getContext() {
            return context;
        }

        public Jedis getClient() {
            return jedisClient;
        }
    }
}
