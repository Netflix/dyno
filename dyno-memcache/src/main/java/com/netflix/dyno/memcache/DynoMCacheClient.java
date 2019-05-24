/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.memcache;


/**
 * Dyno client for Memcached that uses the {@link RollingMemcachedConnectionPoolImpl} for managing connections to {@link MemcachedClient}s
 * with local zone aware based RR load balancing and fallbacks to the remote zones
 *
 * @author poberai
 */
public class DynoMCacheClient { /**implements MemcachedClientIF {

 private final String cacheName;

 private final ConnectionPool<MemcachedClient> connPool;

 public DynoMCacheClient(String name, ConnectionPool<MemcachedClient> pool) {
 this.cacheName = name;
 this.connPool = pool;
 }

 private enum OpName {
 Add, Append, AsyncCas, AsyncDecr, AsyncGet, AsyncGetAndTouch, AsyncGets, AsyncIncr, Cas, Decr, Delete,
 Get, GetAndTouch, GetAsync, GetBulk, Gets, Incr, Prepend, Set, Touch
 }


 private abstract class BaseKeyOperation<T> implements Operation<MemcachedClient, T> {

 private final String key;
 private final OpName op;
 private BaseKeyOperation(final String k, final OpName o) {
 this.key = k;
 this.op = o;
 }
 @Override public String getName() {
 return op.name();
 }

 @Override public String getKey() {
 return key;
 }
 }

 private abstract class BaseAsyncKeyOperation<T> implements AsyncOperation<MemcachedClient, T> {

 private final String key;
 private final OpName op;
 private BaseAsyncKeyOperation(final String k, final OpName o) {
 this.key = k;
 this.op = o;
 }
 @Override public String getName() {
 return op.name();
 }

 @Override public String getKey() {
 return key;
 }
 }

 public String toString() {
 return this.cacheName;
 }

 public static class Builder {

 private String appName;
 private String clusterName;
 private ConnectionPoolConfigurationImpl cpConfig;

 public Builder(String name) {
 appName = name;
 }

 public Builder withDynomiteClusterName(String cluster) {
 clusterName = cluster;
 return this;
 }

 public Builder withConnectionPoolConfig(ConnectionPoolConfigurationImpl config) {
 cpConfig = config;
 return this;
 }

 public DynoMCacheClient build() {

 assert(appName != null);
 assert(clusterName != null);
 assert(cpConfig != null);

 // TODO: Add conn pool impl for MemcachedClient
 throw new RuntimeException("Dyno conn pool for Memcached Client NOT implemented. Coming soon.");
 }

 public static Builder withName(String name) {
 return new Builder(name);
 }
 }

 @Override public Collection<SocketAddress> getAvailableServers() {
 throw new RuntimeException("Not Implemented");
 }

 @Override public Collection<SocketAddress> getUnavailableServers() {
 throw new RuntimeException("Not Implemented");
 }

 @Override public Transcoder<Object> getTranscoder() {
 throw new RuntimeException("Not Implemented");
 }

 @Override public NodeLocator getNodeLocator() {
 throw new RuntimeException("Not Implemented");
 }

 @Override public Future<Boolean> append(final long cas, final String key, final Object val) {

 return new DecoratingFuture<Boolean>(connPool.executeAsync(new BaseAsyncKeyOperation<Boolean>(key, OpName.Append) {
 @Override public ListenableFuture<Boolean> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Boolean>(client.append(cas,  key, val));
 }
 }));
 }

 @Override public Future<Boolean> append(final String key, final Object val) {

 return new DecoratingFuture<Boolean>(connPool.executeAsync(new BaseAsyncKeyOperation<Boolean>(key, OpName.Append) {
 @Override public ListenableFuture<Boolean> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Boolean>(client.append(key, val));
 }
 }));
 }

 @Override public <T> Future<Boolean> append(final long cas, final String key, final T val, final Transcoder<T> tc) {

 return new DecoratingFuture<Boolean>(connPool.executeAsync(new BaseAsyncKeyOperation<Boolean>(key, OpName.Append) {
 @Override public ListenableFuture<Boolean> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Boolean>(client.append(cas,  key, val));
 }
 }));
 }

 @Override public <T> Future<Boolean> append(final String key, final T val, final Transcoder<T> tc) {

 return new DecoratingFuture<Boolean>(connPool.executeAsync(new BaseAsyncKeyOperation<Boolean>(key, OpName.Append) {
 @Override public ListenableFuture<Boolean> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Boolean>(client.append(key, val));
 }
 }));
 }

 @Override public Future<Boolean> prepend(final long cas, final String key, final Object val) {

 return new DecoratingFuture<Boolean>(connPool.executeAsync(new BaseAsyncKeyOperation<Boolean>(key, OpName.Prepend) {
 @Override public ListenableFuture<Boolean> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Boolean>(client.prepend(cas,  key, val));
 }
 }));
 }

 @Override public Future<Boolean> prepend(final String key, final Object val) {

 return new DecoratingFuture<Boolean>(connPool.executeAsync(new BaseAsyncKeyOperation<Boolean>(key, OpName.Prepend) {
 @Override public ListenableFuture<Boolean> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Boolean>(client.prepend(key, val));
 }
 }));
 }

 @Override public <T> Future<Boolean> prepend(final long cas, final String key, final T val, final Transcoder<T> tc) {

 return new DecoratingFuture<Boolean>(connPool.executeAsync(new BaseAsyncKeyOperation<Boolean>(key, OpName.Prepend) {
 @Override public ListenableFuture<Boolean> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Boolean>(client.prepend(cas,  key, val));
 }
 }));
 }

 @Override public <T> Future<Boolean> prepend(final String key, final T val, final Transcoder<T> tc) {

 return new DecoratingFuture<Boolean>(connPool.executeAsync(new BaseAsyncKeyOperation<Boolean>(key, OpName.Prepend) {
 @Override public ListenableFuture<Boolean> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Boolean>(client.prepend(key, val, tc));
 }
 }));
 }

 @Override public <T> Future<CASResponse> asyncCAS(final String key, final long casId, final T value, final Transcoder<T> tc) {

 return new DecoratingFuture<CASResponse>(connPool.executeAsync(new BaseAsyncKeyOperation<CASResponse>(key, OpName.AsyncCas) {
 @Override public ListenableFuture<CASResponse> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<CASResponse>(client.asyncCAS(key, casId, value, tc));
 }
 }));
 }

 @Override public Future<CASResponse> asyncCAS(final String key, final long casId, final Object value) {

 return new DecoratingFuture<CASResponse>(connPool.executeAsync(new BaseAsyncKeyOperation<CASResponse>(key, OpName.AsyncCas) {
 @Override public ListenableFuture<CASResponse> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<CASResponse>(client.asyncCAS(key, casId, value));
 }
 }));
 }

 @Override public Future<CASResponse> asyncCAS(final String key, final long casId, final int exp, final Object value) {

 return new DecoratingFuture<CASResponse>(connPool.executeAsync(new BaseAsyncKeyOperation<CASResponse>(key, OpName.Cas) {
 @Override public ListenableFuture<CASResponse> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<CASResponse>(client.asyncCAS(key, casId, exp, value));
 }
 }));
 }

 @Override public <T> CASResponse cas(final String key, final long casId, final int exp, final T value, final Transcoder<T> tc) {

 return connPool.executeWithFailover(new BaseKeyOperation<CASResponse>(key, OpName.Cas) {
 @Override public CASResponse execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.cas(key, casId, exp, value, tc);
 }

 }).getResult();
 }

 @Override public CASResponse cas(final String key, final long casId, final Object value) {

 return connPool.executeWithFailover(new BaseKeyOperation<CASResponse>(key, OpName.Cas) {
 @Override public CASResponse execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.cas(key, casId, value);
 }

 }).getResult();
 }

 @Override public CASResponse cas(final String key, final long casId, final int exp, final Object value) {

 return connPool.executeWithFailover(new BaseKeyOperation<CASResponse>(key, OpName.Cas) {
 @Override public CASResponse execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.cas(key, casId, exp, value);
 }

 }).getResult();
 }

 @Override public <T> Future<Boolean> add(final String key, final int exp, final T o, final Transcoder<T> tc) {

 return new DecoratingFuture<Boolean>(connPool.executeAsync(new BaseAsyncKeyOperation<Boolean>(key, OpName.Add) {
 @Override public ListenableFuture<Boolean> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Boolean>(client.add(key, exp, o));
 }
 }));
 }

 @Override public Future<Boolean> add(final String key, final int exp, final Object o) {

 return new DecoratingFuture<Boolean>(connPool.executeAsync(new BaseAsyncKeyOperation<Boolean>(key, OpName.Add) {
 @Override public ListenableFuture<Boolean> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Boolean>(client.add(key, exp, o));
 }
 }));
 }

 @Override public <T> Future<Boolean> set(final String key, final int exp, final T o, final Transcoder<T> tc) {

 return new DecoratingFuture<Boolean>(connPool.executeAsync(new BaseAsyncKeyOperation<Boolean>(key, OpName.Set) {
 @Override public ListenableFuture<Boolean> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Boolean>(client.set(key, exp, o, tc));
 }
 }));
 }

 @Override public Future<Boolean> set(final String key, final int exp, final Object o) {

 return new DecoratingFuture<Boolean>(connPool.executeAsync(new BaseAsyncKeyOperation<Boolean>(key, OpName.Set) {
 @Override public ListenableFuture<Boolean> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Boolean>(client.set(key, exp, o));
 }
 }));
 }

 @Override public <T> Future<Boolean> replace(final String key, final int exp, final T o, final Transcoder<T> tc) {

 return new DecoratingFuture<Boolean>(connPool.executeAsync(new BaseAsyncKeyOperation<Boolean>(key, OpName.Append) {
 @Override public ListenableFuture<Boolean> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Boolean>(client.replace(key, exp, o, tc));
 }
 }));
 }

 @Override public Future<Boolean> replace(final String key, final int exp, final Object o) {

 return new DecoratingFuture<Boolean>(connPool.executeAsync(new BaseAsyncKeyOperation<Boolean>(key, OpName.Append) {
 @Override public ListenableFuture<Boolean> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Boolean>(client.replace(key, exp, o));
 }
 }));
 }

 @Override public <T> Future<T> asyncGet(final String key, final Transcoder<T> tc) {

 return new DecoratingFuture<T>(connPool.executeAsync(new BaseAsyncKeyOperation<T>(key, OpName.AsyncGet) {
 @Override public ListenableFuture<T> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<T>(client.asyncGet(key, tc));
 }
 }));
 }

 @Override public Future<Object> asyncGet(final String key) {

 return new DecoratingFuture<Object>(connPool.executeAsync(new BaseAsyncKeyOperation<Object>(key, OpName.AsyncGet) {
 @Override public ListenableFuture<Object> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Object>(client.asyncGet(key));
 }
 }));
 }

 @Override public Future<CASValue<Object>> asyncGetAndTouch(final String key, final int exp) {

 return new DecoratingFuture<CASValue<Object>>(connPool.executeAsync(new BaseAsyncKeyOperation<CASValue<Object>>(key, OpName.AsyncGetAndTouch) {
 @Override public ListenableFuture<CASValue<Object>> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<CASValue<Object>>(client.asyncGets(key));
 }
 }));
 }

 @Override public <T> Future<CASValue<T>> asyncGetAndTouch(final String key, final int exp, final Transcoder<T> tc) {

 return new DecoratingFuture<CASValue<T>>(connPool.executeAsync(new BaseAsyncKeyOperation<CASValue<T>>(key, OpName.AsyncGetAndTouch) {
 @Override public ListenableFuture<CASValue<T>> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<CASValue<T>>(client.asyncGetAndTouch(key, exp, tc));
 }
 }));
 }

 @Override public <T> Future<CASValue<T>> asyncGets(final String key, final Transcoder<T> tc) {

 return new DecoratingFuture<CASValue<T>>(connPool.executeAsync(new BaseAsyncKeyOperation<CASValue<T>>(key, OpName.AsyncGets) {
 @Override public ListenableFuture<CASValue<T>> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<CASValue<T>>(client.asyncGets(key, tc));
 }
 }));
 }

 @Override public Future<CASValue<Object>> asyncGets(final String key) {

 return new DecoratingFuture<CASValue<Object>>(connPool.executeAsync(new BaseAsyncKeyOperation<CASValue<Object>>(key, OpName.AsyncGets) {
 @Override public ListenableFuture<CASValue<Object>> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<CASValue<Object>>(client.asyncGets(key));
 }
 }));
 }

 @Override public <T> CASValue<T> gets(final String key, final Transcoder<T> tc) {

 return connPool.executeWithFailover(new BaseKeyOperation<CASValue<T>>(key, OpName.Gets) {
 @Override public CASValue<T> execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.gets(key, tc);
 }

 }).getResult();
 }

 @Override public CASValue<Object> gets(final String key) {

 return connPool.executeWithFailover(new BaseKeyOperation<CASValue<Object>>(key, OpName.Gets) {
 @Override public CASValue<Object> execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.gets(key);
 }

 }).getResult();
 }

 @Override public <T> T get(final String key, final Transcoder<T> tc) {

 return connPool.executeWithFailover(new BaseKeyOperation<T>(key, OpName.Get) {
 @Override public T execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.get(key, tc);
 }

 }).getResult();
 }

 @Override public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keys, Iterator<Transcoder<T>> tcs) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys, Iterator<Transcoder<T>> tcs) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keys, Transcoder<T> tc) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys, Transcoder<T> tc) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public BulkFuture<Map<String, Object>> asyncGetBulk(Iterator<String> keys) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public BulkFuture<Map<String, Object>> asyncGetBulk(Collection<String> keys) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public <T> BulkFuture<Map<String, T>> asyncGetBulk(Transcoder<T> tc, String... keys) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public BulkFuture<Map<String, Object>> asyncGetBulk(String... keys) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public <T> Map<String, T> getBulk(Iterator<String> keys, Transcoder<T> tc) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public Map<String, Object> getBulk(Iterator<String> keys) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public <T> Future<Boolean> touch(final String key, final int exp, final Transcoder<T> tc) {

 return new DecoratingFuture<Boolean>(connPool.executeAsync(new BaseAsyncKeyOperation<Boolean>(key, OpName.Touch) {
 @Override public ListenableFuture<Boolean> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Boolean>(client.touch(key, exp, tc));
 }
 }));
 }

 @Override public <T> Future<Boolean> touch(final String key, final int exp) {

 return new DecoratingFuture<Boolean>(connPool.executeAsync(new BaseAsyncKeyOperation<Boolean>(key, OpName.Append) {
 @Override public ListenableFuture<Boolean> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Boolean>(client.touch(key, exp));
 }
 }));
 }

 @Override public Map<SocketAddress, String> getVersions() {
 throw new RuntimeException("Not Implemented");
 }

 @Override public Map<SocketAddress, Map<String, String>> getStats() {
 throw new RuntimeException("Not Implemented");
 }

 @Override public Map<SocketAddress, Map<String, String>> getStats(String prefix) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public long incr(final String key, final long by) {

 return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.Incr) {
 @Override public Long execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.incr(key, by);
 }

 }).getResult();
 }

 @Override public long incr(final String key, final int by) {
 return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.Incr) {
 @Override public Long execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.incr(key, by);
 }

 }).getResult();
 }

 @Override public long decr(final String key, final long by) {
 return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.Decr) {
 @Override public Long execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.decr(key, by);
 }

 }).getResult();
 }

 @Override public long decr(final String key, final int by) {
 return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.Decr) {
 @Override public Long execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.decr(key, by);
 }

 }).getResult();
 }

 @Override public long incr(final String key, final long by, final long def, final int exp) {
 return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.Incr) {
 @Override public Long execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.incr(key, by, def, exp);
 }

 }).getResult();
 }

 @Override public long incr(final String key, final int by, final long def, final int exp) {
 return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.Incr) {
 @Override public Long execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.incr(key, by, def, exp);
 }

 }).getResult();
 }

 @Override public long decr(final String key, final long by, final long def, final int exp) {
 return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.Decr) {
 @Override public Long execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.incr(key, by, def, exp);
 }

 }).getResult();
 }

 @Override public long decr(final String key, final int by, final long def, final int exp) {
 return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.Decr) {
 @Override public Long execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.incr(key, by, def, exp);
 }

 }).getResult();
 }

 @Override public Future<Long> asyncIncr(final String key, final long by) {

 return new DecoratingFuture<Long>(connPool.executeAsync(new BaseAsyncKeyOperation<Long>(key, OpName.AsyncIncr) {
 @Override public ListenableFuture<Long> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Long>(client.asyncIncr(key, by));
 }
 }));
 }

 @Override public Future<Long> asyncIncr(final String key, final int by) {

 return new DecoratingFuture<Long>(connPool.executeAsync(new BaseAsyncKeyOperation<Long>(key, OpName.AsyncIncr) {
 @Override public ListenableFuture<Long> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Long>(client.asyncIncr(key, by));
 }
 }));
 }

 @Override public Future<Long> asyncDecr(final String key, final long by) {

 return new DecoratingFuture<Long>(connPool.executeAsync(new BaseAsyncKeyOperation<Long>(key, OpName.AsyncDecr) {
 @Override public ListenableFuture<Long> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Long>(client.asyncDecr(key, by));
 }
 }));
 }

 @Override public Future<Long> asyncDecr(final String key, final int by) {

 return new DecoratingFuture<Long>(connPool.executeAsync(new BaseAsyncKeyOperation<Long>(key, OpName.AsyncDecr) {
 @Override public ListenableFuture<Long> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Long>(client.asyncDecr(key, by));
 }
 }));
 }

 @Override public long incr(final String key, final long by, final long def) {
 return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.Incr) {
 @Override public Long execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.incr(key, by, def);
 }

 }).getResult();
 }

 @Override public long incr(final String key, final int by, final long def) {
 return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.Incr) {
 @Override public Long execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.incr(key, by, def);
 }

 }).getResult();
 }

 @Override public long decr(final String key, final long by, final long def) {
 return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.Decr) {
 @Override public Long execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.decr(key, by, def);
 }

 }).getResult();
 }

 @Override public long decr(final String key, final int by, final long def) {
 return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.Decr) {
 @Override public Long execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.decr(key, by, def);
 }

 }).getResult();
 }

 @Override public Future<Boolean> delete(final String key, final long cas) {

 return new DecoratingFuture<Boolean>(connPool.executeAsync(new BaseAsyncKeyOperation<Boolean>(key, OpName.Delete) {
 @Override public ListenableFuture<Boolean> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Boolean>(client.delete(key, cas));
 }
 }));
 }

 @Override public Future<Boolean> flush(int delay) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public Future<Boolean> flush() {
 throw new RuntimeException("Not Implemented");
 }

 @Override public void shutdown() {
 throw new RuntimeException("Not Implemented");
 }

 @Override public boolean shutdown(long timeout, TimeUnit unit) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public boolean waitForQueues(long timeout, TimeUnit unit) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public boolean addObserver(ConnectionObserver obs) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public boolean removeObserver(ConnectionObserver obs) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public Set<String> listSaslMechanisms() {
 throw new RuntimeException("Not Implemented");
 }

 @Override public CASValue<Object> getAndTouch(final String key, final int exp) {
 return connPool.executeWithFailover(new BaseKeyOperation<CASValue<Object>>(key, OpName.GetAndTouch) {
 @Override public CASValue<Object> execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.getAndTouch(key, exp);
 }

 }).getResult();
 }

 @Override public <T> CASValue<T> getAndTouch(final String key, final int exp, final Transcoder<T> tc) {
 return connPool.executeWithFailover(new BaseKeyOperation<CASValue<T>>(key, OpName.GetAndTouch) {
 @Override public CASValue<T> execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.getAndTouch(key, exp ,tc);
 }

 }).getResult();
 }

 @Override public Object get(final String key) {
 return connPool.executeWithFailover(new BaseKeyOperation<Object>(key, OpName.Get) {
 @Override public Object execute(final MemcachedClient client, ConnectionContext state) throws DynoException {
 return client.get(key);
 }

 }).getResult();
 }

 @Override public <T> Map<String, T> getBulk(Collection<String> keys, Transcoder<T> tc) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public Map<String, Object> getBulk(Collection<String> keys) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public <T> Map<String, T> getBulk(Transcoder<T> tc, String... keys) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public Map<String, Object> getBulk(String... keys) {
 throw new RuntimeException("Not Implemented");
 }

 @Override public Future<Boolean> delete(final String key) {

 return new DecoratingFuture<Boolean>(connPool.executeAsync(new BaseAsyncKeyOperation<Boolean>(key, OpName.Delete) {
 @Override public ListenableFuture<Boolean> executeAsync(MemcachedClient client) throws DynoException {
 return new DecoratingListenableFuture<Boolean>(client.delete(key));
 }
 }));
 }
 */
}