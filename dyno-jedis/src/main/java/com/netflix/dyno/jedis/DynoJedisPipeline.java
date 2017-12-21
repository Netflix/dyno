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

import com.netflix.dyno.connectionpool.*;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.exception.FatalConnectionException;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import com.netflix.dyno.connectionpool.impl.utils.ZipUtils;
import com.netflix.dyno.jedis.JedisConnectionFactory.JedisConnection;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.params.geo.GeoRadiusParam;
import redis.clients.jedis.params.sortedset.ZAddParams;
import redis.clients.jedis.params.sortedset.ZIncrByParams;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.netflix.dyno.connectionpool.ConnectionPoolConfiguration.CompressionStrategy;

@NotThreadSafe
public class DynoJedisPipeline implements RedisPipeline, BinaryRedisPipeline, AutoCloseable {

	private static final Logger Logger = LoggerFactory.getLogger(DynoJedisPipeline.class);

	// ConnPool and connection to exec the pipeline
	private final ConnectionPoolImpl<Jedis> connPool;
	private volatile Connection<Jedis> connection;
	private final DynoJedisPipelineMonitor opMonitor;
	private final ConnectionPoolMonitor cpMonitor;

	// the cached pipeline
	private volatile Pipeline jedisPipeline = null;
	// the cached row key for the pipeline. all subsequent requests to pipeline
	// must be the same. this is used to check that.
	private final AtomicReference<String> theKey = new AtomicReference<String>(null);
	private final AtomicReference<byte[]> theBinaryKey = new AtomicReference<byte[]>(null);
	private final AtomicReference<String> hashtag = new AtomicReference<String>(null);
	// used for tracking errors
	private final AtomicReference<DynoException> pipelineEx = new AtomicReference<DynoException>(null);

	private static final String DynoPipeline = "DynoPipeline";

	DynoJedisPipeline(ConnectionPoolImpl<Jedis> cPool, DynoJedisPipelineMonitor operationMonitor,
			ConnectionPoolMonitor connPoolMonitor) {
		this.connPool = cPool;
		this.opMonitor = operationMonitor;
		this.cpMonitor = connPoolMonitor;
	}

	private void pipelined(final byte[] key) {
		try {
			try {
				connection = connPool.getConnectionForOperation(new BaseOperation<Jedis, String>() {

					@Override
					public String getName() {
						return DynoPipeline;
					}

					@Override
					public String getStringKey() {// we do not use it in this context
						return null;
					}

					@Override
					public byte[] getBinaryKey() {
						return key;
					}

				});
			} catch (NoAvailableHostsException nahe) {
				cpMonitor.incOperationFailure(connection != null ? connection.getHost() : null, nahe);
				discardPipelineAndReleaseConnection();
				throw nahe;
			}
		} catch (NoAvailableHostsException nahe) {
			cpMonitor.incOperationFailure(connection != null ? connection.getHost() : null, nahe);
			discardPipelineAndReleaseConnection();
			throw nahe;
		}
		Jedis jedis = ((JedisConnection) connection).getClient();
		jedisPipeline = jedis.pipelined();
		cpMonitor.incOperationSuccess(connection.getHost(), 0);
	}

	private void pipelined(final String key) {
		try {
			try {
				connection = connPool.getConnectionForOperation(new BaseOperation<Jedis, String>() {

					@Override
					public String getName() {
						return DynoPipeline;
					}

					@Override
					public String getStringKey() {
						return key;
					}

					@Override
					public byte[] getBinaryKey() { // we do not use it in this context
						return null;
					}

				});
			} catch (NoAvailableHostsException nahe) {
				cpMonitor.incOperationFailure(connection != null ? connection.getHost() : null, nahe);
				discardPipelineAndReleaseConnection();
				throw nahe;
			}
		} catch (NoAvailableHostsException nahe) {
			cpMonitor.incOperationFailure(connection != null ? connection.getHost() : null, nahe);
			discardPipelineAndReleaseConnection();
			throw nahe;
		}
		Jedis jedis = ((JedisConnection) connection).getClient();
		jedisPipeline = jedis.pipelined();
		cpMonitor.incOperationSuccess(connection.getHost(), 0);
	}

	private void checkHashtag(final String key, final String hashtagValue) {
		if (this.hashtag.get() != null) {
			verifyHashtagValue(hashtagValue);
		} else {
			boolean success = this.hashtag.compareAndSet(null, hashtagValue);
			if (!success) {
				verifyHashtagValue(hashtagValue);
			} else {
				pipelined(key);
			}
		}

	}

	/**
	 * Checks that a pipeline is associated with a single key. Binary keys do not
	 * support hashtags.
	 * 
	 * @param key
	 */
	private void checkKey(final byte[] key) {
		if (theBinaryKey.get() != null) {
			verifyKey(key);
		} else {
			boolean success = theBinaryKey.compareAndSet(null, key);
			if (!success) {
				// someone already beat us to it. that's fine, just verify
				// that the key is the same
				verifyKey(key);
			} else {
				pipelined(key);
			}
		}
	}

	/**
	 * Checks that a pipeline is associated with a single key. If there is a hashtag
	 * defined in the first host of the connectionpool then we check that first.
	 * 
	 * @param key
	 */
	private void checkKey(final String key) {

		/*
		 * Get hashtag from the first host of the active pool We cannot use the
		 * connection object because as of now we have not selected a connection. A
		 * connection is selected based on the key or hashtag respectively.
		 */
		String hashtag = connPool.getConfiguration().getHashtag();
		if (hashtag == null || hashtag.isEmpty()) {
			if (theKey.get() != null) {
				verifyKey(key);
			} else {
				boolean success = theKey.compareAndSet(null, key);
				if (!success) {
					// someone already beat us to it. that's fine, just verify
					// that the key is the same
					verifyKey(key);
				} else {
					pipelined(key);
				}
			}
		} else {
			/*
			 * We have a identified a hashtag in the Host object. That means Dynomite has a
			 * defined hashtag. Producing the hashvalue out of the hashtag and using that as
			 * a reference to the pipeline
			 */
			String hashValue = StringUtils.substringBetween(key, Character.toString(hashtag.charAt(0)),
					Character.toString(hashtag.charAt(1)));
			checkHashtag(key, hashValue);
		}
	}

	/**
	 * Verifies binary key with pipeline binary key
	 */
	private void verifyKey(final byte[] key) {
		if (!theBinaryKey.get().equals(key)) {
			try {
				throw new RuntimeException("Must have same key for Redis Pipeline in Dynomite. This key: " + key);
			} finally {
				discardPipelineAndReleaseConnection();
			}
		}
	}

	/**
	 * Verifies key with pipeline key
	 */
	private void verifyKey(final String key) {

		if (!theKey.get().equals(key)) {
			try {
				throw new RuntimeException("Must have same key for Redis Pipeline in Dynomite. This key: " + key);
			} finally {
				discardPipelineAndReleaseConnection();
			}
		}
	}

	private void verifyHashtagValue(final String hashtagValue) {

		if (!this.hashtag.get().equals(hashtagValue)) {
			try {
				throw new RuntimeException(
						"Must have same hashtag for Redis Pipeline in Dynomite. This hashvalue: " + hashtagValue);
			} finally {
				discardPipelineAndReleaseConnection();
			}
		}
	}

	private String decompressValue(String value) {
		try {
			if (ZipUtils.isCompressed(value)) {
				return ZipUtils.decompressFromBase64String(value);
			}
		} catch (IOException e) {
			Logger.warn("Unable to decompress value [" + value + "]");
		}

		return value;
	}

	private byte[] decompressValue(byte[] value) {
		try {
			if (ZipUtils.isCompressed(value)) {
				return ZipUtils.decompressBytesNonBase64(value);
			}
		} catch (IOException e) {
			Logger.warn("Unable to decompress byte array value [" + value + "]");
		}
		return value;
	}

	/**
	 * As long as jdk 7 and below is supported we need to define our own function
	 * interfaces
	 */
	private interface Func0<R> {
		R call();
	}

	public class PipelineResponse extends Response<String> {

		private Response<String> response;

		public PipelineResponse(Builder<String> b) {
			super(BuilderFactory.STRING);
		}

		public PipelineResponse apply(Func0<? extends Response<String>> f) {
			this.response = f.call();
			return this;
		}

		@Override
		public String get() {
			return decompressValue(response.get());
		}

	}

	public class PipelineLongResponse extends Response<Long> {
		private Response<Long> response;

		public PipelineLongResponse(Builder<Long> b) {
			super(b);
		}

		public PipelineLongResponse apply(Func0<? extends Response<Long>> f) {
			this.response = f.call();
			return this;
		}
	}

	public class PipelineListResponse extends Response<List<String>> {

		private Response<List<String>> response;

		public PipelineListResponse(Builder<List> b) {
			super(BuilderFactory.STRING_LIST);
		}

		public PipelineListResponse apply(Func0<? extends Response<List<String>>> f) {
			this.response = f.call();
			return this;
		}

		@Override
		public List<String> get() {
			return new ArrayList<String>(
					CollectionUtils.transform(response.get(), new CollectionUtils.Transform<String, String>() {
						@Override
						public String get(String s) {
							return decompressValue(s);
						}
					}));
		}
	}

	public class PipelineBinaryResponse extends Response<byte[]> {

		private Response<byte[]> response;

		public PipelineBinaryResponse(Builder<String> b) {
			super(BuilderFactory.BYTE_ARRAY);
		}

		public PipelineBinaryResponse apply(Func0<? extends Response<byte[]>> f) {
			this.response = f.call();
			return this;
		}

		@Override
		public byte[] get() {
			return decompressValue(response.get());
		}

	}

	public class PipelineMapResponse extends Response<Map<String, String>> {

		private Response<Map<String, String>> response;

		public PipelineMapResponse(Builder<Map<String, String>> b) {
			super(BuilderFactory.STRING_MAP);
		}

		@Override
		public Map<String, String> get() {
			return CollectionUtils.transform(response.get(),
					new CollectionUtils.MapEntryTransform<String, String, String>() {
						@Override
						public String get(String key, String val) {
							return decompressValue(val);
						}
					});
		}
	}

	public class PipelineBinaryMapResponse extends Response<Map<byte[], byte[]>> {

		private Response<Map<byte[], byte[]>> response;

		public PipelineBinaryMapResponse(Builder<Map<byte[], byte[]>> b) {
			super(BuilderFactory.BYTE_ARRAY_MAP);
		}

		public PipelineBinaryMapResponse apply(Func0<? extends Response<Map<byte[], byte[]>>> f) {
			this.response = f.call();
			return this;
		}

		@Override
		public Map<byte[], byte[]> get() {
			return CollectionUtils.transform(response.get(),
					new CollectionUtils.MapEntryTransform<byte[], byte[], byte[]>() {
						@Override
						public byte[] get(byte[] key, byte[] val) {
							return decompressValue(val);
						}
					});
		}

	}

	private abstract class PipelineOperation<R> {

		abstract Response<R> execute(Pipeline jedisPipeline) throws DynoException;

		Response<R> execute(final byte[] key, final OpName opName) {
			checkKey(key);
			return execute(key, opName);
		}

		Response<R> execute(final String key, final OpName opName) {
			checkKey(key);
			return executeOperation(opName);
		}

		Response<R> executeOperation(final OpName opName) {
			try {
				opMonitor.recordOperation(opName.name());
				return execute(jedisPipeline);

			} catch (JedisConnectionException ex) {
				handleConnectionException(ex);
				throw ex;
			}
		}

		void handleConnectionException(JedisConnectionException ex) {
			DynoException e = new FatalConnectionException(ex).setAttempt(1);
			pipelineEx.set(e);
			cpMonitor.incOperationFailure(connection.getHost(), e);
		}
	}

	private abstract class PipelineCompressionOperation<R> extends PipelineOperation<R> {

		/**
		 * Compresses the value based on the threshold defined by
		 * {@link ConnectionPoolConfiguration#getValueCompressionThreshold()}
		 *
		 * @param value
		 * @return
		 */
		public String compressValue(String value) {
			String result = value;
			int thresholdBytes = connPool.getConfiguration().getValueCompressionThreshold();

			try {
				// prefer speed over accuracy here so rather than using
				// getBytes() to get the actual size
				// just estimate using 2 bytes per character
				if ((2 * value.length()) > thresholdBytes) {
					result = ZipUtils.compressStringToBase64String(value);
				}
			} catch (IOException e) {
				Logger.warn("UNABLE to compress [" + value + "]; sending value uncompressed");
			}

			return result;
		}

		public byte[] compressValue(byte[] value) {
			int thresholdBytes = connPool.getConfiguration().getValueCompressionThreshold();

			if (value.length > thresholdBytes) {
				try {
					return ZipUtils.compressBytesNonBase64(value);
				} catch (IOException e) {
					Logger.warn("UNABLE to compress byte array [" + value + "]; sending value uncompressed");
				}
			}

			return value;
		}

	}

	@Override
	public Response<Long> append(final String key, final String value) {

		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.append(key, value);
			}

		}.execute(key, OpName.APPEND);
	}

	@Override
	public Response<List<String>> blpop(final String arg) {

		return new PipelineOperation<List<String>>() {

			@Override
			Response<List<String>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.blpop(arg);
			}
		}.execute(arg, OpName.BLPOP);

	}

	@Override
	public Response<List<String>> brpop(final String arg) {
		return new PipelineOperation<List<String>>() {

			@Override
			Response<List<String>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.brpop(arg);
			}
		}.execute(arg, OpName.BRPOP);

	}

	@Override
	public Response<Long> decr(final String key) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.decr(key);
			}
		}.execute(key, OpName.DECR);
	}

	@Override
	public Response<Long> decrBy(final String key, final long integer) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.decrBy(key, integer);
			}
		}.execute(key, OpName.DECRBY);
	}

	@Override
	public Response<Long> del(final String key) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.del(key);
			}
		}.execute(key, OpName.DEL);

	}

	@Override
	public Response<String> echo(final String string) {
		return new PipelineOperation<String>() {

			@Override
			Response<String> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.echo(string);
			}
		}.execute(string, OpName.ECHO);

	}

	@Override
	public Response<Boolean> exists(final String key) {
		return new PipelineOperation<Boolean>() {

			@Override
			Response<Boolean> execute(final Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.exists(key);
			}
		}.execute(key, OpName.EXISTS);
	}

	@Override
	public Response<Long> expire(final String key, final int seconds) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(final Pipeline jedisPipeline) throws DynoException {
				long startTime = System.nanoTime() / 1000;
				try {
					return jedisPipeline.expire(key, seconds);
				} finally {
					long duration = System.nanoTime() / 1000 - startTime;
					opMonitor.recordSendLatency(OpName.EXPIRE.name(), duration, TimeUnit.MICROSECONDS);
				}
			}
		}.execute(key, OpName.EXPIRE);

	}

	@Override
	public Response<Long> pexpire(String key, long milliseconds) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> expireAt(final String key, final long unixTime) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(final Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.expireAt(key, unixTime);
			}
		}.execute(key, OpName.EXPIREAT);

	}

	@Override
	public Response<Long> pexpireAt(String key, long millisecondsTimestamp) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<String> get(final String key) {
		if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
			return new PipelineOperation<String>() {
				@Override
				Response<String> execute(Pipeline jedisPipeline) throws DynoException {
					long startTime = System.nanoTime() / 1000;
					try {
						return jedisPipeline.get(key);
					} finally {
						long duration = System.nanoTime() / 1000 - startTime;
						opMonitor.recordSendLatency(OpName.GET.name(), duration, TimeUnit.MICROSECONDS);
					}
				}
			}.execute(key, OpName.GET);
		} else {
			return new PipelineCompressionOperation<String>() {
				@Override
				Response<String> execute(final Pipeline jedisPipeline) throws DynoException {
					return new PipelineResponse(null).apply(new Func0<Response<String>>() {
						@Override
						public Response<String> call() {
							return jedisPipeline.get(key);
						}
					});
				}
			}.execute(key, OpName.GET);
		}

	}

	@Override
	public Response<Boolean> getbit(final String key, final long offset) {
		return new PipelineOperation<Boolean>() {

			@Override
			Response<Boolean> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.getbit(key, offset);
			}
		}.execute(key, OpName.GETBIT);

	}

	@Override
	public Response<String> getrange(final String key, final long startOffset, final long endOffset) {
		return new PipelineOperation<String>() {

			@Override
			Response<String> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.getrange(key, startOffset, endOffset);
			}
		}.execute(key, OpName.GETRANGE);

	}

	@Override
	public Response<String> getSet(final String key, final String value) {
		if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
			return new PipelineOperation<String>() {
				@Override
				Response<String> execute(Pipeline jedisPipeline) throws DynoException {
					return jedisPipeline.getSet(key, value);
				}
			}.execute(key, OpName.GETSET);
		} else {
			return new PipelineCompressionOperation<String>() {
				@Override
				Response<String> execute(final Pipeline jedisPipeline) throws DynoException {
					return new PipelineResponse(null).apply(new Func0<Response<String>>() {
						@Override
						public Response<String> call() {
							return jedisPipeline.getSet(key, compressValue(value));
						}
					});
				}
			}.execute(key, OpName.GETSET);
		}
	}

	@Override
	public Response<Long> hdel(final String key, final String... field) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.hdel(key, field);
			}
		}.execute(key, OpName.HDEL);

	}

	@Override
	public Response<Boolean> hexists(final String key, final String field) {
		return new PipelineOperation<Boolean>() {

			@Override
			Response<Boolean> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.hexists(key, field);
			}
		}.execute(key, OpName.HEXISTS);

	}

	@Override
	public Response<String> hget(final String key, final String field) {
		if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
			return new PipelineOperation<String>() {
				@Override
				Response<String> execute(Pipeline jedisPipeline) throws DynoException {
					return jedisPipeline.hget(key, field);
				}
			}.execute(key, OpName.HGET);
		} else {
			return new PipelineCompressionOperation<String>() {
				@Override
				Response<String> execute(final Pipeline jedisPipeline) throws DynoException {
					return new PipelineResponse(null).apply(new Func0<Response<String>>() {
						@Override
						public Response<String> call() {
							return jedisPipeline.hget(key, field);
						}
					});
				}
			}.execute(key, OpName.HGET);
		}

	}

	/**
	 * This method is a BinaryRedisPipeline command which dyno does not yet properly
	 * support, therefore the interface is not yet implemented.
	 */
	public Response<byte[]> hget(final byte[] key, final byte[] field) {
		if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
			return new PipelineOperation<byte[]>() {
				@Override
				Response<byte[]> execute(Pipeline jedisPipeline) throws DynoException {
					return jedisPipeline.hget(key, field);
				}
			}.execute(key, OpName.HGET);
		} else {
			return new PipelineCompressionOperation<byte[]>() {
				@Override
				Response<byte[]> execute(final Pipeline jedisPipeline) throws DynoException {
					return new PipelineBinaryResponse(null).apply(new Func0<Response<byte[]>>() {
						@Override
						public Response<byte[]> call() {
							return jedisPipeline.hget(key, field);
						}
					});
				}
			}.execute(key, OpName.HGET);
		}
	}

	@Override
	public Response<Map<String, String>> hgetAll(final String key) {

		return new PipelineOperation<Map<String, String>>() {

			@Override
			Response<Map<String, String>> execute(Pipeline jedisPipeline) throws DynoException {
				long startTime = System.nanoTime() / 1000;
				try {
					return jedisPipeline.hgetAll(key);
				} finally {
					long duration = System.nanoTime() / 1000 - startTime;
					opMonitor.recordSendLatency(OpName.HGETALL.name(), duration, TimeUnit.MICROSECONDS);
				}
			}

		}.execute(key, OpName.HGETALL);

	}

	public Response<Map<byte[], byte[]>> hgetAll(final byte[] key) {
		if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
			return new PipelineOperation<Map<byte[], byte[]>>() {
				@Override
				Response<Map<byte[], byte[]>> execute(Pipeline jedisPipeline) throws DynoException {
					long startTime = System.nanoTime() / 1000;
					try {
						return jedisPipeline.hgetAll(key);
					} finally {
						long duration = System.nanoTime() / 1000 - startTime;
						opMonitor.recordSendLatency(OpName.HGETALL.name(), duration, TimeUnit.MICROSECONDS);
					}
				}
			}.execute(key, OpName.HGETALL);
		} else {
			return new PipelineCompressionOperation<Map<byte[], byte[]>>() {
				@Override
				Response<Map<byte[], byte[]>> execute(final Pipeline jedisPipeline) throws DynoException {
					return new PipelineBinaryMapResponse(null).apply(new Func0<Response<Map<byte[], byte[]>>>() {
						@Override
						public Response<Map<byte[], byte[]>> call() {
							return jedisPipeline.hgetAll(key);
						}
					});
				}
			}.execute(key, OpName.HGETALL);
		}
	}

	@Override
	public Response<Long> hincrBy(final String key, final String field, final long value) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.hincrBy(key, field, value);
			}
		}.execute(key, OpName.HINCRBY);
	}

	/* not supported by RedisPipeline 2.7.3 */
	public Response<Double> hincrByFloat(final String key, final String field, final double value) {
		return new PipelineOperation<Double>() {

			@Override
			Response<Double> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.hincrByFloat(key, field, value);
			}
		}.execute(key, OpName.HINCRBYFLOAT);
	}

	@Override
	public Response<Set<String>> hkeys(final String key) {
		return new PipelineOperation<Set<String>>() {

			@Override
			Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.hkeys(key);
			}
		}.execute(key, OpName.HKEYS);

	}

	public Response<ScanResult<Map.Entry<String, String>>> hscan(final String key, int cursor) {
		throw new UnsupportedOperationException("'HSCAN' cannot be called in pipeline");
	}

	@Override
	public Response<Long> hlen(final String key) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.hlen(key);
			}
		}.execute(key, OpName.HLEN);

	}

	/**
	 * This method is a BinaryRedisPipeline command which dyno does not yet properly
	 * support, therefore the interface is not yet implemented.
	 */
	public Response<List<byte[]>> hmget(final byte[] key, final byte[]... fields) {
		return new PipelineOperation<List<byte[]>>() {

			@Override
			Response<List<byte[]>> execute(Pipeline jedisPipeline) throws DynoException {
				long startTime = System.nanoTime() / 1000;
				try {
					return jedisPipeline.hmget(key, fields);
				} finally {
					long duration = System.nanoTime() / 1000 - startTime;
					opMonitor.recordSendLatency(OpName.HMGET.name(), duration, TimeUnit.MICROSECONDS);
				}
			}
		}.execute(key, OpName.HMGET);
	}

	@Override
	public Response<List<String>> hmget(final String key, final String... fields) {
		if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
			return new PipelineOperation<List<String>>() {
				@Override
				Response<List<String>> execute(Pipeline jedisPipeline) throws DynoException {
					long startTime = System.nanoTime() / 1000;
					try {
						return jedisPipeline.hmget(key, fields);
					} finally {
						long duration = System.nanoTime() / 1000 - startTime;
						opMonitor.recordSendLatency(OpName.HMGET.name(), duration, TimeUnit.MICROSECONDS);
					}
				}
			}.execute(key, OpName.HMGET);
		} else {
			return new PipelineCompressionOperation<List<String>>() {
				@Override
				Response<List<String>> execute(final Pipeline jedisPipeline) throws DynoException {
					long startTime = System.nanoTime() / 1000;
					try {
						return new PipelineListResponse(null).apply(new Func0<Response<List<String>>>() {
							@Override
							public Response<List<String>> call() {
								return jedisPipeline.hmget(key, fields);
							}
						});
					} finally {
						long duration = System.nanoTime() / 1000 - startTime;
						opMonitor.recordSendLatency(OpName.HMGET.name(), duration, TimeUnit.MICROSECONDS);
					}
				}
			}.execute(key, OpName.HGET);
		}
	}

	/**
	 * This method is a BinaryRedisPipeline command which dyno does not yet properly
	 * support, therefore the interface is not yet implemented since only a few
	 * binary commands are present.
	 */
	public Response<String> hmset(final byte[] key, final Map<byte[], byte[]> hash) {
		if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
			return new PipelineOperation<String>() {
				@Override
				Response<String> execute(Pipeline jedisPipeline) throws DynoException {
					long startTime = System.nanoTime() / 1000;
					try {
						return jedisPipeline.hmset(key, hash);
					} finally {
						long duration = System.nanoTime() / 1000 - startTime;
						opMonitor.recordSendLatency(OpName.HMSET.name(), duration, TimeUnit.MICROSECONDS);
					}
				}
			}.execute(key, OpName.HMSET);
		} else {
			return new PipelineCompressionOperation<String>() {
				@Override
				Response<String> execute(final Pipeline jedisPipeline) throws DynoException {
					return new PipelineResponse(null).apply(new Func0<Response<String>>() {
						@Override
						public Response<String> call() {
							return jedisPipeline.hmset(key, CollectionUtils.transform(hash,
									new CollectionUtils.MapEntryTransform<byte[], byte[], byte[]>() {
										@Override
										public byte[] get(byte[] key, byte[] val) {
											return compressValue(val);
										}
									}));
						}
					});
				}
			}.execute(key, OpName.HMSET);
		}
	}

	@Override
	public Response<String> hmset(final String key, final Map<String, String> hash) {
		if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
			return new PipelineOperation<String>() {
				@Override
				Response<String> execute(Pipeline jedisPipeline) throws DynoException {
					long startTime = System.nanoTime() / 1000;
					try {
						return jedisPipeline.hmset(key, hash);
					} finally {
						long duration = System.nanoTime() / 1000 - startTime;
						opMonitor.recordSendLatency(OpName.HMSET.name(), duration, TimeUnit.MICROSECONDS);
					}
				}
			}.execute(key, OpName.HMSET);
		} else {
			return new PipelineCompressionOperation<String>() {
				@Override
				Response<String> execute(final Pipeline jedisPipeline) throws DynoException {
					return new PipelineResponse(null).apply(new Func0<Response<String>>() {
						@Override
						public Response<String> call() {
							return jedisPipeline.hmset(key, CollectionUtils.transform(hash,
									new CollectionUtils.MapEntryTransform<String, String, String>() {
										@Override
										public String get(String key, String val) {
											return compressValue(val);
										}
									}));
						}
					});
				}
			}.execute(key, OpName.HMSET);
		}

	}

	@Override
	public Response<Long> hset(final String key, final String field, final String value) {
		if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
			return new PipelineOperation<Long>() {
				@Override
				Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
					return jedisPipeline.hset(key, field, value);
				}
			}.execute(key, OpName.HSET);
		} else {
			return new PipelineCompressionOperation<Long>() {
				@Override
				Response<Long> execute(final Pipeline jedisPipeline) throws DynoException {
					return new PipelineLongResponse(null).apply(new Func0<Response<Long>>() {
						@Override
						public Response<Long> call() {
							return jedisPipeline.hset(key, field, compressValue(value));
						}
					});
				}
			}.execute(key, OpName.HSET);
		}
	}

	/**
	 * This method is a BinaryRedisPipeline command which dyno does not yet properly
	 * support, therefore the interface is not yet implemented.
	 */
	public Response<Long> hset(final byte[] key, final byte[] field, final byte[] value) {
		if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
			return new PipelineOperation<Long>() {
				@Override
				Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
					return jedisPipeline.hset(key, field, value);
				}
			}.execute(key, OpName.HSET);
		} else {
			return new PipelineCompressionOperation<Long>() {
				@Override
				Response<Long> execute(final Pipeline jedisPipeline) throws DynoException {
					return new PipelineLongResponse(null).apply(new Func0<Response<Long>>() {
						@Override
						public Response<Long> call() {
							return jedisPipeline.hset(key, field, compressValue(value));
						}
					});
				}
			}.execute(key, OpName.HSET);
		}
	}

	@Override
	public Response<Long> hsetnx(final String key, final String field, final String value) {
		if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
			return new PipelineOperation<Long>() {
				@Override
				Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
					return jedisPipeline.hsetnx(key, field, value);
				}
			}.execute(key, OpName.HSETNX);
		} else {
			return new PipelineCompressionOperation<Long>() {
				@Override
				Response<Long> execute(final Pipeline jedisPipeline) throws DynoException {
					return new PipelineLongResponse(null).apply(new Func0<Response<Long>>() {
						@Override
						public Response<Long> call() {
							return jedisPipeline.hsetnx(key, field, compressValue(value));
						}
					});
				}
			}.execute(key, OpName.HSETNX);
		}
	}

	@Override
	public Response<List<String>> hvals(final String key) {
		if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
			return new PipelineOperation<List<String>>() {
				@Override
				Response<List<String>> execute(Pipeline jedisPipeline) throws DynoException {
					return jedisPipeline.hvals(key);
				}
			}.execute(key, OpName.HVALS);
		} else {
			return new PipelineCompressionOperation<List<String>>() {
				@Override
				Response<List<String>> execute(final Pipeline jedisPipeline) throws DynoException {
					return new PipelineListResponse(null).apply(new Func0<Response<List<String>>>() {
						@Override
						public Response<List<String>> call() {
							return jedisPipeline.hvals(key);
						}
					});
				}
			}.execute(key, OpName.HVALS);
		}

	}

	@Override
	public Response<Long> incr(final String key) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.incr(key);
			}

		}.execute(key, OpName.INCR);

	}

	@Override
	public Response<Long> incrBy(final String key, final long integer) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.incrBy(key, integer);
			}

		}.execute(key, OpName.INCRBY);

	}

	/* not supported by RedisPipeline 2.7.3 */
	public Response<Double> incrByFloat(final String key, final double increment) {
		return new PipelineOperation<Double>() {

			@Override
			Response<Double> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.incrByFloat(key, increment);
			}

		}.execute(key, OpName.INCRBYFLOAT);

	}

	@Override
	public Response<String> lindex(final String key, final long index) {
		return new PipelineOperation<String>() {

			@Override
			Response<String> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.lindex(key, index);
			}

		}.execute(key, OpName.LINDEX);

	}

	@Override
	public Response<Long> linsert(final String key, final LIST_POSITION where, final String pivot, final String value) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.linsert(key, where, pivot, value);
			}

		}.execute(key, OpName.LINSERT);

	}

	@Override
	public Response<Long> llen(final String key) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.llen(key);
			}

		}.execute(key, OpName.LLEN);

	}

	@Override
	public Response<String> lpop(final String key) {
		return new PipelineOperation<String>() {

			@Override
			Response<String> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.lpop(key);
			}

		}.execute(key, OpName.LPOP);

	}

	@Override
	public Response<Long> lpush(final String key, final String... string) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.lpush(key, string);
			}

		}.execute(key, OpName.LPUSH);

	}

	@Override
	public Response<Long> lpushx(final String key, final String... string) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.lpushx(key, string);
			}

		}.execute(key, OpName.LPUSHX);

	}

	@Override
	public Response<List<String>> lrange(final String key, final long start, final long end) {
		return new PipelineOperation<List<String>>() {

			@Override
			Response<List<String>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.lrange(key, start, end);
			}

		}.execute(key, OpName.LRANGE);

	}

	@Override
	public Response<Long> lrem(final String key, final long count, final String value) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.lrem(key, count, value);
			}

		}.execute(key, OpName.LREM);

	}

	@Override
	public Response<String> lset(final String key, final long index, final String value) {
		return new PipelineOperation<String>() {

			@Override
			Response<String> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.lset(key, index, value);
			}

		}.execute(key, OpName.LSET);

	}

	@Override
	public Response<String> ltrim(final String key, final long start, final long end) {
		return new PipelineOperation<String>() {

			@Override
			Response<String> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.ltrim(key, start, end);
			}

		}.execute(key, OpName.LTRIM);

	}

	@Override
	public Response<Long> move(final String key, final int dbIndex) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.move(key, dbIndex);
			}

		}.execute(key, OpName.MOVE);

	}

	@Override
	public Response<Long> persist(final String key) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.persist(key);
			}

		}.execute(key, OpName.PERSIST);

	}

	/* not supported by RedisPipeline 2.7.3 */
	public Response<String> rename(final String oldkey, final String newkey) {
		return new PipelineOperation<String>() {

			@Override
			Response<String> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.rename(oldkey, newkey);
			}
		}.execute(oldkey, OpName.RENAME);

	}

	/* not supported by RedisPipeline 2.7.3 */
	public Response<Long> renamenx(final String oldkey, final String newkey) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.renamenx(oldkey, newkey);
			}
		}.execute(oldkey, OpName.RENAMENX);

	}

	@Override
	public Response<String> rpop(final String key) {
		return new PipelineOperation<String>() {

			@Override
			Response<String> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.rpop(key);
			}

		}.execute(key, OpName.RPOP);

	}

	@Override
	public Response<Long> rpush(final String key, final String... string) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.rpush(key, string);
			}

		}.execute(key, OpName.RPUSH);

	}

	@Override
	public Response<Long> rpushx(final String key, final String... string) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.rpushx(key, string);
			}

		}.execute(key, OpName.RPUSHX);

	}

	@Override
	public Response<Long> sadd(final String key, final String... member) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.sadd(key, member);
			}

		}.execute(key, OpName.SADD);

	}

	@Override
	public Response<Long> scard(final String key) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.scard(key);
			}

		}.execute(key, OpName.SCARD);

	}

	@Override
	public Response<Boolean> sismember(final String key, final String member) {
		return new PipelineOperation<Boolean>() {

			@Override
			Response<Boolean> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.sismember(key, member);
			}

		}.execute(key, OpName.SISMEMBER);
	}

	@Override
	public Response<String> set(final String key, final String value) {
		if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
			return new PipelineOperation<String>() {
				@Override
				Response<String> execute(Pipeline jedisPipeline) throws DynoException {
					long startTime = System.nanoTime() / 1000;
					try {
						return jedisPipeline.set(key, value);
					} finally {
						long duration = System.nanoTime() / 1000 - startTime;
						opMonitor.recordSendLatency(OpName.SET.name(), duration, TimeUnit.MICROSECONDS);
					}
				}

			}.execute(key, OpName.SET);
		} else {
			return new PipelineCompressionOperation<String>() {
				@Override
				Response<String> execute(final Pipeline jedisPipeline) throws DynoException {
					long startTime = System.nanoTime() / 1000;
					try {
						return new PipelineResponse(null).apply(new Func0<Response<String>>() {
							@Override
							public Response<String> call() {
								return jedisPipeline.set(key, compressValue(value));
							}
						});
					} finally {
						long duration = System.nanoTime() / 1000 - startTime;
						opMonitor.recordSendLatency(OpName.SET.name(), duration, TimeUnit.MICROSECONDS);
					}
				}
			}.execute(key, OpName.SET);
		}
	}

	@Override
	public Response<Boolean> setbit(final String key, final long offset, final boolean value) {
		return new PipelineOperation<Boolean>() {

			@Override
			Response<Boolean> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.setbit(key, offset, value);
			}

		}.execute(key, OpName.SETBIT);

	}

	@Override
	public Response<String> setex(final String key, final int seconds, final String value) {
		if (CompressionStrategy.NONE == connPool.getConfiguration().getCompressionStrategy()) {
			return new PipelineOperation<String>() {
				@Override
				Response<String> execute(Pipeline jedisPipeline) throws DynoException {
					return jedisPipeline.setex(key, seconds, value);
				}
			}.execute(key, OpName.SETEX);
		} else {
			return new PipelineCompressionOperation<String>() {
				@Override
				Response<String> execute(final Pipeline jedisPipeline) throws DynoException {
					return new PipelineResponse(null).apply(new Func0<Response<String>>() {
						@Override
						public Response<String> call() {
							return jedisPipeline.setex(key, seconds, compressValue(value));
						}
					});
				}
			}.execute(key, OpName.SETEX);
		}

	}

	@Override
	public Response<Long> setnx(final String key, final String value) {
		return new PipelineOperation<Long>() {
			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.setnx(key, value);
			}
		}.execute(key, OpName.SETNX);
	}

	@Override
	public Response<Long> setrange(final String key, final long offset, final String value) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.setrange(key, offset, value);
			}

		}.execute(key, OpName.SETRANGE);

	}

	@Override
	public Response<Set<String>> smembers(final String key) {
		return new PipelineOperation<Set<String>>() {

			@Override
			Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.smembers(key);
			}

		}.execute(key, OpName.SMEMBERS);

	}

	@Override
	public Response<List<String>> sort(final String key) {
		return new PipelineOperation<List<String>>() {

			@Override
			Response<List<String>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.sort(key);
			}

		}.execute(key, OpName.SORT);

	}

	@Override
	public Response<List<String>> sort(final String key, final SortingParams sortingParameters) {
		return new PipelineOperation<List<String>>() {

			@Override
			Response<List<String>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.sort(key, sortingParameters);
			}

		}.execute(key, OpName.SORT);

	}

	@Override
	public Response<String> spop(final String key) {
		return new PipelineOperation<String>() {

			@Override
			Response<String> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.spop(key);
			}

		}.execute(key, OpName.SPOP);

	}

	@Override
	public Response<Set<String>> spop(final String key, final long count) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<String> srandmember(final String key) {
		return new PipelineOperation<String>() {

			@Override
			Response<String> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.srandmember(key);
			}

		}.execute(key, OpName.SRANDMEMBER);

	}

	@Override
	public Response<Long> srem(final String key, final String... member) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.srem(key, member);
			}

		}.execute(key, OpName.SREM);

	}

	/**
	 * This method is not supported by the BinaryRedisPipeline interface.
	 */
	public Response<ScanResult<String>> sscan(final String key, final int cursor) {
		throw new UnsupportedOperationException("'SSCAN' cannot be called in pipeline");
	}

	/**
	 * This method is not supported by the BinaryRedisPipeline interface.
	 */
	public Response<ScanResult<String>> sscan(final String key, final String cursor) {
		throw new UnsupportedOperationException("'SSCAN' cannot be called in pipeline");
	}

	@Override
	public Response<Long> strlen(final String key) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.strlen(key);
			}

		}.execute(key, OpName.STRLEN);

	}

	@Override
	public Response<String> substr(final String key, final int start, final int end) {
		return new PipelineOperation<String>() {

			@Override
			Response<String> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.substr(key, start, end);
			}

		}.execute(key, OpName.SUBSTR);

	}

	@Override
	public Response<Long> ttl(final String key) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.ttl(key);
			}

		}.execute(key, OpName.TTL);

	}

	@Override
	public Response<String> type(final String key) {
		return new PipelineOperation<String>() {

			@Override
			Response<String> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.type(key);
			}

		}.execute(key, OpName.TYPE);

	}

	@Override
	public Response<Long> zadd(final String key, final double score, final String member) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zadd(key, score, member);
			}

		}.execute(key, OpName.ZADD);

	}

	@Override
	public Response<Long> zadd(final String key, final Map<String, Double> scoreMembers) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zadd(key, scoreMembers);
			}

		}.execute(key, OpName.ZADD);

	}

	@Override
	public Response<Long> zcard(final String key) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zcard(key);
			}

		}.execute(key, OpName.ZCARD);

	}

	@Override
	public Response<Long> zcount(final String key, final double min, final double max) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zcount(key, min, max);
			}

		}.execute(key, OpName.ZCOUNT);

	}

	@Override
	public Response<Double> zincrby(final String key, final double score, final String member) {
		return new PipelineOperation<Double>() {

			@Override
			Response<Double> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zincrby(key, score, member);
			}

		}.execute(key, OpName.ZINCRBY);

	}

	@Override
	public Response<Set<String>> zrange(final String key, final long start, final long end) {
		return new PipelineOperation<Set<String>>() {

			@Override
			Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zrange(key, start, end);
			}

		}.execute(key, OpName.ZRANGE);

	}

	@Override
	public Response<Set<String>> zrangeByScore(final String key, final double min, final double max) {
		return new PipelineOperation<Set<String>>() {

			@Override
			Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zrangeByScore(key, min, max);
			}

		}.execute(key, OpName.ZRANGEBYSCORE);

	}

	@Override
	public Response<Set<String>> zrangeByScore(final String key, final String min, final String max) {
		return new PipelineOperation<Set<String>>() {

			@Override
			Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zrangeByScore(key, min, max);
			}

		}.execute(key, OpName.ZRANGEBYSCORE);

	}

	@Override
	public Response<Set<String>> zrangeByScore(final String key, final double min, final double max, final int offset,
			final int count) {
		return new PipelineOperation<Set<String>>() {

			@Override
			Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zrangeByScore(key, min, max, offset, count);
			}

		}.execute(key, OpName.ZRANGEBYSCORE);

	}

	@Override
	public Response<Set<Tuple>> zrangeByScoreWithScores(final String key, final double min, final double max) {
		return new PipelineOperation<Set<Tuple>>() {

			@Override
			Response<Set<Tuple>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zrangeByScoreWithScores(key, min, max);
			}

		}.execute(key, OpName.ZRANGEBYSCOREWITHSCORES);

	}

	@Override
	public Response<Set<Tuple>> zrangeByScoreWithScores(final String key, final double min, final double max,
			final int offset, final int count) {
		return new PipelineOperation<Set<Tuple>>() {

			@Override
			Response<Set<Tuple>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zrangeByScoreWithScores(key, min, max, offset, count);
			}

		}.execute(key, OpName.ZRANGEBYSCOREWITHSCORES);

	}

	@Override
	public Response<Set<String>> zrevrangeByScore(final String key, final double max, final double min) {
		return new PipelineOperation<Set<String>>() {

			@Override
			Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zrevrangeByScore(key, max, min);
			}

		}.execute(key, OpName.ZREVRANGEBYSCORE);

	}

	@Override
	public Response<Set<String>> zrevrangeByScore(final String key, final String max, final String min) {
		return new PipelineOperation<Set<String>>() {

			@Override
			Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zrevrangeByScore(key, max, min);
			}

		}.execute(key, OpName.ZREVRANGEBYSCORE);

	}

	@Override
	public Response<Set<String>> zrevrangeByScore(final String key, final double max, final double min,
			final int offset, final int count) {
		return new PipelineOperation<Set<String>>() {

			@Override
			Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zrevrangeByScore(key, max, min, offset, count);
			}

		}.execute(key, OpName.ZREVRANGEBYSCORE);

	}

	@Override
	public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key, final double max, final double min) {
		return new PipelineOperation<Set<Tuple>>() {

			@Override
			Response<Set<Tuple>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zrevrangeByScoreWithScores(key, max, min);
			}

		}.execute(key, OpName.ZREVRANGEBYSCOREWITHSCORES);

	}

	@Override
	public Response<Set<Tuple>> zrevrangeByScoreWithScores(final String key, final double max, final double min,
			final int offset, final int count) {
		return new PipelineOperation<Set<Tuple>>() {

			@Override
			Response<Set<Tuple>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zrevrangeByScoreWithScores(key, max, min, offset, count);
			}

		}.execute(key, OpName.ZREVRANGEBYSCOREWITHSCORES);

	}

	@Override
	public Response<Set<Tuple>> zrangeWithScores(final String key, final long start, final long end) {
		return new PipelineOperation<Set<Tuple>>() {

			@Override
			Response<Set<Tuple>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zrangeWithScores(key, start, end);
			}

		}.execute(key, OpName.ZRANGEWITHSCORES);

	}

	@Override
	public Response<Long> zrank(final String key, final String member) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zrank(key, member);
			}

		}.execute(key, OpName.ZRANK);

	}

	@Override
	public Response<Long> zrem(final String key, final String... member) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zrem(key, member);
			}

		}.execute(key, OpName.ZREM);

	}

	@Override
	public Response<Long> zremrangeByRank(final String key, final long start, final long end) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zremrangeByRank(key, start, end);
			}

		}.execute(key, OpName.ZREMRANGEBYRANK);

	}

	@Override
	public Response<Long> zremrangeByScore(final String key, final double start, final double end) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zremrangeByScore(key, start, end);
			}

		}.execute(key, OpName.ZREMRANGEBYSCORE);

	}

	@Override
	public Response<Set<String>> zrevrange(final String key, final long start, final long end) {
		return new PipelineOperation<Set<String>>() {

			@Override
			Response<Set<String>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zrevrange(key, start, end);
			}

		}.execute(key, OpName.ZREVRANGE);

	}

	@Override
	public Response<Set<Tuple>> zrevrangeWithScores(final String key, final long start, final long end) {
		return new PipelineOperation<Set<Tuple>>() {

			@Override
			Response<Set<Tuple>> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zrevrangeWithScores(key, start, end);
			}

		}.execute(key, OpName.ZREVRANGEWITHSCORES);

	}

	@Override
	public Response<Long> zrevrank(final String key, final String member) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zrevrank(key, member);
			}

		}.execute(key, OpName.ZREVRANK);

	}

	@Override
	public Response<Double> zscore(final String key, final String member) {
		return new PipelineOperation<Double>() {

			@Override
			Response<Double> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.zscore(key, member);
			}

		}.execute(key, OpName.ZSCORE);

	}

	/**
	 * This method is not supported by the BinaryRedisPipeline interface.
	 */
	public Response<ScanResult<Tuple>> zscan(final String key, final int cursor) {
		throw new UnsupportedOperationException("'ZSCAN' cannot be called in pipeline");
	}

	@Override
	public Response<Long> zlexcount(String key, String min, String max) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<String>> zrangeByLex(String key, String min, String max) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<String>> zrangeByLex(String key, String min, String max, int offset, int count) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> zremrangeByLex(String key, String start, String end) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> bitcount(final String key) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.bitcount(key);
			}

		}.execute(key, OpName.BITCOUNT);

	}

	@Override
	public Response<Long> bitcount(final String key, final long start, final long end) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.bitcount(key, start, end);
			}

		}.execute(key, OpName.BITCOUNT);

	}

	/**** Binary Operations ****/
	@Override
	public Response<String> set(final byte[] key, final byte[] value) {
		return new PipelineOperation<String>() {
			@Override
			Response<String> execute(Pipeline jedisPipeline) throws DynoException {
				long startTime = System.nanoTime() / 1000;
				try {
					return jedisPipeline.set(key, value);
				} finally {
					long duration = System.nanoTime() / 1000 - startTime;
					opMonitor.recordSendLatency(OpName.SET.name(), duration, TimeUnit.MICROSECONDS);
				}
			}

		}.execute(key, OpName.SET);
	}

	@Override
	public Response<Long> pfadd(String key, String... elements) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> pfcount(String key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<Long>> bitfield(String key, String... arguments) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<String>> zrevrangeByLex(String key, String max, String min) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<String>> zrevrangeByLex(String key, String max, String min, int offset, int count) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> geoadd(String arg0, Map<String, GeoCoordinate> arg1) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> geoadd(String arg0, double arg1, double arg2, String arg3) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Double> geodist(String arg0, String arg1, String arg2) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Double> geodist(String arg0, String arg1, String arg2, GeoUnit arg3) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<String>> geohash(String arg0, String... arg1) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<GeoCoordinate>> geopos(String arg0, String... arg1) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<GeoRadiusResponse>> georadius(String arg0, double arg1, double arg2, double arg3,
			GeoUnit arg4) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<GeoRadiusResponse>> georadius(String arg0, double arg1, double arg2, double arg3, GeoUnit arg4,
			GeoRadiusParam arg5) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<GeoRadiusResponse>> georadiusByMember(String arg0, String arg1, double arg2, GeoUnit arg3) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<GeoRadiusResponse>> georadiusByMember(String arg0, String arg1, double arg2, GeoUnit arg3,
			GeoRadiusParam arg4) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> zadd(String arg0, Map<String, Double> arg1, ZAddParams arg2) {
		throw new UnsupportedOperationException("not yet implemented");

	}

	@Override
	public Response<Long> zadd(String arg0, double arg1, String arg2, ZAddParams arg3) {
		throw new UnsupportedOperationException("not yet implemented");

	}

	@Override
	public Response<Double> zincrby(String arg0, double arg1, String arg2, ZIncrByParams arg3) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	public void sync() {
		long startTime = System.nanoTime() / 1000;
		try {
			jedisPipeline.sync();
			opMonitor.recordPipelineSync();
		} catch (JedisConnectionException jce) {
			String msg = "Failed sync() to host: " + getHostInfo();
			pipelineEx.set(new FatalConnectionException(msg, jce));
			cpMonitor.incOperationFailure(connection == null ? null : connection.getHost(), jce);
			throw jce;
		} finally {
			long duration = System.nanoTime() / 1000 - startTime;
			opMonitor.recordLatency(duration, TimeUnit.MICROSECONDS);
			discardPipeline(false);
			releaseConnection();
		}
	}

	public List<Object> syncAndReturnAll() {
		long startTime = System.nanoTime() / 1000;
		try {
			List<Object> result = jedisPipeline.syncAndReturnAll();
			opMonitor.recordPipelineSync();
			return result;
		} catch (JedisConnectionException jce) {
			String msg = "Failed syncAndReturnAll() to host: " + getHostInfo();
			pipelineEx.set(new FatalConnectionException(msg, jce));
			cpMonitor.incOperationFailure(connection == null ? null : connection.getHost(), jce);
			throw jce;
		} finally {
			long duration = System.nanoTime() / 1000 - startTime;
			opMonitor.recordLatency(duration, TimeUnit.MICROSECONDS);
			discardPipeline(false);
			releaseConnection();
		}
	}

	private void discardPipeline(boolean recordLatency) {
		try {
			if (jedisPipeline != null) {
				long startTime = System.nanoTime() / 1000;
				jedisPipeline.sync();
				if (recordLatency) {
					long duration = System.nanoTime() / 1000 - startTime;
					opMonitor.recordLatency(duration, TimeUnit.MICROSECONDS);
				}
				jedisPipeline = null;
			}
		} catch (Exception e) {
			Logger.warn(String.format("Failed to discard jedis pipeline, %s", getHostInfo()), e);
		}
	}

	private void releaseConnection() {
		if (connection != null) {
			try {
				connection.getContext().reset();
				connection.getParentConnectionPool().returnConnection(connection);
				if (pipelineEx.get() != null) {
					connPool.getHealthTracker().trackConnectionError(connection.getParentConnectionPool(),
							pipelineEx.get());
					pipelineEx.set(null);
				}
				connection = null;
			} catch (Exception e) {
				Logger.warn(String.format("Failed to return connection in Dyno Jedis Pipeline, %s", getHostInfo()), e);
			}
		}
	}

	public void discardPipelineAndReleaseConnection() {
		opMonitor.recordPipelineDiscard();
		discardPipeline(true);
		releaseConnection();
	}

	@Override
	public void close() throws Exception {
		discardPipelineAndReleaseConnection();
	}

	private String getHostInfo() {
		if (connection != null && connection.getHost() != null) {
			return connection.getHost().toString();
		}

		return "unknown";
	}

	@Override
	public Response<Long> append(byte[] key, byte[] value) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<byte[]>> blpop(byte[] arg) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<byte[]>> brpop(byte[] arg) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> decr(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> decrBy(final byte[] key, final long integer) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.decrBy(key, integer);
			}
		}.execute(key, OpName.DECRBY);	}

	@Override
	public Response<Long> del(final byte[] key) {
		return new PipelineOperation<Long>() {

			@Override
			Response<Long> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.del(key);
			}
		}.execute(key, OpName.DEL);
	}

	@Override
	public Response<byte[]> echo(byte[] string) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Boolean> exists(final byte[] key) {
		return new PipelineOperation<Boolean>() {

			@Override
			Response<Boolean> execute(final Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.exists(key);
			}
		}.execute(key, OpName.EXISTS);	
	}

	@Override
	public Response<Long> expire(byte[] key, int seconds) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> pexpire(byte[] key, long milliseconds) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> expireAt(byte[] key, long unixTime) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> pexpireAt(byte[] key, long millisecondsTimestamp) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<byte[]> get(final byte[] key) {
		return new PipelineOperation<byte[]>() {
			@Override
			Response<byte[]> execute(Pipeline jedisPipeline) throws DynoException {
				long startTime = System.nanoTime() / 1000;
				try {
					return jedisPipeline.get(key);
				} finally {
					long duration = System.nanoTime() / 1000 - startTime;
					opMonitor.recordSendLatency(OpName.GET.name(), duration, TimeUnit.MICROSECONDS);
				}
			}
		}.execute(key, OpName.GET);	
	}

	@Override
	public Response<Boolean> getbit(byte[] key, long offset) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<byte[]> getSet(final byte[] key, final byte[] value) {
		return new PipelineOperation<byte[]>() {
			@Override
			Response<byte[]> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.getSet(key, value);
			}
		}.execute(key, OpName.GETSET);	}

	@Override
	public Response<Long> getrange(byte[] key, long startOffset, long endOffset) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> hdel(byte[] key, byte[]... field) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Boolean> hexists(byte[] key, byte[] field) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> hincrBy(byte[] key, byte[] field, long value) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<byte[]>> hkeys(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> hlen(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> hsetnx(byte[] key, byte[] field, byte[] value) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<byte[]>> hvals(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> incr(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> incrBy(byte[] key, long integer) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<byte[]> lindex(byte[] key, long index) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> llen(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<byte[]> lpop(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> lpush(byte[] key, byte[]... string) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> lpushx(byte[] key, byte[]... bytes) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<byte[]>> lrange(byte[] key, long start, long end) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> lrem(byte[] key, long count, byte[] value) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<String> lset(byte[] key, long index, byte[] value) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<String> ltrim(byte[] key, long start, long end) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> move(byte[] key, int dbIndex) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> persist(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<byte[]> rpop(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> rpush(byte[] key, byte[]... string) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> rpushx(byte[] key, byte[]... string) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> sadd(byte[] key, byte[]... member) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> scard(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Boolean> setbit(byte[] key, long offset, byte[] value) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> setrange(byte[] key, long offset, byte[] value) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<String> setex(final byte[] key, final int seconds, final byte[] value) {
		return new PipelineOperation<String>() {
			@Override
			Response<String> execute(Pipeline jedisPipeline) throws DynoException {
				return jedisPipeline.setex(key, seconds, value);
			}
		}.execute(key, OpName.SETEX);	}

	@Override
	public Response<Long> setnx(byte[] key, byte[] value) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<byte[]>> smembers(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Boolean> sismember(byte[] key, byte[] member) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<byte[]>> sort(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<byte[]>> sort(byte[] key, SortingParams sortingParameters) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<byte[]> spop(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<byte[]>> spop(byte[] key, long count) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<byte[]> srandmember(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> srem(byte[] key, byte[]... member) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> strlen(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<String> substr(byte[] key, int start, int end) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> ttl(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<String> type(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> zadd(byte[] key, double score, byte[] member) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> zadd(byte[] key, double score, byte[] member, ZAddParams params) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> zadd(byte[] key, Map<byte[], Double> scoreMembers) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> zadd(byte[] key, Map<byte[], Double> scoreMembers, ZAddParams params) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> zcard(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> zcount(byte[] key, double min, double max) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Double> zincrby(byte[] key, double score, byte[] member) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Double> zincrby(byte[] key, double score, byte[] member, ZIncrByParams params) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<byte[]>> zrange(byte[] key, long start, long end) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<byte[]>> zrangeByScore(byte[] key, double min, double max) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<byte[]>> zrangeByScore(byte[] key, byte[] min, byte[] max) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<byte[]>> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<byte[]>> zrangeByScore(byte[] key, byte[] min, byte[] max, int offset, int count) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, double min, double max) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, double min, double max, int offset, int count) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<Tuple>> zrangeByScoreWithScores(byte[] key, byte[] min, byte[] max, int offset, int count) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<byte[]>> zrevrangeByScore(byte[] key, double max, double min) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<byte[]>> zrevrangeByScore(byte[] key, byte[] max, byte[] min) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<byte[]>> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<byte[]>> zrevrangeByScore(byte[] key, byte[] max, byte[] min, int offset, int count) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, double max, double min) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, double max, double min, int offset, int count) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<Tuple>> zrevrangeByScoreWithScores(byte[] key, byte[] max, byte[] min, int offset, int count) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<Tuple>> zrangeWithScores(byte[] key, long start, long end) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> zrank(byte[] key, byte[] member) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> zrem(byte[] key, byte[]... member) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> zremrangeByRank(byte[] key, long start, long end) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> zremrangeByScore(byte[] key, double start, double end) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> zremrangeByScore(byte[] key, byte[] start, byte[] end) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<byte[]>> zrevrange(byte[] key, long start, long end) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<Tuple>> zrevrangeWithScores(byte[] key, long start, long end) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> zrevrank(byte[] key, byte[] member) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Double> zscore(byte[] key, byte[] member) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> zlexcount(byte[] key, byte[] min, byte[] max) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<byte[]>> zrangeByLex(byte[] key, byte[] min, byte[] max) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<byte[]>> zrangeByLex(byte[] key, byte[] min, byte[] max, int offset, int count) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<byte[]>> zrevrangeByLex(byte[] key, byte[] max, byte[] min) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Set<byte[]>> zrevrangeByLex(byte[] key, byte[] max, byte[] min, int offset, int count) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> zremrangeByLex(byte[] key, byte[] min, byte[] max) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> bitcount(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> bitcount(byte[] key, long start, long end) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> pfadd(byte[] key, byte[]... elements) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> pfcount(byte[] key) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> geoadd(byte[] key, double longitude, double latitude, byte[] member) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Long> geoadd(byte[] key, Map<byte[], GeoCoordinate> memberCoordinateMap) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Double> geodist(byte[] key, byte[] member1, byte[] member2) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<Double> geodist(byte[] key, byte[] member1, byte[] member2, GeoUnit unit) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<byte[]>> geohash(byte[] key, byte[]... members) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<GeoCoordinate>> geopos(byte[] key, byte[]... members) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<GeoRadiusResponse>> georadius(byte[] key, double longitude, double latitude, double radius,
			GeoUnit unit) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<GeoRadiusResponse>> georadius(byte[] key, double longitude, double latitude, double radius,
			GeoUnit unit, GeoRadiusParam param) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<GeoRadiusResponse>> georadiusByMember(byte[] key, byte[] member, double radius, GeoUnit unit) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<GeoRadiusResponse>> georadiusByMember(byte[] key, byte[] member, double radius, GeoUnit unit,
			GeoRadiusParam param) {
		throw new UnsupportedOperationException("not yet implemented");
	}

	@Override
	public Response<List<Long>> bitfield(byte[] key, byte[]... elements) {
		throw new UnsupportedOperationException("not yet implemented");
	}

}
