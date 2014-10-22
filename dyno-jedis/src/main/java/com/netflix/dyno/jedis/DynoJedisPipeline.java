package com.netflix.dyno.jedis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.RedisPipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;

import com.netflix.dyno.connectionpool.BaseOperation;
import com.netflix.dyno.connectionpool.Connection;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl;
import com.netflix.dyno.jedis.JedisConnectionFactory.JedisConnection;

public class DynoJedisPipeline implements RedisPipeline {

	private static final Logger Logger = LoggerFactory.getLogger(DynoJedisPipeline.class);
	
	private Pipeline jedisPipeline; 
	private final ConnectionPoolImpl<Jedis> connPool;
	private Connection<Jedis> connection;
	private String theKey; 
	
	private static final String DynoPipeline = "DynoPipeline";
	
	DynoJedisPipeline(ConnectionPoolImpl<Jedis> cPool) {
		this.connPool = cPool;
	}
	
	private void checkKey(final String key) {
		
		if (theKey != null) {
			
			if (!theKey.equals(key)) {
				try { 
					throw new RuntimeException("Must have same key for Redis Pipeline in Dynomite");
				} finally {
					discardPipeline();
					releaseConnection();
				}
			}
		} else {

			theKey = key;
			connection = connPool.getConnectionForOperation(new BaseOperation<Jedis, String>() {

				@Override
				public String getName() {
					return DynoPipeline;
				}

				@Override
				public String getKey() {
					return key;
				}
			});
			
			Jedis jedis = ((JedisConnection)connection).getClient();
			
			jedisPipeline = jedis.pipelined();
		}
	}
	
	@Override
	public Response<Long> append(String key, String value) {
	    checkKey(key);
	    return jedisPipeline.append(key, value);
	}

	@Override
	public Response<List<String>> blpop(String arg) {
	    checkKey(arg);
	    return jedisPipeline.blpop(arg);
	}

	@Override
	public Response<List<String>> brpop(String arg) {
	    checkKey(arg);
	    return jedisPipeline.brpop(arg);
	}

	@Override
	public Response<Long> decr(String key) {
	    checkKey(key);
	    return jedisPipeline.decr(key);
	}

	@Override
	public Response<Long> decrBy(String key, long integer) {
	    checkKey(key);
	    return jedisPipeline.decrBy(key, integer);
	}

	@Override
	public Response<Long> del(String key) {
	    checkKey(key);
	    return jedisPipeline.del(key);
	}

	@Override
	public Response<String> echo(String string) {
	    checkKey(string);
	    return jedisPipeline.echo(string);
	}

	@Override
	public Response<Boolean> exists(String key) {
	    checkKey(key);
	    return jedisPipeline.exists(key);
	}

	@Override
	public Response<Long> expire(String key, int seconds) {
	    checkKey(key);
	    return jedisPipeline.expire(key, seconds);
	}

	@Override
	public Response<Long> expireAt(String key, long unixTime) {
	    checkKey(key);
	    return jedisPipeline.expireAt(key, unixTime);
	}

	@Override
	public Response<String> get(String key) {
	    checkKey(key);
	    return jedisPipeline.get(key);
	}

	@Override
	public Response<Boolean> getbit(String key, long offset) {
	    checkKey(key);
	    return jedisPipeline.getbit(key, offset);
	}

	@Override
	public Response<String> getrange(String key, long startOffset, long endOffset) {
	    checkKey(key);
	    return jedisPipeline.getrange(key, startOffset, endOffset);
	}

	@Override
	public Response<String> getSet(String key, String value) {
	    checkKey(key);
	    return jedisPipeline.getSet(key, value);
	}

	@Override
	public Response<Long> hdel(String key, String... field) {
	    checkKey(key);
	    return jedisPipeline.hdel(key, field);
	}

	@Override
	public Response<Boolean> hexists(String key, String field) {
	    checkKey(key);
	    return jedisPipeline.hexists(key, field);
	}

	@Override
	public Response<String> hget(String key, String field) {
	    checkKey(key);
	    return jedisPipeline.hget(key, field);
	}

	@Override
	public Response<Map<String, String>> hgetAll(String key) {
	    checkKey(key);
	    return jedisPipeline.hgetAll(key);
	}

	@Override
	public Response<Long> hincrBy(String key, String field, long value) {
	    checkKey(key);
	    return jedisPipeline.hincrBy(key, field, value);
	}

	@Override
	public Response<Set<String>> hkeys(String key) {
	    checkKey(key);
	    return jedisPipeline.hkeys(key);
	}

	@Override
	public Response<Long> hlen(String key) {
	    checkKey(key);
	    return jedisPipeline.hlen(key);
	}

	@Override
	public Response<List<String>> hmget(String key, String... fields) {
	    checkKey(key);
	    return jedisPipeline.hmget(key, fields);
	}

	@Override
	public Response<String> hmset(String key, Map<String, String> hash) {
	    checkKey(key);
	    return jedisPipeline.hmset(key, hash);
	}

	@Override
	public Response<Long> hset(String key, String field, String value) {
	    checkKey(key);
	    return jedisPipeline.hset(key, field, value);
	}

	@Override
	public Response<Long> hsetnx(String key, String field, String value) {
	    checkKey(key);
	    return jedisPipeline.hsetnx(key, field, value);
	}

	@Override
	public Response<List<String>> hvals(String key) {
	    checkKey(key);
	    return jedisPipeline.hvals(key);
	}

	@Override
	public Response<Long> incr(String key) {
	    checkKey(key);
	    return jedisPipeline.incr(key);
	}

	@Override
	public Response<Long> incrBy(String key, long integer) {
	    checkKey(key);
	    return jedisPipeline.incrBy(key, integer);
	}

	@Override
	public Response<String> lindex(String key, long index) {
	    checkKey(key);
	    return jedisPipeline.lindex(key, index);
	}

	@Override
	public Response<Long> linsert(String key, LIST_POSITION where, String pivot, String value) {
	    checkKey(key);
	    return linsert(key, where, pivot, value);
	}

	@Override
	public Response<Long> llen(String key) {
	    checkKey(key);
		return jedisPipeline.llen(key);
	}

	@Override
	public Response<String> lpop(String key) {
	    checkKey(key);
	    return jedisPipeline.lpop(key);
	}

	@Override
	public Response<Long> lpush(String key, String... string) {
	    checkKey(key);
	    return jedisPipeline.lpush(key, string);
	}

	@Override
	public Response<Long> lpushx(String key, String... string) {
	    checkKey(key);
	    return jedisPipeline.lpushx(key, string);
	}

	@Override
	public Response<List<String>> lrange(String key, long start, long end) {
	    checkKey(key);
	    return jedisPipeline.lrange(key, start, end);
	}

	@Override
	public Response<Long> lrem(String key, long count, String value) {
	    checkKey(key);
	    return jedisPipeline.lrem(key, count, value);
	}

	@Override
	public Response<String> lset(String key, long index, String value) {
	    checkKey(key);
	    return jedisPipeline.lset(key, index, value);
	}

	@Override
	public Response<String> ltrim(String key, long start, long end) {
	    checkKey(key);
	    return jedisPipeline.ltrim(key, start, end);
	}

	@Override
	public Response<Long> move(String key, int dbIndex) {
	    checkKey(key);
	    return jedisPipeline.move(key, dbIndex);
	}

	@Override
	public Response<Long> persist(String key) {
	    checkKey(key);
	    return jedisPipeline.persist(key);
	}

	@Override
	public Response<String> rpop(String key) {
	    checkKey(key);
	    return jedisPipeline.rpop(key);
	}

	@Override
	public Response<Long> rpush(String key, String... string) {
	    checkKey(key);
	    return jedisPipeline.rpush(key, string);
	}

	@Override
	public Response<Long> rpushx(String key, String... string) {
	    checkKey(key);
	    return jedisPipeline.rpushx(key, string);
	}

	@Override
	public Response<Long> sadd(String key, String... member) {
	    checkKey(key);
	    return jedisPipeline.sadd(key, member);
	}

	@Override
	public Response<Long> scard(String key) {
	    checkKey(key);
	    return jedisPipeline.scard(key);
	}

	@Override
	public Response<Boolean> sismember(String key, String member) {
	    checkKey(key);
	    return jedisPipeline.sismember(key, member);
	}

	@Override
	public Response<String> set(String key, String value) {
	    checkKey(key);
	    return jedisPipeline.set(key, value);
	}

	@Override
	public Response<Boolean> setbit(String key, long offset, boolean value) {
	    checkKey(key);
	    return jedisPipeline.setbit(key, offset, value);
	}

	@Override
	public Response<String> setex(String key, int seconds, String value) {
	    checkKey(key);
	    return jedisPipeline.setex(key, seconds, value);
	}

	@Override
	public Response<Long> setnx(String key, String value) {
	    checkKey(key);
	    return jedisPipeline.setnx(key, value);
	}

	@Override
	public Response<Long> setrange(String key, long offset, String value) {
	    checkKey(key);
	    return jedisPipeline.setrange(key, offset, value);
	}

	@Override
	public Response<Set<String>> smembers(String key) {
	    checkKey(key);
	    return jedisPipeline.smembers(key);
	}

	@Override
	public Response<List<String>> sort(String key) {
	    checkKey(key);
	    return jedisPipeline.sort(key);
	}

	@Override
	public Response<List<String>> sort(String key, SortingParams sortingParameters) {
	    checkKey(key);
	    return jedisPipeline.sort(key, sortingParameters);
	}

	@Override
	public Response<String> spop(String key) {
	    checkKey(key);
	    return jedisPipeline.spop(key);
	}

	@Override
	public Response<String> srandmember(String key) {
	    checkKey(key);
	    return jedisPipeline.srandmember(key);
	}

	@Override
	public Response<Long> srem(String key, String... member) {
	    checkKey(key);
	    return jedisPipeline.srem(key, member);
	}

	@Override
	public Response<Long> strlen(String key) {
	    checkKey(key);
	    return jedisPipeline.strlen(key);
	}

	@Override
	public Response<String> substr(String key, int start, int end) {
	    checkKey(key);
	    return jedisPipeline.substr(key, start, end);
	}

	@Override
	public Response<Long> ttl(String key) {
	    checkKey(key);
	    return jedisPipeline.ttl(key);
	}

	@Override
	public Response<String> type(String key) {
	    checkKey(key);
	    return jedisPipeline.type(key);
	}

	@Override
	public Response<Long> zadd(String key, double score, String member) {
	    checkKey(key);
	    return jedisPipeline.zadd(key, score, member);
	}

	@Override
	public Response<Long> zcard(String key) {
	    checkKey(key);
	    return jedisPipeline.zcard(key);
	}

	@Override
	public Response<Long> zcount(String key, double min, double max) {
	    checkKey(key);
	    return jedisPipeline.zcount(key, min, max);
	}

	@Override
	public Response<Double> zincrby(String key, double score, String member) {
	    checkKey(key);
	    return jedisPipeline.zincrby(key, score, member);
	}

	@Override
	public Response<Set<String>> zrange(String key, long start, long end) {
	    checkKey(key);
	    return jedisPipeline.zrange(key, start, end);
	}


	@Override
	public Response<Set<String>> zrangeByScore(String key, double min, double max) {
	    checkKey(key);
	    return jedisPipeline.zrangeByScore(key, min, max);
	}

	@Override
	public Response<Set<String>> zrangeByScore(String key, String min, String max) {
	    checkKey(key);
	    return jedisPipeline.zrangeByScore(key, min, max);
	}

	@Override
	public Response<Set<String>> zrangeByScore(String key, double min, double max, int offset, int count) {
	    checkKey(key);
	    return jedisPipeline.zrangeByScore(key, min, max, offset, count);
	}

	@Override
	public Response<Set<Tuple>> zrangeByScoreWithScores(String key, double min, double max) {
	    checkKey(key);
	    return jedisPipeline.zrangeByScoreWithScores(key, min, max);
	}

	@Override
	public Response<Set<Tuple>> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
	    checkKey(key);
	    return jedisPipeline.zrangeByScoreWithScores(key, min, max, offset, count);
	}

	@Override
	public Response<Set<String>> zrevrangeByScore(String key, double max, double min) {
	    checkKey(key);
	    return zrevrangeByScore(key, max, min);
	}

	@Override
	public Response<Set<String>> zrevrangeByScore(String key, String max, String min) {
	    checkKey(key);
		return jedisPipeline.zrevrangeByScore(key, max, min);
	}

	@Override
	public Response<Set<String>> zrevrangeByScore(String key, double max, double min, int offset, int count) {
	    checkKey(key);
	    return zrevrangeByScore(key, max, min, offset, count);
	}

	@Override
	public Response<Set<Tuple>> zrevrangeByScoreWithScores(String key, double max, double min) {
		checkKey(key);
		return jedisPipeline.zrevrangeByScoreWithScores(key, max, min);
	}

	@Override
	public Response<Set<Tuple>> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
	    checkKey(key);
	    return zrevrangeByScoreWithScores(key, max, min, offset, count);
	}

	@Override
	public Response<Set<Tuple>> zrangeWithScores(String key, long start, long end) {
	    checkKey(key);
		return jedisPipeline.zrangeWithScores(key, start, end);
	}

	@Override
	public Response<Long> zrank(String key, String member) {
	    checkKey(key);
	    return jedisPipeline.zrank(key, member);
	}

	@Override
	public Response<Long> zrem(String key, String... member) {
	    checkKey(key);
	    return jedisPipeline.zrem(key, member);
	}

	@Override
	public Response<Long> zremrangeByRank(String key, long start, long end) {
	    checkKey(key);
	    return jedisPipeline.zremrangeByRank(key, start, end);
	}

	@Override
	public Response<Long> zremrangeByScore(String key, double start, double end) {
	    checkKey(key);
	    return jedisPipeline.zremrangeByScore(key, start, end);
	}

	@Override
	public Response<Set<String>> zrevrange(String key, long start, long end) {
	    checkKey(key);
	    return jedisPipeline.zrevrange(key, start, end);
	}

	@Override
	public Response<Set<Tuple>> zrevrangeWithScores(String key, long start, long end) {
	    checkKey(key);
	    return zrevrangeWithScores(key, start, end);
	}

	@Override
	public Response<Long> zrevrank(String key, String member) {
	    checkKey(key);
		return jedisPipeline.zrevrank(key, member);
	}

	@Override
	public Response<Double> zscore(String key, String member) {
	    checkKey(key);
	    return jedisPipeline.zscore(key, member);
	}

	@Override
	public Response<Long> bitcount(String key) {
	    checkKey(key);
	    return jedisPipeline.bitcount(key);
	}

	@Override
	public Response<Long> bitcount(String key, long start, long end) {
	    checkKey(key);
	    return jedisPipeline.bitcount(key, start, end);
	}

	public void sync() {
		try {
			jedisPipeline.sync();
		} finally {
			discardPipeline();
			releaseConnection();
		}
	}
	
	private void discardPipeline() {
		
		try { 
			if (jedisPipeline != null) {
				jedisPipeline.sync();
			}
		} catch (Exception e) {
			Logger.warn("Failed to discard jedis pipeline", e);
		}
	}
	
	private void releaseConnection() {
		if (connection != null) {
			try { 
				connection.getParentConnectionPool().returnConnection(connection);
			} catch (Exception e) {
				Logger.warn("Failed to return connection in Dyno Jedis Pipeline", e);
			}
		}
	}
}
