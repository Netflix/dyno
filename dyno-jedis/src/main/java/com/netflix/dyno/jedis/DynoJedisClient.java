package com.netflix.dyno.jedis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Jedis;

import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl;
import com.netflix.dyno.contrib.DynoCPMonitor;
import com.netflix.dyno.contrib.DynoOPMonitor;

public class DynoJedisClient {
	
	private static final Logger Logger = LoggerFactory.getLogger(DynoJedisClient.class);
	
	private final ConnectionPool<Jedis> connPool;
	
	public DynoJedisClient(String name, ConnectionPool<Jedis> pool) {
		this.connPool = pool;
	}
	
	private enum OpName {
		 APPEND, DECR, DECRBY, DEL, DUMP, EXISTS, EXPIRE, EXPIREAT, GET, GETBIT, GETRANGE, GETSET, 
		 HDEL, HEXISTS,  HGET, HGETALL, HINCRBY, HINCRBYFLOAT, HKEYS, HLEN, HMGET, HMSET, HSET, HSETNX, HVALS, 
		 INCR, INCRBY, INCRBYFLOAT, LINDEX, LINSERT, LLEN, LPOP, LPUSH, LPUSHX, LRANGE, LREM, LSET, LTRIM, 
		 PERSIST, PEXPIRE, PEXPIREAT, PSETEX, PTTL, RESTORE, RPOP, RPOPLPUSH, RPUSH, RPUSHX, 
		 SADD, SCARD, SDIFF, SDIFFSTORE, SET, SETBIT, SETEX, SETNX, SETRANGE, SINTER, SINTERSTORE, SISMEMBER, SMEMBERS, SMOVE, SPOP, SRANDMEMBER, SREM, STRLEN, SUNION, SUNIONSTORE,
		 TTL, TYPE, ZCARD, ZCOUNT, ZINCRBY, ZRANK, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREVRANK, ZSCORE
		 ;
	}
	
	private abstract class BaseKeyOperation<T> implements Operation<Jedis, T> {
		
		private final String key;
		private final OpName op;
		private BaseKeyOperation(final String k, final OpName o) {
			this.key = k;
			this.op = o;
		}
		@Override
		public String getName() {
			return op.name();
		}

		@Override
		public String getKey() {
			return key;
		}
	}
	
	public OperationResult<Long> append(final String key, final String value) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.APPEND) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.append(key, value);
			}
			
		});
	}
	
	public OperationResult<Long> decr(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.DECR) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.decr(key);
			}

		});
	}
	
	public OperationResult<Long> decrBy(final String key, final Long delta) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.DECRBY) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.decrBy(key, delta);
			}

		});
	}
	
	public OperationResult<Long> del(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.DEL) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.del(key);
			}

		});
	}

	public OperationResult<byte[]> dump(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<byte[]>(key, OpName.DUMP) {

			@Override
			public byte[] execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.dump(key);
			}

		});
	}
	
	public OperationResult<Boolean> exists(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.EXISTS) {

			@Override
			public Boolean execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.exists(key);
			}

		});
	}
	
	public OperationResult<Long> expire(final String key, final Integer seconds) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.EXPIRE) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.expire(key, seconds);
			}

		});
	}
	
	public OperationResult<Long> expireAt(final String key, final Long unixTime) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.EXPIREAT) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.expireAt(key, unixTime);
			}

		});
	}
	
	public OperationResult<String> get(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.GET) {
			
			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.get(key);
			}
			
		});
	}
	
	public OperationResult<Boolean> getbit(final String key, final Long offset, final String value) throws DynoException {

		return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.GETBIT) {

			@Override
			public Boolean execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.getbit(key, offset);
			}

		});
	}

	public OperationResult<String> getrange(final String key, final Long startOffset, final Long endOffset) throws DynoException {

		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.GETRANGE) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.getrange(key, startOffset, endOffset);
			}

		});
	}

	public OperationResult<String> getSet(final String key, final String value) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.GETSET) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.getSet(key, value);
			}
			
		});
	}
	
	public OperationResult<Long> hdel(final String key, final String ... fields) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HDEL) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.hdel(key, fields);
			}

		});
	}

	public OperationResult<Boolean> hexists(final String key, final String field) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.HEXISTS) {

			@Override
			public Boolean execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.hexists(key, field);
			}

		});
	}

	public OperationResult<String> hget(final String key, final String field) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.HGET) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.hget(key, field);
			}

		});
	}

	public OperationResult<Map<String, String>> hgetAll(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Map<String, String>>(key, OpName.HGETALL) {

			@Override
			public Map<String, String> execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.hgetAll(key);
			}

		});
	}

	public OperationResult<Long> hincrBy(final String key, final String field, final Long value) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HINCRBY) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.hincrBy(key, field, value);
			}

		});
	}

	public OperationResult<Double> hincrByFloat(final String key, final String field, final Double increment) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Double>(key, OpName.HINCRBYFLOAT) {

			@Override
			public Double execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.hincrByFloat(key, field, increment);
			}

		});
	}

	public OperationResult<Long> hsetnx(final String key, final String field, final String value) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HSETNX) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.hsetnx(key, field, value);
			}

		});
	}
	
	public OperationResult<Set<String>> hkeys(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.HKEYS) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.hkeys(key);
			}

		});
	}
	
	public OperationResult<Long> hlen(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HLEN) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.hlen(key);
			}

		});
	}

	public OperationResult<List<String>> hmget(final String key, final String ... fields) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<List<String>>(key, OpName.HMGET) {

			@Override
			public List<String> execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.hmget(key, fields);
			}

		});
	}

	public OperationResult<String> hmset(final String key, final Map<String, String> hash) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.HMSET) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.hmset(key, hash);
			}

		});
	}

	public OperationResult<Long> hset(final String key, final String field, final String value) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HSET) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.hset(key, field, value);
			}

		});
	}

	public OperationResult<List<String>> hvals(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<List<String>>(key, OpName.HVALS) {

			@Override
			public List<String> execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.hvals(key);
			}

		});
	}

	public OperationResult<Long> incr(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.INCR) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.incr(key);
			}

		});
	}


	public OperationResult<Long> incrBy(final String key, final Long delta) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.INCRBY) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.incrBy(key, delta);
			}

		});
	}

	public OperationResult<Double> incrByFloat(final String key, final Double increment) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Double>(key, OpName.INCRBYFLOAT) {

			@Override
			public Double execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.incrByFloat(key, increment);
			}

		});
	}	
	
	public OperationResult<String> lindex(final String key, final Long index) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.LINDEX) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.lindex(key, index);
			}

		});
	}
	
	public OperationResult<Long> linsert(final String key, final LIST_POSITION where, final String pivot, final String value) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.LINSERT) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.linsert(key, where, pivot, value);
			}

		});
	}

	public OperationResult<Long> llen(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.LLEN) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.llen(key);
			}

		});
	}
	
	public OperationResult<String> lpop(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.LPOP) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.lpop(key);
			}

		});
	}

	public OperationResult<Long> lpush(final String key, final String ... values) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.LPUSH) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.lpush(key, values);
			}

		});
	}

	public OperationResult<Long> lpushx(final String key, final String ... values) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.LPUSHX) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.lpushx(key, values);
			}

		});
	}

	public OperationResult<List<String>> lrange(final String key, final Long start, final Long end) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<List<String>>(key, OpName.LRANGE) {

			@Override
			public List<String> execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.lrange(key, start, end);
			}

		});
	}

	public OperationResult<Long> lrem(final String key, final Long count, final String value) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.LREM) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.lrem(key, count, value);
			}

		});
	}
	
	public OperationResult<String> lset(final String key, final Long index, final String value) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.LSET) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.lset(key, index, value);
			}

		});
	}

	public OperationResult<String> ltrim(final String key, final Long start, final Long end) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.LTRIM) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.ltrim(key, start, end);
			}

		});
	}
	
	public OperationResult<Long> persist(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.PERSIST) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.persist(key);
			}

		});
	}	
	
	public OperationResult<Long> pexpire(final String key, final Integer milliseconds) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.PEXPIRE) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.pexpire(key, milliseconds);
			}

		});
	}
	
	public OperationResult<Long> pexpireAt(final String key, final Long millisecondsTimestamp) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.PEXPIREAT) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.pexpireAt(key, millisecondsTimestamp);
			}

		});
	}
	
	public OperationResult<String> psetex(final String key, final Integer milliseconds, final String value) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.PSETEX) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.psetex(key, milliseconds, value);
			}

		});
	}

	public OperationResult<Long> pttl(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.PTTL) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.pttl(key);
			}

		});
	}
	
	public OperationResult<String> restore(final String key, final Integer ttl, final byte[] serializedValue) throws DynoException {

		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.RESTORE) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.restore(key, ttl, serializedValue);
			}

		});
	}

	public OperationResult<String> rpop(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.RPOP) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.rpop(key);
			}
			
		});
	}

	public OperationResult<String> rpoplpush(final String srckey, final String dstkey) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(srckey, OpName.RPOPLPUSH) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.rpoplpush(srckey, dstkey);
			}

		});
	}
	
	public OperationResult<Long> rpush(final String key, final String ... values) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.RPUSH) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.rpush(key, values);
			}

		});
	}

	public OperationResult<Long> rpushx(final String key, final String ... values) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.RPUSHX) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.rpushx(key, values);
			}

		});
	}
	
	public OperationResult<Long> sadd(final String key, final String ... members) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SADD) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.sadd(key, members);
			}

		});
	}
	
	public OperationResult<Long> scard(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SCARD) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.scard(key);
			}

		});
	}

	public OperationResult<Set<String>> sdiff(final String ... keys) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(keys[0], OpName.SDIFF) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.sdiff(keys);
			}

		});
	}

	public OperationResult<Long> sdiffstore(final String dstkey, final String ... keys) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(dstkey, OpName.SDIFFSTORE) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.sdiffstore(dstkey, keys);
			}

		});
	}

	public OperationResult<String> set(final String key, final String value) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.SET) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.set(key, value);
			}
			
		});
	}
	
	public OperationResult<Boolean> setbit(final String key, final Long offset, final Boolean value) throws DynoException {

		return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.SETBIT) {

			@Override
			public Boolean execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.setbit(key, offset, value);
			}

		});
	}

	public OperationResult<String> setex(final String key, final Integer seconds, final String value) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.SETEX) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.setex(key, seconds, value);
			}

		});
	}

	public OperationResult<Long> setnx(final String key, final String value) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SETNX) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.setnx(key, value);
			}

		});
	}

	public OperationResult<Long> setrange(final String key, final Long offset, final String value) throws DynoException {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SETRANGE) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.setrange(key, offset, value);
			}

		});
	}
	
	public OperationResult<Set<String>> sinter(final String ... keys) throws DynoException {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(keys[0], OpName.SINTER) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.sinter(keys);
			}

		});
	}

	public OperationResult<Long> sinterstore(final String dstkey, final String ... keys) throws DynoException {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(dstkey, OpName.SINTERSTORE) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.sinterstore(dstkey, keys);
			}

		});
	}

	public OperationResult<Boolean> sismember(final String key, final String member) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.SISMEMBER) {

			@Override
			public Boolean execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.sismember(key, member);
			}

		});
	}

	public OperationResult<Set<String>> smembers(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.SMEMBERS) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.smembers(key);
			}

		});
	}
	
	public OperationResult<Long> smove(final String srckey, final String dstkey, final String member) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(srckey, OpName.SMOVE) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.smove(srckey, dstkey, member);
			}

		});
	}
	
	public OperationResult<String> spop(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.SPOP) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.spop(key);
			}

		});
	}
	public OperationResult<String> srandmember(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.SRANDMEMBER) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.srandmember(key);
			}

		});
	}
	public OperationResult<Long> srem(final String key, final String ... members) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SREM) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.srem(key, members);
			}

		});
	}
	
	public OperationResult<Long> strlen(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.STRLEN) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.strlen(key);
			}

		});
	}

	public OperationResult<Set<String>> sunion(final String ... keys) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(keys[0], OpName.SUNION) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.sunion(keys);
			}

		});
	}

	public OperationResult<Long> sunionstore(final String dstkey, final String ... keys) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(dstkey, OpName.SUNIONSTORE) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.sunionstore(dstkey, keys);
			}

		});
	}
	
	public OperationResult<Long> ttl(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.TTL) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.ttl(key);
			}

		});
	}
	
	public OperationResult<String> type(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.TYPE) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.type(key);
			}

		});
	}
	
	public OperationResult<Long> zcard(final String key) throws DynoException {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZCARD) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.zcard(key);
			}

		});
	}
	
	public OperationResult<Long> zcount(final String key, final Double min, final Double max) throws DynoException {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZCOUNT) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.zcount(key, min, max);
			}

		});
	}
	
	public OperationResult<Double> zincrby(final String key, final Double score, final String member) throws DynoException {

		return connPool.executeWithFailover(new BaseKeyOperation<Double>(key, OpName.ZINCRBY) {

			@Override
			public Double execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.zincrby(key, score, member);
			}

		});
	}
	
	public OperationResult<Long> zrank(final String key, final String member) throws DynoException {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZRANK) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.zrank(key, member);
			}

		});
	}
	
	public OperationResult<Long> zremrangeByRank(final String key, final Long start, final Long end) throws DynoException {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZREMRANGEBYRANK) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.zremrangeByRank(key, start, end);
			}

		});
	}

	public OperationResult<Long> zremrangeByScore(final String key, final Double start, final Double end) throws DynoException {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZREMRANGEBYSCORE) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.zremrangeByScore(key, start, end);
			}

		});
	}

	public OperationResult<Long> zrevrank(final String key, final String member) throws DynoException {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZREVRANK) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.zrevrank(key, member);
			}

		});
	}

	public OperationResult<Double> zscore(final String key, final String member) throws DynoException {

		return connPool.executeWithFailover(new BaseKeyOperation<Double>(key, OpName.ZSCORE) {

			@Override
			public Double execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.zscore(key, member);
			}

		});
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

		public Builder withCPConfig(ConnectionPoolConfigurationImpl config) {
			cpConfig = config;
			return this;
		}

		public DynoJedisClient build() {

			assert(appName != null);
			assert(clusterName != null);
			assert(cpConfig != null);
			
			DynoCPMonitor cpMonitor = new DynoCPMonitor(appName);
			DynoOPMonitor opMonitor = new DynoOPMonitor(appName);
			
			JedisConnectionFactory connFactory = new JedisConnectionFactory(opMonitor);

			ConnectionPoolImpl<Jedis> pool = new ConnectionPoolImpl<Jedis>(connFactory, cpConfig, cpMonitor);
			
			try {
				pool.start().get();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
			final DynoJedisClient client = new DynoJedisClient(appName, pool);
			return client;
		}
		
		public static Builder withName(String name) {
			return new Builder(name);
		}
	}

}
