package com.netflix.dyno.jedis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.BitOP;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.MultiKeyCommands;
import redis.clients.jedis.SortingParams;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.ZParams;

import com.netflix.dyno.connectionpool.ConnectionContext;
import com.netflix.dyno.connectionpool.ConnectionPool;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.exception.DynoException;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl;
import com.netflix.dyno.contrib.DynoCPMonitor;
import com.netflix.dyno.contrib.DynoOPMonitor;
import com.netflix.dyno.contrib.EurekaHostsSupplier;

public class DynoJedisClient implements JedisCommands, MultiKeyCommands {
	
	private final ConnectionPool<Jedis> connPool;
	
	public DynoJedisClient(String name, ConnectionPool<Jedis> pool) {
		this.connPool = pool;
	}
	
	private enum OpName {
		 APPEND, BITCOUNT, BLPOP, BRPOP, DECR, DECRBY, DEL, DUMP, ECHO, EXISTS, EXPIRE, EXPIREAT, GET, GETBIT, GETRANGE, GETSET, 
		 HDEL, HEXISTS,  HGET, HGETALL, HINCRBY, HINCRBYFLOAT, HKEYS, HLEN, HMGET, HMSET, HSET, HSETNX, HVALS, 
		 INCR, INCRBY, INCRBYFLOAT, LINDEX, LINSERT, LLEN, LPOP, LPUSH, LPUSHX, LRANGE, LREM, LSET, LTRIM, 
		 MOVE, PERSIST, PEXPIRE, PEXPIREAT, PSETEX, PTTL, RESTORE, RPOP, RPOPLPUSH, RPUSH, RPUSHX, 
		 SADD, SCARD, SDIFF, SDIFFSTORE, SET, SETBIT, SETEX, SETNX, SETRANGE, SINTER, SINTERSTORE, SISMEMBER, SMEMBERS, 
		 SMOVE, SORT, SPOP, SRANDMEMBER, SREM, STRLEN, SUBSTR, SUNION, SUNIONSTORE, TTL, TYPE, 
		 ZADD, ZCARD, ZCOUNT, ZINCRBY, ZRANGE, ZRANGEWITHSCORES, ZRANK, ZRANGEBYSCORE, ZRANGEBYSCOREWITHSCORES, ZREM, ZREMRANGEBYRANK, 
		 ZREMRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYSCORE, ZREVRANGEBYSCOREWITHSCORES, ZREVRANGEWITHSCORES, ZREVRANK, ZSCORE
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
	
	@Override
	public Long append(final String key, final String value)  {
		return d_append(key, value).getResult();
	}

	public OperationResult<Long> d_append(final String key, final String value)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.APPEND) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.append(key, value);
			}
			
		});
	}
	
	@Override
	public Long decr(final String key)  {
		return d_decr(key).getResult();
	}
	
	public OperationResult<Long> d_decr(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.DECR) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.decr(key);
			}

		});
	}
	
	@Override
	public Long decrBy(final String key, final long delta)  {
		return d_decrBy(key, delta).getResult();
	}

	public OperationResult<Long> d_decrBy(final String key, final Long delta)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.DECRBY) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.decrBy(key, delta);
			}

		});
	}
	
	@Override
	public Long del(final String key)  {
		return d_del(key).getResult();
	}
	
	public OperationResult<Long> d_del(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.DEL) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.del(key);
			}

		});
	}

	public byte[] dump(final String key) {
		return d_dump(key).getResult();
	}
	
	public OperationResult<byte[]> d_dump(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<byte[]>(key, OpName.DUMP) {

			@Override
			public byte[] execute(Jedis client, ConnectionContext state)  {
				return client.dump(key);
			}

		});
	}
	
	@Override
	public Boolean exists(final String key) {
		return d_exists(key).getResult();
	}
	
	public OperationResult<Boolean> d_exists(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.EXISTS) {

			@Override
			public Boolean execute(Jedis client, ConnectionContext state)  {
				return client.exists(key);
			}

		});
	}

	@Override
	public Long expire(final String key, final int seconds) {
		return d_expire(key, seconds).getResult();
	}	

	public OperationResult<Long> d_expire(final String key, final Integer seconds)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.EXPIRE) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.expire(key, seconds);
			}

		});
	}

	@Override
	public Long expireAt(final String key, final long unixTime)  {
		return d_expireAt(key, unixTime).getResult();
	}
	
	public OperationResult<Long> d_expireAt(final String key, final Long unixTime)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.EXPIREAT) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.expireAt(key, unixTime);
			}

		});
	}

	@Override
	public String get(final String key)  {
		return d_get(key).getResult();
	}

	public OperationResult<String> d_get(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.GET) {
			
			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.get(key);
			}
			
		});
	}

	@Override
	public Boolean getbit(final String key, final long offset)  {
		return d_getbit(key, offset).getResult();
	}

	public OperationResult<Boolean> d_getbit(final String key, final Long offset)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.GETBIT) {

			@Override
			public Boolean execute(Jedis client, ConnectionContext state)  {
				return client.getbit(key, offset);
			}

		});
	}

	@Override
	public String getrange(final String key, final long startOffset, final long endOffset)  {
		return d_getrange(key, startOffset, endOffset).getResult();
	}

	public OperationResult<String> d_getrange(final String key, final Long startOffset, final Long endOffset)  {

		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.GETRANGE) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.getrange(key, startOffset, endOffset);
			}

		});
	}

	@Override
	public String getSet(final String key, final String value)  {
		return d_getSet(key, value).getResult();
	}

	public OperationResult<String> d_getSet(final String key, final String value)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.GETSET) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.getSet(key, value);
			}
			
		});
	}

	@Override
	public Long hdel(final String key, final String ... fields)  {
		return d_hdel(key, fields).getResult();
	}

	public OperationResult<Long> d_hdel(final String key, final String ... fields)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HDEL) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.hdel(key, fields);
			}

		});
	}

	@Override
	public Boolean hexists(final String key, final String field)  {
		return d_hexists(key, field).getResult();
	}

	public OperationResult<Boolean> d_hexists(final String key, final String field)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.HEXISTS) {

			@Override
			public Boolean execute(Jedis client, ConnectionContext state)  {
				return client.hexists(key, field);
			}

		});
	}

	@Override
	public String hget(final String key, final String field)  {
		return d_hget(key, field).getResult();
	}
	
	public OperationResult<String> d_hget(final String key, final String field)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.HGET) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.hget(key, field);
			}

		});
	}

	@Override
	public Map<String, String> hgetAll(final String key)  {
		return d_hgetAll(key).getResult();
	}
	
	public OperationResult<Map<String, String>> d_hgetAll(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Map<String, String>>(key, OpName.HGETALL) {

			@Override
			public Map<String, String> execute(Jedis client, ConnectionContext state)  {
				return client.hgetAll(key);
			}

		});
	}

	@Override
	public Long hincrBy(final String key, final String field, final long value) {
		return d_hincrBy(key, field, value).getResult();
	}
	
	public OperationResult<Long> d_hincrBy(final String key, final String field, final Long value)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HINCRBY) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.hincrBy(key, field, value);
			}

		});
	}

	@Override
	public Long hsetnx(final String key, final String field, final String value)  {
		return d_hsetnx(key, field, value).getResult();
	}
	
	public OperationResult<Long> d_hsetnx(final String key, final String field, final String value)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HSETNX) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.hsetnx(key, field, value);
			}

		});
	}

	@Override
	public Set<String> hkeys(final String key)  {
		return d_hkeys(key).getResult();
	}
	
	public OperationResult<Set<String>> d_hkeys(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.HKEYS) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state)  {
				return client.hkeys(key);
			}

		});
	}

	@Override
	public Long hlen(final String key)  {
		return d_hlen(key).getResult();
	}
	
	public OperationResult<Long> d_hlen(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HLEN) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.hlen(key);
			}

		});
	}

	@Override
	public List<String> hmget(final String key, final String ... fields)  {
		return d_hmget(key, fields).getResult();
	}
	
	public OperationResult<List<String>> d_hmget(final String key, final String ... fields)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<List<String>>(key, OpName.HMGET) {

			@Override
			public List<String> execute(Jedis client, ConnectionContext state)  {
				return client.hmget(key, fields);
			}

		});
	}

	@Override
	public String hmset(final String key, final Map<String, String> hash)  {
		return d_hmset(key, hash).getResult();
	}

	public OperationResult<String> d_hmset(final String key, final Map<String, String> hash)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.HMSET) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.hmset(key, hash);
			}

		});
	}

	@Override
	public Long hset(final String key, final String field, final String value)  {
		return d_hset(key, field, value).getResult();
	}
	
	public OperationResult<Long> d_hset(final String key, final String field, final String value)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HSET) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.hset(key, field, value);
			}

		});
	}

	@Override
	public List<String> hvals(final String key)  {
		return d_hvals(key).getResult();
	}
	
	public OperationResult<List<String>> d_hvals(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<List<String>>(key, OpName.HVALS) {

			@Override
			public List<String> execute(Jedis client, ConnectionContext state)  {
				return client.hvals(key);
			}

		});
	}

	@Override
	public Long incr(final String key)  {
		return d_incr(key).getResult();
	}
	
	public OperationResult<Long> d_incr(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.INCR) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.incr(key);
			}

		});
	}

	@Override
	public Long incrBy(final String key, final long delta)  {
		return d_incrBy(key, delta).getResult();
	}
	
	public OperationResult<Long> d_incrBy(final String key, final Long delta)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.INCRBY) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.incrBy(key, delta);
			}

		});
	}

	public Double incrByFloat(final String key, final double increment)  {
		return d_incrByFloat(key, increment).getResult();
	}
	
	public OperationResult<Double> d_incrByFloat(final String key, final Double increment)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Double>(key, OpName.INCRBYFLOAT) {

			@Override
			public Double execute(Jedis client, ConnectionContext state)  {
				return client.incrByFloat(key, increment);
			}

		});
	}	

	@Override
	public String lindex(final String key, final long index)  {
	    return d_lindex(key, index).getResult();
	}

	public OperationResult<String> d_lindex(final String key, final Long index)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.LINDEX) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.lindex(key, index);
			}

		});
	}

	@Override
	public Long linsert(final String key, final LIST_POSITION where, final String pivot, final String value)  {
	    return d_linsert(key, where, pivot, value).getResult();
	}

	public OperationResult<Long> d_linsert(final String key, final LIST_POSITION where, final String pivot, final String value)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.LINSERT) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.linsert(key, where, pivot, value);
			}

		});
	}

	@Override
	public Long llen(final String key)  {
	    return d_llen(key).getResult();
	}

	public OperationResult<Long> d_llen(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.LLEN) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.llen(key);
			}

		});
	}

	@Override
	public String lpop(final String key)  {
	    return d_lpop(key).getResult();
	}

	public OperationResult<String> d_lpop(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.LPOP) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.lpop(key);
			}

		});
	}

	@Override
	public Long lpush(final String key, final String ... values)  {
	    return d_lpush(key, values).getResult();
	}

	public OperationResult<Long> d_lpush(final String key, final String ... values)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.LPUSH) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.lpush(key, values);
			}

		});
	}

	@Override
	public Long lpushx(final String key, final String ... values)  {
	    return d_lpushx(key, values).getResult();
	}

	public OperationResult<Long> d_lpushx(final String key, final String ... values)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.LPUSHX) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.lpushx(key, values);
			}

		});
	}

	@Override
	public List<String> lrange(final String key, final long start, final long end)  {
	    return d_lrange(key, start, end).getResult();
	}

	public OperationResult<List<String>> d_lrange(final String key, final Long start, final Long end)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<List<String>>(key, OpName.LRANGE) {

			@Override
			public List<String> execute(Jedis client, ConnectionContext state)  {
				return client.lrange(key, start, end);
			}

		});
	}

	@Override
	public Long lrem(final String key, final long count, final String value)  {
	    return d_lrem(key, count, value).getResult();
	}

	public OperationResult<Long> d_lrem(final String key, final Long count, final String value)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.LREM) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.lrem(key, count, value);
			}

		});
	}

	@Override
	public String lset(final String key, final long index, final String value)  {
	    return d_lset(key, index, value).getResult();
	}

	public OperationResult<String> d_lset(final String key, final Long index, final String value)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.LSET) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.lset(key, index, value);
			}

		});
	}

	@Override
	public String ltrim(final String key, final long start, final long end)  {
	    return d_ltrim(key, start, end).getResult();
	}

	public OperationResult<String> d_ltrim(final String key, final Long start, final Long end)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.LTRIM) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.ltrim(key, start, end);
			}

		});
	}

        @Override
	public Long persist(final String key)  {
	    return d_persist(key).getResult();
	}

	public OperationResult<Long> d_persist(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.PERSIST) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.persist(key);
			}

		});
	}	

	public Long pexpire(final String key, final int milliseconds)  {
	    return d_pexpire(key, milliseconds).getResult();
	}

	public OperationResult<Long> d_pexpire(final String key, final Integer milliseconds)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.PEXPIRE) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.pexpire(key, milliseconds);
			}

		});
	}

	public Long pexpireAt(final String key, final long millisecondsTimestamp)  {
	    return d_pexpireAt(key, millisecondsTimestamp).getResult();
	}

	public OperationResult<Long> d_pexpireAt(final String key, final Long millisecondsTimestamp)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.PEXPIREAT) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.pexpireAt(key, millisecondsTimestamp);
			}

		});
	}

	public String psetex(final String key, final int milliseconds, final String value)  {
	    return d_psetex(key, milliseconds, value).getResult();
	}

	public OperationResult<String> d_psetex(final String key, final Integer milliseconds, final String value)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.PSETEX) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.psetex(key, milliseconds, value);
			}

		});
	}

	public Long pttl(final String key)  {
	    return d_pttl(key).getResult();
	}

	public OperationResult<Long> d_pttl(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.PTTL) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.pttl(key);
			}

		});
	}

	public String restore(final String key, final Integer ttl, final byte[] serializedValue)  {
	    return d_restore(key, ttl, serializedValue).getResult();
	}

	public OperationResult<String> d_restore(final String key, final Integer ttl, final byte[] serializedValue)  {

		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.RESTORE) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.restore(key, ttl, serializedValue);
			}

		});
	}

	public String rpop(final String key)  {
	    return d_rpop(key).getResult();
	}

	public OperationResult<String> d_rpop(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.RPOP) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.rpop(key);
			}
			
		});
	}

	public String rpoplpush(final String srckey, final String dstkey)  {
	    return d_rpoplpush(srckey, dstkey).getResult();
	}

	public OperationResult<String> d_rpoplpush(final String srckey, final String dstkey)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(srckey, OpName.RPOPLPUSH) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.rpoplpush(srckey, dstkey);
			}

		});
	}

	public Long rpush(final String key, final String ... values)  {
	    return d_rpush(key, values).getResult();
	}

	public OperationResult<Long> d_rpush(final String key, final String ... values)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.RPUSH) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.rpush(key, values);
			}

		});
	}

	@Override
	public Long rpushx(final String key, final String ... values)  {
	    return d_rpushx(key, values).getResult();
	}

	public OperationResult<Long> d_rpushx(final String key, final String ... values)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.RPUSHX) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.rpushx(key, values);
			}

		});
	}

	@Override
	public Long sadd(final String key, final String ... members)  {
	    return d_sadd(key, members).getResult();
	}

	public OperationResult<Long> d_sadd(final String key, final String ... members)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SADD) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.sadd(key, members);
			}

		});
	}

	@Override
	public Long scard(final String key)  {
	    return d_scard(key).getResult();
	}

	public OperationResult<Long> d_scard(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SCARD) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.scard(key);
			}

		});
	}

	public Set<String> sdiff(final String ... keys)  {
	    return d_sdiff(keys).getResult();
	}

	public OperationResult<Set<String>> d_sdiff(final String ... keys)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(keys[0], OpName.SDIFF) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state)  {
				return client.sdiff(keys);
			}

		});
	}

	public Long sdiffstore(final String dstkey, final String ... keys)  {
	    return d_sdiffstore(dstkey, keys).getResult();
	}

	public OperationResult<Long> d_sdiffstore(final String dstkey, final String ... keys)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(dstkey, OpName.SDIFFSTORE) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.sdiffstore(dstkey, keys);
			}

		});
	}

	@Override
	public String set(final String key, final String value)  {
	    return d_set(key, value).getResult();
	}

	public OperationResult<String> d_set(final String key, final String value)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.SET) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.set(key, value);
			}
			
		});
	}

	@Override
	public Boolean setbit(final String key, final long offset, final boolean value)  {
	    return d_setbit(key, offset, value).getResult();
	}

	public OperationResult<Boolean> d_setbit(final String key, final Long offset, final Boolean value)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.SETBIT) {

			@Override
			public Boolean execute(Jedis client, ConnectionContext state)  {
				return client.setbit(key, offset, value);
			}

		});
	}
	

	@Override
	public Boolean setbit(String key, long offset, String value) {
		return d_setbit(key, offset, value).getResult();
	}

	public OperationResult<Boolean> d_setbit(final String key, final Long offset, final String value)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.SETBIT) {

			@Override
			public Boolean execute(Jedis client, ConnectionContext state)  {
				return client.setbit(key, offset, value);
			}

		});
	}

	@Override
	public String setex(final String key, final int seconds, final String value)  {
	    return d_setex(key, seconds, value).getResult();
	}

	public OperationResult<String> d_setex(final String key, final Integer seconds, final String value)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.SETEX) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.setex(key, seconds, value);
			}

		});
	}

	@Override
	public Long setnx(final String key, final String value)  {
	    return d_setnx(key, value).getResult();
	}

	public OperationResult<Long> d_setnx(final String key, final String value)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SETNX) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.setnx(key, value);
			}

		});
	}

	@Override
	public Long setrange(final String key, final long offset, final String value)  {
	    return d_setrange(key, offset, value).getResult();
	}

	public OperationResult<Long> d_setrange(final String key, final Long offset, final String value)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SETRANGE) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.setrange(key, offset, value);
			}

		});
	}

	@Override
	public Boolean sismember(final String key, final String member)  {
	    return d_sismember(key, member).getResult();
	}

	public OperationResult<Boolean> d_sismember(final String key, final String member)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.SISMEMBER) {

			@Override
			public Boolean execute(Jedis client, ConnectionContext state)  {
				return client.sismember(key, member);
			}

		});
	}

	@Override
	public Set<String> smembers(final String key)  {
	    return d_smembers(key).getResult();
	}

	public OperationResult<Set<String>> d_smembers(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.SMEMBERS) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state)  {
				return client.smembers(key);
			}

		});
	}

	public Long smove(final String srckey, final String dstkey, final String member)  {
	    return d_smove(srckey, dstkey, member).getResult();
	}

	public OperationResult<Long> d_smove(final String srckey, final String dstkey, final String member)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(srckey, OpName.SMOVE) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.smove(srckey, dstkey, member);
			}

		});
	}
	
	@Override
	public List<String> sort(String key) {
		return d_sort(key).getResult();
	}

	public OperationResult<List<String>> d_sort(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<List<String>>(key, OpName.SORT) {

			@Override
			public List<String> execute(Jedis client, ConnectionContext state)  {
				return client.sort(key);
			}

		});
	}


	@Override
	public List<String> sort(String key, SortingParams sortingParameters) {
		return d_sort(key, sortingParameters).getResult();
	}

	public OperationResult<List<String>> d_sort(final String key, final SortingParams sortingParameters)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<List<String>>(key, OpName.SORT) {

			@Override
			public List<String> execute(Jedis client, ConnectionContext state)  {
				return client.sort(key, sortingParameters);
			}

		});
	}


	@Override
	public String spop(final String key)  {
	    return d_spop(key).getResult();
	}

	public OperationResult<String> d_spop(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.SPOP) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.spop(key);
			}

		});
	}

	@Override
	public String srandmember(final String key)  {
	    return d_srandmember(key).getResult();
	}

	public OperationResult<String> d_srandmember(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.SRANDMEMBER) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.srandmember(key);
			}

		});
	}
	
	@Override
	public Long srem(final String key, final String ... members)  {
	    return d_srem(key, members).getResult();
	}

	public OperationResult<Long> d_srem(final String key, final String ... members)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SREM) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.srem(key, members);
			}

		});
	}

	@Override
	public Long strlen(final String key)  {
	    return d_strlen(key).getResult();
	}

	public OperationResult<Long> d_strlen(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.STRLEN) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.strlen(key);
			}

		});
	}


	@Override
	public String substr(String key, int start, int end) {
		return d_substr(key, start, end).getResult();
	}

	public OperationResult<String> d_substr(final String key, final Integer start, final Integer end)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.SUBSTR) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.substr(key, start, end);
			}

		});
	}
	
	@Override
	public Long ttl(final String key)  {
	    return d_ttl(key).getResult();
	}

	public OperationResult<Long> d_ttl(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.TTL) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.ttl(key);
			}

		});
	}

	@Override
	public String type(final String key)  {
	    return d_type(key).getResult();
	}

	public OperationResult<String> d_type(final String key)  {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.TYPE) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.type(key);
			}

		});
	}
	

	@Override
	public Long zadd(String key, double score, String member) {
		return d_zadd(key, score, member).getResult();
	}

	public OperationResult<Long> d_zadd(final String key, final Double score, final String member)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZADD) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.zadd(key, score, member);
			}

		});
	}
	

	@Override
	public Long zadd(String key, Map<Double, String> scoreMembers) {
		return d_zadd(key, scoreMembers).getResult();
	}

	public OperationResult<Long> d_zadd(final String key, final Map<Double, String> scoreMembers)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZADD) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.zadd(key, scoreMembers);
			}

		});
	}

	@Override
	public Long zcard(final String key)  {
	    return d_zcard(key).getResult();
	}

	public OperationResult<Long> d_zcard(final String key)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZCARD) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.zcard(key);
			}

		});
	}

	@Override
	public Long zcount(final String key, final double min, final double max)  {
	    return d_zcount(key, min, max).getResult();
	}

	public OperationResult<Long> d_zcount(final String key, final Double min, final Double max)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZCOUNT) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.zcount(key, min, max);
			}

		});
	}
	

	@Override
	public Long zcount(String key, String min, String max) {
		return d_zcount(key, min, max).getResult();
	}
	
	public OperationResult<Long> d_zcount(final String key, final String min, final String max)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZCOUNT) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.zcount(key, min, max);
			}

		});
	}

	@Override
	public Double zincrby(final String key, final double score, final String member)  {
	    return d_zincrby(key, score, member).getResult();
	}

	public OperationResult<Double> d_zincrby(final String key, final Double score, final String member)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Double>(key, OpName.ZINCRBY) {

			@Override
			public Double execute(Jedis client, ConnectionContext state)  {
				return client.zincrby(key, score, member);
			}

		});
	}
	

	@Override
	public Set<String> zrange(String key, long start, long end) {
		return d_zrange(key, start, end).getResult();
	}

	public OperationResult<Set<String>> d_zrange(final String key, final Long start, final Long end)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZRANGE) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state)  {
				return client.zrange(key, start, end);
			}

		});
	}

	@Override
	public Long zrank(final String key, final String member)  {
	    return d_zrank(key, member).getResult();
	}

	public OperationResult<Long> d_zrank(final String key, final String member)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZRANK) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.zrank(key, member);
			}

		});
	}
	

	@Override
	public Long zrem(String key, String ... member) {
		return d_zrem(key, member).getResult();
	}

	public OperationResult<Long> d_zrem(final String key, final String ...member)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZREM) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.zrem(key, member);
			}

		});
	}

	@Override
	public Long zremrangeByRank(final String key, final long start, final long end)  {
	    return d_zremrangeByRank(key, start, end).getResult();
	}

	public OperationResult<Long> d_zremrangeByRank(final String key, final Long start, final Long end)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZREMRANGEBYRANK) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.zremrangeByRank(key, start, end);
			}

		});
	}

	@Override
	public Long zremrangeByScore(final String key, final double start, final double end)  {
	    return d_zremrangeByScore(key, start, end).getResult();
	}

	public OperationResult<Long> d_zremrangeByScore(final String key, final Double start, final Double end)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZREMRANGEBYSCORE) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.zremrangeByScore(key, start, end);
			}

		});
	}


	@Override
	public Set<String> zrevrange(String key, long start, long end) {
		return d_zrevrange(key, start, end).getResult();
	}

	public OperationResult<Set<String>> d_zrevrange(final String key, final Long start, final Long end)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZREVRANGE) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state)  {
				return client.zrevrange(key, start, end);
			}

		});
	}

	@Override
	public Long zrevrank(final String key, final String member)  {
	    return d_zrevrank(key, member).getResult();
	}

	public OperationResult<Long> d_zrevrank(final String key, final String member)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZREVRANK) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.zrevrank(key, member);
			}

		});
	}


	@Override
	public Set<Tuple> zrangeWithScores(String key, long start, long end) {
		return d_zrangeWithScores(key, start, end).getResult();
	}

	public OperationResult<Set<Tuple>> d_zrangeWithScores(final String key, final Long start, final Long end)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZRANGEWITHSCORES) {

			@Override
			public Set<Tuple> execute(Jedis client, ConnectionContext state)  {
				return client.zrangeWithScores(key, start, end);
			}

		});
	}


	@Override
	public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
		return d_zrevrangeWithScores(key, start, end).getResult();
	}

	public OperationResult<Set<Tuple>> d_zrevrangeWithScores(final String key, final Long start, final Long end)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZREVRANGEWITHSCORES) {

			@Override
			public Set<Tuple> execute(Jedis client, ConnectionContext state)  {
				return client.zrevrangeWithScores(key, start, end);
			}

		});
	}

	@Override
	public Double zscore(final String key, final String member)  {
	    return d_zscore(key, member).getResult();
	}

	public OperationResult<Double> d_zscore(final String key, final String member)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Double>(key, OpName.ZSCORE) {

			@Override
			public Double execute(Jedis client, ConnectionContext state)  {
				return client.zscore(key, member);
			}

		});
	}

	@Override
	public Set<String> zrangeByScore(String key, double min, double max) {
		return d_zrangeByScore(key, min, max).getResult();
	}

	public OperationResult<Set<String>> d_zrangeByScore(final String key, final Double min, final Double max)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZRANGEBYSCORE) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state)  {
				return client.zrangeByScore(key, min, max);
			}

		});
	}

	@Override
	public Set<String> zrangeByScore(String key, String min, String max) {
		return d_zrangeByScore(key, min, max).getResult();
	}

	public OperationResult<Set<String>> d_zrangeByScore(final String key, final String min, final String max)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZRANGEBYSCORE) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state)  {
				return client.zrangeByScore(key, min, max);
			}

		});
	}
	
	@Override
	public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
		return d_zrangeByScore(key, min, max, offset, count).getResult();
	}

	public OperationResult<Set<String>> d_zrangeByScore(final String key, final Double min, final Double max, final Integer offset, final Integer count)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZRANGEBYSCORE) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state)  {
				return client.zrangeByScore(key, min, max, offset, count);
			}

		});
	}
	
	@Override
	public Set<String> zrevrangeByScore(String key, String max, String min) {
		return d_zrevrangeByScore(key, max, min).getResult();
	}

	public OperationResult<Set<String>> d_zrevrangeByScore(final String key, final String max, final String min)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZREVRANGEBYSCORE) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state)  {
				return client.zrevrangeByScore(key, max, min);
			}

		});
	}
	
	@Override
	public Set<String> zrangeByScore(String key, String min, String max, int offset, int count) {
		return d_zrangeByScore(key, min, max, offset, count).getResult();
	}

	public OperationResult<Set<String>> d_zrangeByScore(final String key, final String min, final String max, final Integer offset, final Integer count)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZRANGEBYSCORE) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state)  {
				return client.zrangeByScore(key, min, max, offset, count);
			}

		});
	}
	
	@Override
	public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
		return d_zrevrangeByScore(key, max, min, offset, count).getResult();
	}

	public OperationResult<Set<String>> d_zrevrangeByScore(final String key, final Double max, final Double min, final Integer offset, final Integer count)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZREVRANGEBYSCORE) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state)  {
				return client.zrevrangeByScore(key, max, min, offset, count);
			}

		});
	}
	
	@Override
	public Set<String> zrevrangeByScore(String key, double max, double min) {
		return d_zrevrangeByScore(key, max, min).getResult();
	}

	public OperationResult<Set<String>> d_zrevrangeByScore(final String key, final Double max, final Double min)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZREVRANGEBYSCORE) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state)  {
				return client.zrevrangeByScore(key, max, min);
			}

		});
	}

	
	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max) {
		return d_zrangeByScoreWithScores(key, min, max).getResult();
	}

	public OperationResult<Set<Tuple>> d_zrangeByScoreWithScores(final String key, final Double min, final Double max)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZREVRANGEBYSCORE) {

			@Override
			public Set<Tuple> execute(Jedis client, ConnectionContext state)  {
				return client.zrangeByScoreWithScores(key, min, max);
			}

		});
	}
	
	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min) {
		return d_zrevrangeByScoreWithScores(key, min, max).getResult();
	}

	public OperationResult<Set<Tuple>> d_zrevrangeByScoreWithScores(final String key, final Double max, final Double min)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZREVRANGEBYSCOREWITHSCORES) {

			@Override
			public Set<Tuple> execute(Jedis client, ConnectionContext state)  {
				return client.zrevrangeByScoreWithScores(key, max, min);
			}

		});
	}
	
	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, double min, double max, int offset, int count) {
		return d_zrangeByScoreWithScores(key, min, max, offset, count).getResult();
	}

	public OperationResult<Set<Tuple>> d_zrangeByScoreWithScores(final String key, final Double min, final Double max, final Integer offset, final Integer count)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZRANGEBYSCOREWITHSCORES) {

			@Override
			public Set<Tuple> execute(Jedis client, ConnectionContext state)  {
				return client.zrangeByScoreWithScores(key, min, max, offset, count);
			}

		});
	}
	
	@Override
	public Set<String> zrevrangeByScore(String key, String max, String min, int offset, int count) {
		return d_zrevrangeByScore(key, max, min, offset, count).getResult();
	}

	public OperationResult<Set<String>> d_zrevrangeByScore(final String key, final String max, final String min, final Integer offset, final Integer count)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.ZREVRANGEBYSCORE) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state)  {
				return client.zrevrangeByScore(key, max, min, offset, count);
			}

		});
	}
	
	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max) {
		return d_zrangeByScoreWithScores(key, min, max).getResult();
	}

	public OperationResult<Set<Tuple>> d_zrangeByScoreWithScores(final String key, final String min, final String max) {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZRANGEBYSCOREWITHSCORES) {

			@Override
			public Set<Tuple> execute(Jedis client, ConnectionContext state)  {
				return client.zrangeByScoreWithScores(key, min, max);
			}

		});
	}
	
	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min) {
		return d_zrevrangeByScoreWithScores(key, max, min).getResult();
	}

	public OperationResult<Set<Tuple>> d_zrevrangeByScoreWithScores(final String key, final String max, final String min)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZREVRANGEBYSCOREWITHSCORES) {

			@Override
			public Set<Tuple> execute(Jedis client, ConnectionContext state)  {
				return client.zrevrangeByScoreWithScores(key, max, min);
			}

		});
	}
	
	@Override
	public Set<Tuple> zrangeByScoreWithScores(String key, String min, String max, int offset, int count) {
		return d_zrangeByScoreWithScores(key, min, max, offset, count).getResult();
	}

	public OperationResult<Set<Tuple>> d_zrangeByScoreWithScores(final String key, final String min, final String max, final Integer offset, final Integer count)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZRANGEBYSCOREWITHSCORES) {

			@Override
			public Set<Tuple> execute(Jedis client, ConnectionContext state)  {
				return client.zrangeByScoreWithScores(key, min, max, offset, count);
			}

		});
	}
	
	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(String key, double max, double min, int offset, int count) {
		return d_zrevrangeByScoreWithScores(key, max, min, offset, count).getResult();
	}

	public OperationResult<Set<Tuple>> d_zrevrangeByScoreWithScores(final String key, final Double max, final Double min, final Integer offset, final Integer count)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZREVRANGEBYSCOREWITHSCORES) {

			@Override
			public Set<Tuple> execute(Jedis client, ConnectionContext state)  {
				return client.zrevrangeByScoreWithScores(key, max, min, offset, count);
			}

		});
	}
	
	@Override
	public Set<Tuple> zrevrangeByScoreWithScores(String key, String max, String min, int offset, int count) {
		return d_zrevrangeByScoreWithScores(key, max, min, offset, count).getResult();
	}

	public OperationResult<Set<Tuple>> d_zrevrangeByScoreWithScores(final String key, final String max, final String min, final Integer offset, final Integer count)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Set<Tuple>>(key, OpName.ZREVRANGEBYSCOREWITHSCORES) {

			@Override
			public Set<Tuple> execute(Jedis client, ConnectionContext state)  {
				return client.zrevrangeByScoreWithScores(key, max, min, offset, count);
			}

		});
	}
	
	@Override
	public Long zremrangeByScore(String key, String start, String end) {
		return d_zremrangeByScore(key, start, end).getResult();
	}

	public OperationResult<Long> d_zremrangeByScore(final String key, final String start, final String end)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.ZREMRANGEBYSCORE) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.zremrangeByScore(key, start, end);
			}

		});
	}
	
	@Override
	public List<String> blpop(String arg) {
		return d_blpop(arg).getResult();
	}

	public OperationResult<List<String>> d_blpop(final String arg)  {

		return connPool.executeWithFailover(new BaseKeyOperation<List<String>>(arg, OpName.BLPOP) {

			@Override
			public List<String> execute(Jedis client, ConnectionContext state)  {
				return client.blpop(arg);
			}

		});
	}

	@Override
	public List<String> brpop(String arg) {
		return d_brpop(arg).getResult();
	}

	public OperationResult<List<String>> d_brpop(final String arg)  {

		return connPool.executeWithFailover(new BaseKeyOperation<List<String>>(arg, OpName.BRPOP) {

			@Override
			public List<String> execute(Jedis client, ConnectionContext state)  {
				return client.brpop(arg);
			}

		});
	}

	@Override
	public String echo(String string) {
		return d_echo(string).getResult();
	}

	public OperationResult<String> d_echo(final String key)  {

		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.ECHO) {

			@Override
			public String execute(Jedis client, ConnectionContext state)  {
				return client.echo(key);
			}

		});
	}

	@Override
	public Long move(String key, int dbIndex) {
		return d_move(key, dbIndex).getResult();
	}

	public OperationResult<Long> d_move(final String key, final Integer dbIndex)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.MOVE) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.move(key, dbIndex);
			}

		});
	}

	@Override
	public Long bitcount(String key) {
		return d_bitcount(key).getResult();
	}

	public OperationResult<Long> d_bitcount(final String key) {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.BITCOUNT) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.bitcount(key);
			}

		});
	}

	@Override
	public Long bitcount(String key, long start, long end) {
		return d_bitcount(key, start, end).getResult();
	}

	public OperationResult<Long> d_bitcount(final String key, final Long start, final Long end)  {

		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.BITCOUNT) {

			@Override
			public Long execute(Jedis client, ConnectionContext state)  {
				return client.bitcount(key, start, end);
			}

		});
	}
	

	/** MULTI-KEY COMMANDS */

	@Override
	public Long del(String... keys) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public List<String> blpop(int timeout, String... keys) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public List<String> brpop(int timeout, String... keys) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public List<String> blpop(String... args) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public List<String> brpop(String... args) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public Set<String> keys(String pattern) {
		return d_keys(pattern).getResult();
	}

	public OperationResult<Set<String>> d_keys(final String pattern) {
	
		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(pattern, OpName.BITCOUNT) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.keys(pattern);
			}
		});
	}
	
	@Override
	public List<String> mget(String... keys) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public String mset(String... keysvalues) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public Long msetnx(String... keysvalues) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public String rename(String oldkey, String newkey) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public Long renamenx(String oldkey, String newkey) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public Set<String> sinter(String... keys) {
		throw new NotImplementedException("not yet implemented");
	}

	public Long sinterstore(final String dstkey, final String ... keys)  {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public Long sort(String key, SortingParams sortingParameters, String dstkey) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public Long sort(String key, String dstkey) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public Set<String> sunion(String... keys) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public Long sunionstore(String dstkey, String... keys) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public String watch(String... keys) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public String unwatch() {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public Long zinterstore(String dstkey, String... sets) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public Long zinterstore(String dstkey, ZParams params, String... sets) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public Long zunionstore(String dstkey, String... sets) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public Long zunionstore(String dstkey, ZParams params, String... sets) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public String brpoplpush(String source, String destination, int timeout) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public Long publish(String channel, String message) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public void subscribe(JedisPubSub jedisPubSub, String... channels) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public void psubscribe(JedisPubSub jedisPubSub, String... patterns) {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public String randomKey() {
		throw new NotImplementedException("not yet implemented");
	}

	@Override
	public Long bitop(BitOP op, String destKey, String... srcKeys) {
		throw new NotImplementedException("not yet implemented");
	}


	public static class Builder {
		
		private String appName;
		private String clusterName;
		private ConnectionPoolConfigurationImpl cpConfig;
		private HostSupplier hostSupplier;
		
		public Builder() {
		}

		public Builder withApplicationName(String applicationName) {
			appName = applicationName;
			return this;
		}

		public Builder withDynomiteClusterName(String cluster) {
			clusterName = cluster;
			return this;
		}

		public Builder withCPConfig(ConnectionPoolConfigurationImpl config) {
			cpConfig = config;
			return this;
		}

		public Builder withHostSupplier(HostSupplier hSupplier) {
			hostSupplier = hSupplier;
			return this;
		}

		public DynoJedisClient build() {

			assert(appName != null);
			assert(clusterName != null);
			
			if (cpConfig == null) {
				cpConfig = new ConnectionPoolConfigurationImpl(appName);
			}
			
			if (hostSupplier == null) {
				hostSupplier = new EurekaHostsSupplier(clusterName, cpConfig.getPort());
			}
			
			cpConfig.withHostSupplier(hostSupplier);
			
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
	}
}
