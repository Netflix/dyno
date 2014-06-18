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

	/**


EXPIRE
EXPIREAT
GETBIT
GETRANGE
GETSET







PEXPIRE
PEXPIREAT
RESTORE
SCARD
SETBIT
SETEX
SETNX
SETRANGE
SISMEMBER
SMEMBERS
SMOVE
SPOP
SRANDMEMBER
S_LSET
ZCARD
ZCOUNT
ZINCRBY
ZRANK
ZREMRANGEBYRANK
ZREMRANGEBYSCORE
ZREVRANK
ZSCORE
_PSETEX


	 * @author poberai
	 *
	 */
	private enum OpName { 
		APPEND,
		SET, GET, EXISTS, PERSIST, PTTL, TTL, TYPE, DUMP, DECR, INCR, DECRBY, INCRBY, INCRBYFLOAT, STRLEN, 
		HGET, HSET, HSETNX, HGETALL, HKEYS,	HVALS, HLEN, HEXISTS, HDEL, HINCRBY, HINCRBYFLOAT,
		LINDEX, LINSERT, LLEN, LPOP, LPUSH, LPUSHX, LRANGE, LREM, LTRIM, RPOP, RPOPLPUSH, RPUSH, RPUSHX,
		
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
	
	public String get(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.GET) {
			
			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.get(key);
			}
			
		}).getResult();
	}
	
	public Void set(final String key, final String value) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Void>(key, OpName.SET) {

			@Override
			public Void execute(Jedis client, ConnectionContext state) throws DynoException {
				client.set(key, value);
				return null;
			}
			
		}).getResult();
	}
	
	public Long append(final String key, final String value) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.SET) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.append(key, value);
			}
			
		}).getResult();
	}

	public OperationResult<Boolean> exists(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Boolean>(key, OpName.EXISTS) {

			@Override
			public Boolean execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.exists(key);
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
	
	public OperationResult<Long> pttl(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.PTTL) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.pttl(key);
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
	
	public OperationResult<byte[]> dump(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<byte[]>(key, OpName.DUMP) {

			@Override
			public byte[] execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.dump(key);
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
	
	public OperationResult<Long> incr(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.INCR) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.incr(key);
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

	public OperationResult<Long> strlen(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.STRLEN) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.strlen(key);
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

	public OperationResult<Set<String>> hkeys(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Set<String>>(key, OpName.HKEYS) {

			@Override
			public Set<String> execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.hkeys(key);
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

	public OperationResult<Long> hlen(final String key) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HLEN) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.hlen(key);
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

	public OperationResult<Long> hdel(final String key, final String ... fields) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<Long>(key, OpName.HDEL) {

			@Override
			public Long execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.hdel(key, fields);
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

	public OperationResult<String> ltrim(final String key, final Long start, final Long end) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(key, OpName.LTRIM) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.ltrim(key, start, end);
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

	public OperationResult<String> rpushx(final String srckey, final String dstkey) throws DynoException {
		
		return connPool.executeWithFailover(new BaseKeyOperation<String>(srckey, OpName.RPOPLPUSH) {

			@Override
			public String execute(Jedis client, ConnectionContext state) throws DynoException {
				return client.rpoplpush(srckey, dstkey);
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
