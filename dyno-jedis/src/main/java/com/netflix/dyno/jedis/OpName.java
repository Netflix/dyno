package com.netflix.dyno.jedis;

public enum OpName {
	
	 APPEND, 
	 BITCOUNT, BLPOP, BRPOP, 
	 DECR, DECRBY, DEL, DUMP, 
	 ECHO, EXISTS, EXPIRE, EXPIREAT, 
	 GET, GETBIT, GETRANGE, GETSET, 
	 HDEL, HEXISTS,  HGET, HGETALL, HINCRBY, HINCRBYFLOAT, HKEYS, HLEN, HMGET, HMSET, HSET, HSETNX, HVALS, 
	 INCR, INCRBY, INCRBYFLOAT, 
	 KEYS, LINDEX, 
	 LINSERT, LLEN, LPOP, LPUSH, LPUSHX, LRANGE, LREM, LSET, LTRIM, 
	 MOVE, 
	 PERSIST, PEXPIRE, PEXPIREAT, PSETEX, PTTL, 
	 RENAME, RENAMENX, RESTORE, RPOP, RPOPLPUSH, RPUSH, RPUSHX, 
	 SADD, SCARD, SDIFF, SDIFFSTORE, SET, SETBIT, SETEX, SETNX, SETRANGE, SINTER, SINTERSTORE, SISMEMBER, SMEMBERS, 
	 SMOVE, SORT, SPOP, SRANDMEMBER, SREM, STRLEN, SUBSTR, SUNION, SUNIONSTORE, 
	 TTL, TYPE, 
	 ZADD, ZCARD, ZCOUNT, ZINCRBY, ZRANGE, ZRANGEWITHSCORES, ZRANK, ZRANGEBYSCORE, ZRANGEBYSCOREWITHSCORES, ZREM, ZREMRANGEBYRANK, 
	 ZREMRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYSCORE, ZREVRANGEBYSCOREWITHSCORES, ZREVRANGEWITHSCORES, ZREVRANK, ZSCORE
	 ;
}
