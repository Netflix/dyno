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

public enum OpName {
	
	 APPEND, 
	 BITCOUNT, BLPOP, BRPOP, 
	 DECR, DECRBY, DEL, DUMP, 
	 ECHO, EXISTS, EXPIRE, EXPIREAT,EVAL,
	 GET, GETBIT, GETRANGE, GETSET, 
	 HDEL, HEXISTS,  HGET, HGETALL, HINCRBY, HINCRBYFLOAT, HKEYS, HLEN, HMGET, HMSET, HSET, HSCAN, HSETNX, HVALS, 
	 INCR, INCRBY, INCRBYFLOAT, 
	 KEYS, LINDEX, 
	 LINSERT, LLEN, LPOP, LPUSH, LPUSHX, LRANGE, LREM, LSET, LTRIM, 
	 MOVE, 
	 PERSIST, PEXPIRE, PEXPIREAT, PSETEX, PTTL, 
	 RENAME, RENAMENX, RESTORE, RPOP, RPOPLPUSH, RPUSH, RPUSHX, 
	 SADD, SCAN, SCARD, SDIFF, SDIFFSTORE, SET, SETBIT, SETEX, SETNX, SETRANGE, SINTER, SINTERSTORE, SISMEMBER, SMEMBERS,
	 SMOVE, SORT, SPOP, SRANDMEMBER, SREM, SSCAN, STRLEN, SUBSTR, SUNION, SUNIONSTORE, 
	 TTL, TYPE, 
	 ZADD, ZCARD, ZCOUNT, ZINCRBY, ZRANGE, ZRANGEWITHSCORES, ZRANK, ZRANGEBYSCORE, ZRANGEBYSCOREWITHSCORES, ZREM, ZREMRANGEBYRANK, 
	 ZREMRANGEBYSCORE, ZREVRANGE, ZREVRANGEBYSCORE, ZREVRANGEBYSCOREWITHSCORES, ZREVRANGEWITHSCORES, ZREVRANK, ZSCAN, ZSCORE
	 ;
}

