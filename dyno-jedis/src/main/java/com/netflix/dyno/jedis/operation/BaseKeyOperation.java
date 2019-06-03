package com.netflix.dyno.jedis.operation;

import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.jedis.OpName;
import redis.clients.jedis.Jedis;

public abstract class BaseKeyOperation<T> implements Operation<Jedis, T> {

    private final String key;
    private final byte[] binaryKey;
    private final OpName op;

    public BaseKeyOperation(final String k, final OpName o) {
        this.key = k;
        this.binaryKey = null;
        this.op = o;
    }

    public BaseKeyOperation(final byte[] k, final OpName o) {
        this.key = null;
        this.binaryKey = k;
        this.op = o;
    }

    @Override
    public String getName() {
        return op.name();
    }

    @Override
    public String getStringKey() {
        return this.key;
    }

    public byte[] getBinaryKey() {
        return this.binaryKey;
    }

}
