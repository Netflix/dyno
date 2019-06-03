package com.netflix.dyno.jedis.operation;

import com.netflix.dyno.connectionpool.Operation;
import com.netflix.dyno.jedis.OpName;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * A poor man's solution for multikey operation. This is similar to
 * basekeyoperation just that it takes a list of keys as arguments. For
 * token aware, we just use the first key in the list. Ideally we should be
 * doing a scatter gather
 */
public abstract class MultiKeyOperation<T> implements Operation<Jedis, T> {

    private final List<String> keys;
    private final List<byte[]> binaryKeys;
    private final OpName op;

    public MultiKeyOperation(final List keys, final OpName o) {
        Object firstKey = (keys != null && keys.size() > 0) ? keys.get(0) : null;

        if (firstKey != null) {
            if (firstKey instanceof String) {//string key
                this.keys = keys;
                this.binaryKeys = null;
            } else if (firstKey instanceof byte[]) {//binary key
                this.keys = null;
                this.binaryKeys = keys;
            } else {//something went wrong here
                this.keys = null;
                this.binaryKeys = null;
            }
        } else {
            this.keys = null;
            this.binaryKeys = null;
        }

        this.op = o;
    }

    @Override
    public String getName() {
        return op.name();
    }

    /**
     * Sends back only the first key of the multi key operation.
     * @return
     */
    @Override
    public String getStringKey() {
        return (this.keys != null) ? this.keys.get(0) : null;
    }

    public byte[] getBinaryKey() {
        return (binaryKeys != null) ? binaryKeys.get(0) : null;
    }

}
