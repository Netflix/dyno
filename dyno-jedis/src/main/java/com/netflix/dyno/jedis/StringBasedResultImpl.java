package com.netflix.dyno.jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.netflix.dyno.connectionpool.StringBasedResult;

import redis.clients.jedis.ScanResult;

/**
 * Encapsulates the results of performing a distributed operation.
 * <p>
 * Example usage
 * 
 * <pre>
 *    StringBasedResult<String> cbi = null;
 *    do {
 *        cbi = client.dyno_xxx();
 *        .
 *        .
 *        .
 *    } while (!cbi.isComplete());
 * </pre>
 */
public class StringBasedResultImpl<T> implements StringBasedResult<T> {

    private final Map<String, T> result;

    public StringBasedResultImpl(Map<String, T> result) {
        this.result = result;
    }

    @Override
    public List<T> getResult() {
        final List<T> aggregated = new ArrayList<>();
        for (T sr : result.values()) {
            aggregated.add(sr);
        }
        return aggregated;
    }

    @Override
    public String getHost(String host) {
        String sr = (String) result.get(host);
        if (sr != null) {
            return sr;
        }

        return null;
    }

    @Override
    public boolean isComplete() {
        for (T r : result.values()) {
            if (!r.equals("0")) {
                return false;
            }
        }

        return true;
    }

}
