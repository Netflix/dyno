package com.netflix.dyno.connectionpool;

import java.util.List;

/**
 * The result of performing a scatter-gather operations
 */
public interface StringBasedResult<T> {

    List<T> getResult();

    String getHost(String host);

    boolean isComplete();

}