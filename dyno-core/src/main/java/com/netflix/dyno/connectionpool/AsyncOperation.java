package com.netflix.dyno.connectionpool;

import java.util.concurrent.Future;

import com.netflix.dyno.connectionpool.exception.DynoException;

public interface AsyncOperation<CL, R> extends BaseOperation<CL, R> {

    Future<R> executeAsync(CL client) throws DynoException;

}
