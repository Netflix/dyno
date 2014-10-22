package com.netflix.dyno.connectionpool;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;

public interface ListenableFuture<V> extends Future<V> {

	public void addListener(Runnable listener, Executor executor);
}
