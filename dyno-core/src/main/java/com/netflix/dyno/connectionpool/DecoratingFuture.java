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
package com.netflix.dyno.connectionpool;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DecoratingFuture<V> implements Future<V> {

	private final ListenableFuture<OperationResult<V>> innerFuture; 
	
	public DecoratingFuture(final ListenableFuture<OperationResult<V>> listenableFuture) {
		innerFuture = listenableFuture;
	}
	
	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return innerFuture.cancel(mayInterruptIfRunning);
	}

	@Override
	public boolean isCancelled() {
		return innerFuture.isCancelled();
	}

	@Override
	public boolean isDone() {
		return innerFuture.isDone();
	}

	@Override
	public V get() throws InterruptedException, ExecutionException {
		OperationResult<V> opResult = innerFuture.get();
		return opResult.getResult();
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		OperationResult<V> opResult = innerFuture.get(timeout, unit);
		return opResult.getResult();
	}
}
