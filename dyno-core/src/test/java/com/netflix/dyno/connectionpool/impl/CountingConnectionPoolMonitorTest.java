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
package com.netflix.dyno.connectionpool.impl;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.exception.NoAvailableHostsException;
import com.netflix.dyno.connectionpool.exception.PoolExhaustedException;
import com.netflix.dyno.connectionpool.exception.PoolTimeoutException;

public class CountingConnectionPoolMonitorTest {

	@Test
	public void testProcess() throws Exception {

		CountingConnectionPoolMonitor counter = new CountingConnectionPoolMonitor();

		Host host1 = new Host("host1","address1", 1111, "rack1");
		Host host2 = new Host("host2","address2", 2222, "rack1");

		// Host 1
		counter.incConnectionCreated(host1);
		counter.incConnectionClosed(host1, null);
		counter.incConnectionCreateFailed(host1, null);
		counter.incConnectionBorrowed(host1, 0);
		counter.incConnectionReturned(host1);

		counter.incOperationSuccess(host1, 0);
		counter.incOperationFailure(host1, null);

		// Host 2
		counter.incConnectionBorrowed(host2, 0);
		counter.incConnectionReturned(host2);

		counter.incOperationSuccess(host2, 0);
		counter.incOperationFailure(host2, null);

		counter.incOperationFailure(host2, new PoolTimeoutException(""));
		counter.incOperationFailure(host2, new PoolExhaustedException(null, ""));
		counter.incOperationFailure(host2, new NoAvailableHostsException(""));

		// VERIFY COUNTS
		Assert.assertEquals(1, counter.getConnectionCreatedCount());
		Assert.assertEquals(1, counter.getConnectionClosedCount());
		Assert.assertEquals(1, counter.getConnectionCreateFailedCount());
		Assert.assertEquals(2, counter.getConnectionBorrowedCount());
		Assert.assertEquals(2, counter.getConnectionReturnedCount());

		Assert.assertEquals(2, counter.getOperationSuccessCount());
		Assert.assertEquals(5, counter.getOperationFailureCount());

		Assert.assertEquals(1, counter.getHostStats().get(host1).getConnectionsBorrowed());
		Assert.assertEquals(1, counter.getHostStats().get(host1).getConnectionsReturned());
		Assert.assertEquals(1, counter.getHostStats().get(host1).getConnectionsCreated());
		Assert.assertEquals(1, counter.getHostStats().get(host1).getConnectionsCreateFailed());
		Assert.assertEquals(1, counter.getHostStats().get(host1).getOperationSuccessCount());
		Assert.assertEquals(1, counter.getHostStats().get(host1).getOperationErrorCount());

		Assert.assertEquals(1, counter.getHostStats().get(host2).getConnectionsBorrowed());
		Assert.assertEquals(1, counter.getHostStats().get(host2).getConnectionsReturned());
		Assert.assertEquals(0, counter.getHostStats().get(host2).getConnectionsCreated());
		Assert.assertEquals(0, counter.getHostStats().get(host2).getConnectionsCreateFailed());
		Assert.assertEquals(1, counter.getHostStats().get(host2).getOperationSuccessCount());
		Assert.assertEquals(4, counter.getHostStats().get(host2).getOperationErrorCount());
	}
}