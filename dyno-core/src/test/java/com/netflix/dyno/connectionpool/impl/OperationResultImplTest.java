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

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.OperationMonitor;

public class OperationResultImplTest {

	@Test
	public void testProcess() throws Exception {

		OperationMonitor monitor = new LastOperationMonitor();
		OperationResultImpl<Integer> opResult = new OperationResultImpl<Integer>("test", 11, monitor);
		Host host = new Host("testHost", "rand_ip", 1234, "rand_rack");

		opResult.attempts(2)
		.addMetadata("foo", "f1").addMetadata("bar", "b1")
		.setLatency(10, TimeUnit.MILLISECONDS)
		.setNode(host);

		Assert.assertEquals(2, opResult.getAttemptsCount());
		Assert.assertEquals(10, opResult.getLatency());
		Assert.assertEquals(10, opResult.getLatency(TimeUnit.MILLISECONDS));
		Assert.assertEquals(host, opResult.getNode());
		Assert.assertEquals("f1", opResult.getMetadata().get("foo"));
		Assert.assertEquals("b1", opResult.getMetadata().get("bar"));
	}
}
