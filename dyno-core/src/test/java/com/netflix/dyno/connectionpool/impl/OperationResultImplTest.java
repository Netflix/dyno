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
		Host host = new Host("testHost", 1234);

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
