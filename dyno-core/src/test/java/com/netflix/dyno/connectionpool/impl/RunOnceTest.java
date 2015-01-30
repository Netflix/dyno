package com.netflix.dyno.connectionpool.impl;

import org.junit.Assert;
import org.junit.Test;

public class RunOnceTest {

	@Test
	public void testRetry() throws Exception {

		RunOnce retry = new RunOnce();

		RuntimeException e = new RuntimeException("failure");
		retry.begin();

		Assert.assertTrue(retry.allowRetry());

		retry.failure(e);
		Assert.assertFalse(retry.allowRetry());
		retry.failure(e);
		Assert.assertFalse(retry.allowRetry());

		Assert.assertEquals(1, retry.getAttemptCount());
	}
}
