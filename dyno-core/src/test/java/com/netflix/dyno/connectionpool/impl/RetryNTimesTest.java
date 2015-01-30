package com.netflix.dyno.connectionpool.impl;

import org.junit.Assert;
import org.junit.Test;

public class RetryNTimesTest {

	@Test
	public void testNRetries() throws Exception {

		RetryNTimes retry = new RetryNTimes(3, true);

		RuntimeException e = new RuntimeException("failure");
		retry.begin();

		Assert.assertTrue(retry.allowRetry());

		retry.failure(e);
		Assert.assertTrue(retry.allowRetry());
		retry.failure(e);
		Assert.assertTrue(retry.allowRetry());
		retry.failure(e);
		Assert.assertTrue(retry.allowRetry());

		retry.failure(e);
		Assert.assertFalse(retry.allowRetry());

		Assert.assertEquals(4, retry.getAttemptCount());
	}
}
