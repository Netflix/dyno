package com.netflix.dyno.connectionpool.impl;

import org.junit.Assert;
import org.junit.Test;

public class ConnectionContextImplTest {

	@Test
	public void testMetadata() throws Exception {

		ConnectionContextImpl context = new ConnectionContextImpl();

		Assert.assertFalse(context.hasMetadata("m1"));
		context.setMetadata("m1", "foobar");
		Assert.assertTrue(context.hasMetadata("m1"));
		Assert.assertEquals("foobar", context.getMetadata("m1"));

		context.reset();
		Assert.assertFalse(context.hasMetadata("m1"));
	}
}
