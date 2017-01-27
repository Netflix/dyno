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
package com.netflix.dyno.connectionpool.impl.hash;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;

public class BinarySearchTokenMapperTest {

	@Test
	public void testSearchToken() throws Exception {

		final BinarySearchTokenMapper tokenMapper = new BinarySearchTokenMapper(new Murmur1HashPartitioner());
		tokenMapper.initSearchMecahnism(getTestTokens());

		Long failures = 0L;

		failures += runTest(309687905L - 1000000L, 309687905L, "h1", tokenMapper);
		Assert.assertTrue("Failures: " + failures, failures == 0);

		failures += runTest(309687905L + 1L, 309687905L + 1000000L, "h2", tokenMapper);
		Assert.assertTrue("Failures: " + failures, failures == 0);

		failures += runTest(1383429731L + 1L, 1383429731L + 1000000L, "h3", tokenMapper);
		Assert.assertTrue("Failures: " + failures, failures == 0);

		failures += runTest(2457171554L + 1L, 2457171554L + 1000000L, "h4", tokenMapper);
		Assert.assertTrue("Failures: " + failures, failures == 0);

		failures += runTest(3530913377L + 1L, 3530913377L + 1000000L, "h1", tokenMapper);
		Assert.assertTrue("Failures: " + failures, failures == 0);
	}

	@Test
	public void testAddToken() throws Exception {

		final BinarySearchTokenMapper tokenMapper = new BinarySearchTokenMapper(new Murmur1HashPartitioner());
		tokenMapper.initSearchMecahnism(getTestTokens());

		Long failures = 0L;

		failures += runTest(309687905L + 1L, 309687905L + 1000000L, "h2", tokenMapper);
		Assert.assertTrue("Failures: " + failures, failures == 0);

		failures += runTest(1383429731L + 1L, 1383429731L + 1000000L, "h3", tokenMapper);
		Assert.assertTrue("Failures: " + failures, failures == 0);

		// Now construct the midpoint token between 'h2' and 'h3' 
		Long midpoint = 309687905L + (1383429731L - 309687905L)/2;
		tokenMapper.addHostToken(new HostToken(midpoint, new Host("h23", -1, "r1", Status.Up)));

		failures += runTest(309687905L + 1L, 309687905L + 10L, "h23", tokenMapper);
		Assert.assertTrue("Failures: " + failures, failures == 0);

		failures += runTest(1383429731L + 1L, 1383429731L + 1000000L, "h3", tokenMapper);
		Assert.assertTrue("Failures: " + failures, failures == 0);
	}

	@Test
	public void testRemoveToken() throws Exception {

		final BinarySearchTokenMapper tokenMapper = new BinarySearchTokenMapper(new Murmur1HashPartitioner());
		tokenMapper.initSearchMecahnism(getTestTokens());

		Long failures = 0L;

		failures += runTest(309687905L + 1L, 309687905L + 1000000L, "h2", tokenMapper);
		Assert.assertTrue("Failures: " + failures, failures == 0);

		failures += runTest(1383429731L + 1L, 1383429731L + 1000000L, "h3", tokenMapper);
		Assert.assertTrue("Failures: " + failures, failures == 0);

		// Now remove token 'h3'
		tokenMapper.remoteHostToken(new HostToken(1383429731L, new Host("h2", -1, "r1", Status.Up)));

		failures += runTest(309687905L + 1L, 309687905L + 10L, "h3", tokenMapper);
		Assert.assertTrue("Failures: " + failures, failures == 0);
	}

	private long runTest(Long start, Long end, final String expectedToken, final BinarySearchTokenMapper tokenMapper) {

		final AtomicLong failures = new AtomicLong(0L);
		final AtomicLong counter = new AtomicLong(start);

		while (counter.incrementAndGet() <= end) {

			final long hash = counter.get();

			HostToken hToken = tokenMapper.getToken(hash);
			if (!(hToken.getHost().getHostAddress().equals(expectedToken))) {
				failures.incrementAndGet();
			}
		}
		return failures.get();
	}

	private Collection<HostToken> getTestTokens() {

		/**
			cqlsh:dyno_bootstrap> select "availabilityZone","hostname","token" from tokens where "appId" = 'dynomite_redis_puneet';

availabilityZone | hostname                                   | token
------------------+--------------------------------------------+------------
       us-east-1c |  ec2-54-83-179-213.compute-1.amazonaws.com | 1383429731
       us-east-1c |  ec2-54-224-184-99.compute-1.amazonaws.com |  309687905
       us-east-1c |  ec2-54-91-190-159.compute-1.amazonaws.com | 3530913377
       us-east-1c |   ec2-54-81-31-218.compute-1.amazonaws.com | 2457171554
       us-east-1e | ec2-54-198-222-153.compute-1.amazonaws.com |  309687905
       us-east-1e | ec2-54-198-239-231.compute-1.amazonaws.com | 2457171554
       us-east-1e |  ec2-54-226-212-40.compute-1.amazonaws.com | 1383429731
       us-east-1e | ec2-54-197-178-229.compute-1.amazonaws.com | 3530913377

cqlsh:dyno_bootstrap> 
		 */

		List<HostToken> tokens = new ArrayList<HostToken>();

		tokens.add(new HostToken(309687905L, new Host("h1", -1, "r1", Status.Up)));
		tokens.add(new HostToken(1383429731L, new Host("h2", -1, "r1", Status.Up)));
		tokens.add(new HostToken(2457171554L, new Host("h3", -1, "r1", Status.Up)));
		tokens.add(new HostToken(3530913377L, new Host("h4", -1, "r1", Status.Up)));

		return tokens;
	}
}
