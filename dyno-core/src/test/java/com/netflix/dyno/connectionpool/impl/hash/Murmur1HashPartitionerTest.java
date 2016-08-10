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

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.impl.utils.IOUtilities;

public class Murmur1HashPartitionerTest {

	@Test
	public void test1MKeys() throws Exception {

		Murmur1HashPartitioner hash = new Murmur1HashPartitioner();

		List<String> testTokens = readInTestTokens();

		for (int i=0; i<1000000; i++) {
			long lHash = hash.hash("" + i);
			long expected = Long.parseLong(testTokens.get(i));

			Assert.assertEquals("Failed for i: " + i, expected, lHash);
		}
	}

	private List<String> readInTestTokens() throws IOException {
		return IOUtilities.readLines(new File("./src/main/java/TestTokens.txt"));
	}
}


