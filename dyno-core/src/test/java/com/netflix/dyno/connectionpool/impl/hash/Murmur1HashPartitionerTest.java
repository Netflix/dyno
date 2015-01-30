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


