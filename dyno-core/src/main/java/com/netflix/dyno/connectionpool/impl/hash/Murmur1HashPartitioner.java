package com.netflix.dyno.connectionpool.impl.hash;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.HashPartitioner;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.connectionpool.impl.utils.IOUtilities;

/**
 * Impl of {@link HashPartitioner} that uses {@link Murmur1Hash}
 * @author poberai
 *
 */
public class Murmur1HashPartitioner implements HashPartitioner {

    private static final String UTF_8 = "UTF-8";
    private static final Charset charset = Charset.forName(UTF_8);

    @Override
	public Long hash(String key) {
        if (key == null) {
            return 0L;
        }
        ByteBuffer bb = ByteBuffer.wrap(key.getBytes(charset));
        byte[] b = bb.array();
        return UnsignedIntsUtils.toLong(Murmur1Hash.hash(b, b.length));
	}
	
    @Override
	public Long hash(long key) {
		
		ByteBuffer bb = ByteBuffer.allocate(8).putLong(0, key);
		bb.rewind();
        byte[] b = bb.array();
        return UnsignedIntsUtils.toLong(Murmur1Hash.hash(b, b.length));
	}

    @Override
	public Long hash(int key) {
		ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(key);
        bb.rewind();
        byte[] b = bb.array();
        return UnsignedIntsUtils.toLong(Murmur1Hash.hash(b, b.length));
	}

	@Override
	public HostToken getToken(Long keyHash) {
		throw new RuntimeException("NotImplemented");
	}
	
	public static class UnitTest {
		
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

	
}