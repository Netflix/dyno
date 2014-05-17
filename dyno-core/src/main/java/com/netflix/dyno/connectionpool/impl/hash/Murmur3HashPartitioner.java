package com.netflix.dyno.connectionpool.impl.hash;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.List;

import com.google.common.primitives.UnsignedInts;
import com.netflix.dyno.connectionpool.HashPartitioner;
import com.netflix.dyno.connectionpool.HostToken;

public class Murmur3HashPartitioner implements HashPartitioner {

	private static final String UTF_8 = "UTF-8";
	private static final Charset charset = Charset.forName(UTF_8);

	public Murmur3HashPartitioner() {
	}

	@Override
	public Long hash(long key) {

		ByteBuffer bb = ByteBuffer.allocate(8).putLong(0, key);
		byte[] b = bb.array();
		return UnsignedInts.toLong(Murmur3Hash.hash32(b, b.length));
	}

	@Override
	public Long hash(int key) {

		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putInt(key);
		bb.rewind();

		byte[] b = bb.array();
		return UnsignedInts.toLong(Murmur3Hash.hash32(b, b.length));
	}

	@Override
	public Long hash(String key) {
		if (key == null) {
			return 0L;
		}
		ByteBuffer bb = ByteBuffer.wrap(key.getBytes(charset));
		byte[] b = bb.array();
		return UnsignedInts.toLong(Murmur3Hash.hash32(b, b.length));
	}

	@Override
	public HostToken getToken(List<HostToken> hostTokens, Long keyHash) {
		throw new RuntimeException("NotImplemented");
	}

}
