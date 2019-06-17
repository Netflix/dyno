/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.connectionpool.impl.hash;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import com.netflix.dyno.connectionpool.HashPartitioner;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;

/**
 * Impl of {@link HashPartitioner} that uses {@link Murmur2Hash}
 * @author poberai
 *
 */
public class Murmur2HashPartitioner implements HashPartitioner {

    private static final String UTF_8 = "UTF-8";
    private static final Charset charset = Charset.forName(UTF_8);

    public Murmur2HashPartitioner() {
    }

    @Override
    public Long hash(long key) {

        ByteBuffer bb = ByteBuffer.allocate(8).putLong(0, key);
        byte[] b = bb.array();
        return UnsignedIntsUtils.toLong(Murmur2Hash.hash32(b, b.length));
    }

    @Override
    public Long hash(int key) {

        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(key);
        bb.rewind();

        byte[] b = bb.array();
        return UnsignedIntsUtils.toLong(Murmur2Hash.hash32(b, b.length));
    }

    @Override
    public Long hash(String key) {
        if (key == null) {
            return 0L;
        }
        ByteBuffer bb = ByteBuffer.wrap(key.getBytes(charset));
        byte[] b = bb.array();
        return UnsignedIntsUtils.toLong(Murmur2Hash.hash32(b, b.length));
    }

    @Override
    public Long hash(byte[] key) {
        if (key == null) {
            return 0L;
        }
        return UnsignedIntsUtils.toLong(Murmur2Hash.hash32(key, key.length));
    }

    @Override
    public HostToken getToken(Long keyHash) {
        throw new RuntimeException("NotImplemented");
    }
}
