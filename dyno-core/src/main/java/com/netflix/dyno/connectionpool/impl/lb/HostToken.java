/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.dyno.connectionpool.impl.lb;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.Host;

/**
 * Simple class that ecapsulates a host and it's token on the dynomite toppology ring. 
 * The class must implements Comparable<Long> so that it can be stored in a sorted collection that can then be 
 * used in search algos like binary search for efficiently finding the owning token for a hash operation key. 
 * 
 * @author poberai
 *
 */
public class HostToken implements Comparable<Long> {

	private final Long token;
	private final Host host;

	public HostToken(Long token, Host host) {
		this.token = token;
		this.host = host;
	}

	public Long getToken() {
		return token;
	}

	public Host getHost() {
		return host;
	}

	@Override
	public String toString() {
		return "HostToken [token=" + token + ", host=" + host + "]";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + ((token == null) ? 0 : token.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		
		HostToken other = (HostToken) obj;
		boolean equals = true;
		equals &= (token != null) ? (token.equals(other.token)) : (other.token == null);
		equals &= (host != null) ? (host.equals(other.host)) : (other.host == null);
		return equals;
	}

	@Override
	public int compareTo(Long o) {
		return this.token.compareTo(o);
	}
	
	public int compareTo(HostToken o) {
		return this.token.compareTo(o.getToken());
	}

	public static class UnitTest {
		
		@Test
		public void testEquals() throws Exception {
			
			HostToken t1 = new HostToken(1L, new Host("foo", 1234));
			HostToken t2 = new HostToken(1L, new Host("foo", 1234));
			
			Assert.assertEquals(t1, t2);
			
			// change token
			HostToken t3 = new HostToken(2L, new Host("foo", 1234));
			Assert.assertFalse(t1.equals(t3));
			
			// change host name
			HostToken t4 = new HostToken(1L, new Host("foo1", 1234));
			Assert.assertFalse(t1.equals(t4));
			
			// change host port
			HostToken t5 = new HostToken(1L, new Host("foo", 5678));
			Assert.assertFalse(t1.equals(t5));
		}

		@Test
		public void testSort() throws Exception {
			
			HostToken t1 = new HostToken(1L, new Host("foo1", 1234));
			HostToken t2 = new HostToken(2L, new Host("foo2", 1234));
			HostToken t3 = new HostToken(3L, new Host("foo3", 1234));
			HostToken t4 = new HostToken(4L, new Host("foo4", 1234));
			HostToken t5 = new HostToken(5L, new Host("foo5", 1234));
			
			HostToken[] arr = {t5, t2, t4, t3, t1};
			List<HostToken> list = Arrays.asList(arr);
			
			Assert.assertEquals(t5, list.get(0));
			Assert.assertEquals(t2, list.get(1));
			Assert.assertEquals(t4, list.get(2));
			Assert.assertEquals(t3, list.get(3));
			Assert.assertEquals(t1, list.get(4));
			
			Collections.sort(list, new Comparator<HostToken>() {
				@Override
				public int compare(HostToken o1, HostToken o2) {
					return o1.compareTo(o2);
				}
			});

			Assert.assertEquals(t1, list.get(0));
			Assert.assertEquals(t2, list.get(1));
			Assert.assertEquals(t3, list.get(2));
			Assert.assertEquals(t4, list.get(3));
			Assert.assertEquals(t5, list.get(4));
		}
	}
}
