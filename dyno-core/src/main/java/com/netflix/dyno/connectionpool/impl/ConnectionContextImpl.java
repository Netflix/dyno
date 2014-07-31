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
package com.netflix.dyno.connectionpool.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Assert;
import org.junit.Test;

import com.netflix.dyno.connectionpool.ConnectionContext;

public class ConnectionContextImpl implements ConnectionContext {

	private final ConcurrentHashMap<String, Object> context = new ConcurrentHashMap<String, Object>();
	
	@Override
	public void setMetadata(String key, Object obj) {
		context.put(key, obj);
	}

	@Override
	public Object getMetadata(String key) {
		return context.get(key);
	}

	@Override
	public boolean hasMetadata(String key) {
		return context.containsKey(key);
	}

	@Override
	public void reset() {
		context.clear();
	}

	@Override
	public Map<String, Object> getAll() {
		return context;
	}

	public static class UnitTest {
		
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
}
