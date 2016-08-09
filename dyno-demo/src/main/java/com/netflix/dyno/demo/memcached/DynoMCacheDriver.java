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
package com.netflix.dyno.demo.memcached;


//public class DynoMCacheDriver extends DynoDriver {
//
//	private static final DynoDriver Instance = new DynoMCacheDriver();
//	
//	private AtomicReference<DynoMCacheClient> client = new AtomicReference<DynoMCacheClient>(null);
//
//	public static DynoDriver getInstance() {
//		return Instance;
//	}
//	
//	private DynoMCacheDriver() {
//		super();
//	}
//	
//	public DynoClient dynoClientWrapper = new DynoClient () {
//
//		@Override
//		public void init() {
//			 client.set(DynoMCacheClient.Builder.withName("Demo")
//						.withDynomiteClusterName("dynomite_memcached_puneet")
//						.withConnectionPoolConfig(new ConnectionPoolConfigurationImpl("dynomite_memcached_puneet")
//												  .setPort(8102))
//						.build());
//		}
//
//		@Override
//		public String get(String key) throws Exception {
//			return client.get().<String>get(key).getResult();
//		}
//
//		@Override
//		public void set(String key, String value) {
//			client.get().<String>set(key, value);
//		}
//	};
//	
//	public DynoClient getDynoClient() {
//		return dynoClientWrapper;
//	}
//}
