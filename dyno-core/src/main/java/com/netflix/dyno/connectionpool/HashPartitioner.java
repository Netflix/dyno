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
package com.netflix.dyno.connectionpool;

import java.util.List;

import com.netflix.dyno.connectionpool.impl.lb.HostToken;

/**
 * Interface for generating hash for a given key. 
 * The implementing class is also responsible for determining how to map the hash to the underlying dynomite server topology
 * expressed via the Collection<HostToken>
 *  
 * @author poberai
 *
 */
public interface HashPartitioner {

	/**
	 * @param key
	 * @return Long
	 */
	public Long hash(int key);
	
	/**
	 * @param key
	 * @return Long
	 */
	public Long hash(long key);
	
	/**
	 * @param key
	 * @return Long
	 */
	public Long hash(String key);
	
	/**
	 * @param hostTokens
	 * @param keyHash
	 * @return
	 */
	public HostToken getToken(List<HostToken> hostTokens, Long keyHash);
}
