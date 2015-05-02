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

/**
 * Base for any operation that can be performed using a connection from the connection pool
 * @author poberai
 *
 * @param <CL> client
 * @param <R>  result
 */
public interface BaseOperation<CL, R> {

	/**
	 * Op name. Used for tracking metrics etc.
	 * @return String
	 */
	public String getName();
	
	/**
	 * The key for the operation. Useful for implementing token aware routing.
	 * @return String
	 */
	public String getKey();
}
