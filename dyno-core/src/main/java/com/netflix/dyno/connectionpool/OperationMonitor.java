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

import java.util.concurrent.TimeUnit;

/**
 * Interface for recording high level stats for operation executions. 
 * Includes latency, success and failures. 
 * 
 * @author poberai
 *
 */
public interface OperationMonitor {

	/**
	 * Record latency for the operation
	 * @param opName
	 * @param duration
	 * @param unit
	 */
	public void recordLatency(String opName, long duration, TimeUnit unit);
	
	/**
	 * Record success for the operation
	 * @param opName
	 */
	public void recordSuccess(String opName);

	/**
	 * Record failure for the operation
	 * @param opName
	 * @param reason
	 */
	public void recordFailure(String opName, String reason);
}
