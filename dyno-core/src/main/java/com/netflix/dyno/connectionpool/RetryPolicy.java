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
 * Interface for retry policies when executing {@link Operation}
 * @author poberai
 *
 */
public interface RetryPolicy {
    /**
     * Operation is starting
     */
    void begin();

    /**
     * Operation has completed successfully
     */
    void success();

    /**
     * Operation has failed
     */
    void failure(Exception e);

    /**
     * Ask the policy if a retry is allowed. This may internally sleep
     * 
     * @return boolean
     */
    boolean allowRetry();

    /**
     * Ask the policy is a retry can use a remote dc 
     * @return boolean
     */
    boolean allowCrossZoneFallback();
    
    /**
     * Return the number of attempts since begin was called
     * 
     * @return int
     */
    int getAttemptCount();
    
    public static interface RetryPolicyFactory {
    	public RetryPolicy getRetryPolicy();
    }
}