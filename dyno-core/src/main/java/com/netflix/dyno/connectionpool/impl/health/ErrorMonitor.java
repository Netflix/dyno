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
package com.netflix.dyno.connectionpool.impl.health;


public interface ErrorMonitor {

    /**
     * Monitor errors
     * @param numErrors
     * @return true/false indicating whether the error are within the threshold.
     *         True: Errors still ok. False: errors have crossed the threshold
     */
    boolean trackError(int numErrors);


    interface ErrorMonitorFactory {

        @Deprecated
        ErrorMonitor createErrorMonitor();

        ErrorMonitor createErrorMonitor(int maxValue);
    }
}
