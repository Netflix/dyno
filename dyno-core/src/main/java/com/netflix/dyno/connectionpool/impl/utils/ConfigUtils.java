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
package com.netflix.dyno.connectionpool.impl.utils;

/**
 * Simple helper class that provides convenience methods for configuration related tasks.
 *
 * @author jcacciatore
 */
public class ConfigUtils {

    public static String getLocalZone() {
        String az = System.getenv("EC2_AVAILABILITY_ZONE");

        if (az == null) {
            az = System.getProperty("EC2_AVAILABILITY_ZONE");
        }

        return az;
    }

    public static String getDataCenter() {
        String dc = System.getenv("EC2_REGION");

        if (dc == null) {
            dc = System.getProperty("EC2_REGION");
        }

        if (dc == null) {
            String rack = getLocalZone();
            if (rack != null) {
                dc = rack.substring(0, rack.length() - 1);
            }
        }

        return dc;
    }

}
