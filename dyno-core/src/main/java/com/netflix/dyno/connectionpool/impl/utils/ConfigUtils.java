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
 * Simple helper class that provides convenience methods for configuration
 * related tasks.
 *
 * @author jcacciatore
 * @author ipapapanagiotou
 */
public class ConfigUtils {

    public static String getLocalZone() {
	String localRack = System.getenv("LOCAL_RACK") == null ? System.getProperty("LOCAL_RACK") : System.getenv("LOCAL_RACK");

	//backward compatible
	if (localRack == null) {
		localRack = System.getenv("EC2_AVAILABILITY_ZONE") == null ? System.getProperty("EC2_AVAILABILITY_ZONE") : System.getenv("EC2_AVAILABILITY_ZONE");
	}

	return localRack;
    }

    /**
     * 
     * @return the datacenter that the client is in
     */
    public static String getDataCenter() {
	String localDatacenter = System.getenv("LOCAL_DATACENTER") == null ? System.getProperty("LOCAL_DATACENTER") : System.getenv("LOCAL_DATACENTER");

	//backward compatible
	if (localDatacenter == null) {
		localDatacenter = System.getenv("EC2_REGION") == null ? System.getProperty("EC2_REGION") : System.getenv("EC2_REGION");
	}

	if (localDatacenter == null) {
	    return getDataCenterFromRack(getLocalZone());
	}
	return localDatacenter;

    }

    /**
     * 
     * Datacenter format us-east-x, us-west-x etc.
     * @param rack
     * @return the datacenter based on the provided rack
     */
    public static String getDataCenterFromRack(String rack) {
	if (rack != null) {
	    return rack.substring(0, rack.length() - 1);
	}
	return null;
    }

}
