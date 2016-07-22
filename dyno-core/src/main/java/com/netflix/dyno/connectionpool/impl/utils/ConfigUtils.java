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
