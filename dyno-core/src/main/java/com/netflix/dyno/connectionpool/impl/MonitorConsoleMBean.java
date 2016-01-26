package com.netflix.dyno.connectionpool.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface MonitorConsoleMBean {

    /**
     * Note that monitor names == connection pool names
     *
     * @return A comma separated string of all registered monitors
     */
    String getMonitorNames();

    String getMonitorStats(String cpName);

    Map<String, Map<String, List<String>>> getTopologySnapshot(String cpName);

    Map<String, String> getRuntimeConfiguration(String cpName);
}
