package com.netflix.dyno.connectionpool.impl;

import java.util.List;
import java.util.Map;

public interface MonitorConsoleMBean {

    String getMonitorStats(String cpName);

    Map<String, Map<String, List<String>>> getTopologySnapshot(String cpName);
}
