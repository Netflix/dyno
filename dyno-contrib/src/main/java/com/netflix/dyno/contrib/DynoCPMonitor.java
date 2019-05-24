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
package com.netflix.dyno.contrib;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.dyno.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;
import com.netflix.servo.monitor.Monitors;

public class DynoCPMonitor extends CountingConnectionPoolMonitor {

    private static final Logger Logger = LoggerFactory.getLogger(DynoCPMonitor.class);

    public DynoCPMonitor(String namePrefix) {

        try {
            DefaultMonitorRegistry.getInstance().register(Monitors.newObjectMonitor(namePrefix, this));
        } catch (Exception e) {
            Logger.warn("Failed to register metrics with monitor registry", e);
        }
    }

    @Monitor(name = "OperationSuccess", type = DataSourceType.COUNTER)
    @Override
    public long getOperationSuccessCount() {
        return super.getOperationSuccessCount();
    }

    @Monitor(name = "OperationFailure", type = DataSourceType.COUNTER)
    @Override
    public long getOperationFailureCount() {
        return super.getOperationFailureCount();
    }

    @Monitor(name = "ConnectionCreated", type = DataSourceType.COUNTER)
    @Override
    public long getConnectionCreatedCount() {
        return super.getConnectionCreatedCount();
    }

    @Monitor(name = "ConnectionClosed", type = DataSourceType.COUNTER)
    @Override
    public long getConnectionClosedCount() {
        return super.getConnectionClosedCount();
    }

    @Monitor(name = "ConnectionCreateFailed", type = DataSourceType.COUNTER)
    @Override
    public long getConnectionCreateFailedCount() {
        return super.getConnectionCreateFailedCount();
    }

    @Monitor(name = "ConnectionBorrowed", type = DataSourceType.COUNTER)
    @Override
    public long getConnectionBorrowedCount() {
        return super.getConnectionBorrowedCount();
    }

    @Monitor(name = "ConnectionBorrowedAvgLat", type = DataSourceType.GAUGE)
    @Override
    public long getConnectionBorrowedLatMean() {
        return super.getConnectionBorrowedLatMean();
    }

    @Monitor(name = "ConnectionBorrowedLatP50", type = DataSourceType.GAUGE)
    @Override
    public long getConnectionBorrowedLatP50() {
        return super.getConnectionBorrowedLatP50();
    }

    @Monitor(name = "ConnectionBorrowedLatP99", type = DataSourceType.GAUGE)
    @Override
    public long getConnectionBorrowedLatP99() {
        return super.getConnectionBorrowedLatP99();
    }

    @Monitor(name = "ConnectionReturned", type = DataSourceType.COUNTER)
    @Override
    public long getConnectionReturnedCount() {
        return super.getConnectionReturnedCount();
    }

    @Monitor(name = "PoolExhausted", type = DataSourceType.COUNTER)
    @Override
    public long getPoolExhaustedTimeoutCount() {
        return super.getPoolExhaustedTimeoutCount();
    }

    @Monitor(name = "SocketTimeout", type = DataSourceType.COUNTER)
    @Override
    public long getSocketTimeoutCount() {
        return super.getSocketTimeoutCount();
    }

    @Monitor(name = "OperationTimeout", type = DataSourceType.COUNTER)
    @Override
    public long getOperationTimeoutCount() {
        return super.getOperationTimeoutCount();
    }

    @Monitor(name = "NumFailover", type = DataSourceType.COUNTER)
    @Override
    public long getFailoverCount() {
        return super.getFailoverCount();
    }


    @Monitor(name = "ConnectionBusy", type = DataSourceType.COUNTER)
    @Override
    public long getNumBusyConnections() {
        return super.getNumBusyConnections();
    }

    @Monitor(name = "ConnectionOpen", type = DataSourceType.COUNTER)
    @Override
    public long getNumOpenConnections() {
        return super.getNumOpenConnections();
    }

    @Monitor(name = "NoHostCount", type = DataSourceType.COUNTER)
    @Override
    public long getNoHostCount() {
        return super.getNoHostCount();
    }

    @Monitor(name = "UnknownError", type = DataSourceType.COUNTER)
    @Override
    public long getUnknownErrorCount() {
        return super.getUnknownErrorCount();
    }

    @Monitor(name = "BadRequest", type = DataSourceType.COUNTER)
    @Override
    public long getBadRequestCount() {
        return super.getBadRequestCount();
    }


    @Monitor(name = "HostCount", type = DataSourceType.GAUGE)
    @Override
    public long getHostCount() {
        return super.getHostCount();
    }

    @Monitor(name = "HostUpCount", type = DataSourceType.GAUGE)
    @Override
    public long getHostUpCount() {
        return super.getHostUpCount();
    }

    @Monitor(name = "HostDownCount", type = DataSourceType.GAUGE)
    @Override
    public long getHostDownCount() {
        return super.getHostDownCount();
    }
}
