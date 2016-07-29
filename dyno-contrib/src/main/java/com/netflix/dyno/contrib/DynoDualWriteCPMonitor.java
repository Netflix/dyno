package com.netflix.dyno.contrib;

import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.annotations.Monitor;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Adds counters specifically for DynoDualWriterClient
 */
public class DynoDualWriteCPMonitor extends DynoCPMonitor {

    private final AtomicLong exceptionCount       = new AtomicLong();

    public DynoDualWriteCPMonitor(String namePrefix) {
        super(namePrefix);
    }

    /**
     * Tracks the approximate number of threads that are actively executing tasks
     *
     * @return
     */
    @Monitor(name = "ShadowPool_ActiveThreadCount", type = DataSourceType.GAUGE)
    public long getActiveThreadCount() {
        return super.getOperationSuccessCount();
    }

    @Monitor(name = "ShadowPool_ExceptionCount", type = DataSourceType.COUNTER)
    public long getShadowPoolExceptionCount() {
        //TODO
        return 0;
    }

    public void incShadowPoolExceptionCount() {

    }
}
