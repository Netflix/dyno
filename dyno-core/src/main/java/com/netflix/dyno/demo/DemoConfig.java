package com.netflix.dyno.demo;

import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicIntProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;

public class DemoConfig {

	// SAMPLE DATA CONFIG
	public static final DynamicIntProperty NumKeys = DynamicPropertyFactory.getInstance().getIntProperty("dyno.demo.numKeys", 1000);
	public static final DynamicIntProperty NumValues = DynamicPropertyFactory.getInstance().getIntProperty("dyno.demo.numValues", 1000);
	
	// NUM WORKERS
	public static final DynamicIntProperty NumWriters = DynamicPropertyFactory.getInstance().getIntProperty("dyno.demo.numWriters", 1);
	public static final DynamicIntProperty NumReaders = DynamicPropertyFactory.getInstance().getIntProperty("dyno.demo.numReaders", 1);

	// TEST CASE CONFIG
	public static final DynamicBooleanProperty WriteEnabled = DynamicPropertyFactory.getInstance().getBooleanProperty("dyno.demo.writeEnabled", true);
	public static final DynamicBooleanProperty ReadEnabled = DynamicPropertyFactory.getInstance().getBooleanProperty("dyno.demo.readEnabled", true);

	// CONFIG FOR SPY MEMCACHED CLIENT
	public static final DynamicIntProperty Expiration = DynamicPropertyFactory.getInstance().getIntProperty("dyno.demo.expiration", 36000);

	// DYNO HOST ENDPOINT CONFIG
	public static final DynamicStringProperty ServerHostname = DynamicPropertyFactory.getInstance().getStringProperty("dyno.demo.hostname", "localhost");
	public static final DynamicIntProperty ServerPort = DynamicPropertyFactory.getInstance().getIntProperty("dyno.demo.port", 11211);

	public static final DynamicIntProperty ReadRateLimit = DynamicPropertyFactory.getInstance().getIntProperty("dyno.demo.readRateLimit", 10000);
	public static final DynamicIntProperty WriteRateLimit = DynamicPropertyFactory.getInstance().getIntProperty("dyno.demo.writeRateLimit", 2000);

	public static final DynamicIntProperty NumReadersPerConn = DynamicPropertyFactory.getInstance().getIntProperty("dyno.demo.numReadersPerConn", 2);
	public static final DynamicIntProperty NumWritersPerConn = DynamicPropertyFactory.getInstance().getIntProperty("dyno.demo.numWritersPerConn", 1);
}
