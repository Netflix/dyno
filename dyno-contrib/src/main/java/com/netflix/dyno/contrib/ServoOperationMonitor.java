package com.netflix.dyno.contrib;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.netflix.dyno.connectionpool.OperationMonitor;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.Monitors;
import com.netflix.servo.monitor.Timer;

public class ServoOperationMonitor implements OperationMonitor {

	private static final String Success = "__SUCCESS"; 
	private static final String Error = "__ERROR"; 
	
	private final ConcurrentHashMap<String, Counter> counterMap = new ConcurrentHashMap<String, Counter>();
	private final ConcurrentHashMap<String, Timer> timerMap = new ConcurrentHashMap<String, Timer>();

	private final String SuccessPrefix;
	private final String ErrorPrefix;
	private final String TimerPrefix;

	public ServoOperationMonitor(String prefix) {
		SuccessPrefix = prefix + Success;
		ErrorPrefix   = prefix + Error;
		TimerPrefix = prefix = "__";
	}
	
	@Override
	public void recordLatency(String opName, long duration, TimeUnit unit) {
		getOrCreateTimer(TimerPrefix + opName).record(duration, unit);
	}

	@Override
	public void recordSuccess(String opName) {
		getOrCreateCounter(SuccessPrefix + opName).increment();
	}

	@Override
	public void recordFailure(String opName, String reason) {
		getOrCreateCounter(ErrorPrefix + opName).increment();
	}
	
	private Counter getOrCreateCounter(String name) {
		
		Counter counter = counterMap.get(name);
		
		if (counter == null) {
			counter = Monitors.newCounter(name);
			Counter oldCounter = counterMap.putIfAbsent(name, counter);
			if (oldCounter == null) {
				Monitors.registerObject(counter);
				return counter;
			} else {
				return oldCounter;
			}
		} else {
			return counter;
		}
	}
	
	private Timer getOrCreateTimer(String name) {
		
		Timer timer = timerMap.get(name);
		
		if (timer == null) {
			timer = Monitors.newTimer(name, TimeUnit.MILLISECONDS);
			Timer oldTimer = timerMap.putIfAbsent(name, timer);
			if (oldTimer == null) {
				Monitors.registerObject(timer);
				return timer;
			} else {
				return oldTimer;
			}
		} else {
			return timer;
		}
	}
	
}
