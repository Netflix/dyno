package com.netflix.dyno.demo;

public class DynoDemo {

	
	private final DynoDriver driver = new DynoDriver(); 

	private DynoDemo() {
	}
	
	public DynoDriver getDriver() {
		return driver;
	}
	
	// THE SINGLETON INSTANCE
	private static final DynoDemo Instance = new DynoDemo(); 
	
	public static DynoDemo getInstance() {
		return Instance;
	}
}
