package com.netflix.dyno.demo;

import static com.netflix.dyno.demo.DemoConfig.Expiration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import net.spy.memcached.MemcachedClient;

public class DynoWorker {
	
	private final MemcachedClient mc; 
	private final AtomicBoolean isShutdown = new AtomicBoolean(false);
	
	public DynoWorker() {

		try {
			mc = new MemcachedClient(new InetSocketAddress("localhost", 11211));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	
	public String read(String key) {
		return (String)mc.get(key);
	}
	
	public void write(String key, String value) {
		mc.set(key, Expiration.get(), value);
	}
	
	public void shutdown() {
		System.out.println("Shutting down dyno memcached client");
		isShutdown.set(true);
		mc.shutdown();
	}
	
	public boolean isShutdown() {
		return isShutdown.get();
	}
	
	public static void main(String[] args) {
		
		try {
			System.out.println("Running...");
			DynoWorker worker = new DynoWorker();
			
			for (int i=0; i<10000; i++) {
				worker.write(""+i, "value_" + i);
			}
			
			for (int i=0; i<10000; i++) {
				System.out.println(worker.read("" + i));
			}

			worker.shutdown();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
