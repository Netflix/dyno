package com.netflix.dyno.demo;

import static com.netflix.dyno.demo.DemoConfig.Expiration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.RateLimiter;

public class DynoWorker {
	
	private static final Logger Logger = LoggerFactory.getLogger(DynoWorker.class);
	
	private final MemcachedClient mc; 
	private final AtomicBoolean isShutdown = new AtomicBoolean(false);
	
	private final AtomicReference<RateLimiter> writeRateLimiter = 
			new AtomicReference<RateLimiter>(RateLimiter.create(DemoConfig.WriteRateLimit.get()));
	private final AtomicReference<RateLimiter> readRateLimiter = 
			new AtomicReference<RateLimiter>(RateLimiter.create(DemoConfig.ReadRateLimit.get()));

	public DynoWorker() {

		try {
			mc = new MemcachedClient(new InetSocketAddress(DemoConfig.ServerHostname.get(), DemoConfig.ServerPort.get()));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		
		DemoConfig.WriteRateLimit.addCallback(new Runnable() {

			@Override
			public void run() {
				Logger.info("Changing rate limiter: " + DemoConfig.WriteRateLimit.get());
				writeRateLimiter.set(RateLimiter.create(DemoConfig.WriteRateLimit.get()));
				Logger.info("Changing rate limiter: " + DemoConfig.WriteRateLimit.get() + " " + writeRateLimiter.get().getRate());
			}
			
		});
		
		DemoConfig.ReadRateLimit.addCallback(new Runnable() {

			@Override
			public void run() {
				Logger.info("Changing read rate limiter: " + DemoConfig.ReadRateLimit.get());
				readRateLimiter.set(RateLimiter.create(DemoConfig.ReadRateLimit.get()));
				Logger.info("Changing read rate limiter: " + DemoConfig.ReadRateLimit.get() + " " + readRateLimiter.get().getRate());
			}
			
		});
		
	}

	
	public String read(String key) {
		readRateLimiter.get().acquire();
		return (String)mc.get(key);
	}
	
	public void write(String key, String value) {
		
		writeRateLimiter.get().acquire();
		
		OperationFuture<Boolean> future = mc.set(key, Expiration.get(), value);
		try {
			future.get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
