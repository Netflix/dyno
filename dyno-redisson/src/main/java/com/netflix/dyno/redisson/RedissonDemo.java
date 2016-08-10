/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.redisson;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.Future;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.lambdaworks.redis.RedisAsyncConnection;
import com.lambdaworks.redis.RedisClient;

public class RedissonDemo {

	int numThreads = 20;
	int eventLoop = 4;
	
	private final EventLoopGroup eg;
	private final ExecutorService threadPool;
	private final AtomicBoolean stop;
	
	RedisClient client = null;
	RedisAsyncConnection<String, String> rConn = null;

	RedissonDemo(int nThreads, int eLoop) {
		numThreads = nThreads;
		eventLoop = eLoop;
		
		eg = new NioEventLoopGroup(eventLoop);
		threadPool = Executors.newFixedThreadPool(numThreads+1);
		stop = new AtomicBoolean(false);
		client = new RedisClient(eg, "ec2-54-227-136-137.compute-1.amazonaws.com", 8102);
		rConn = client.connectAsync();
	}
	
	public RedissonDemo(String nThreads, String loop) {
		this(Integer.parseInt(nThreads), Integer.parseInt(loop));
	}

	public void run() throws Exception {
		
		System.out.println("\n\nTHREADS: " + numThreads + " LOOP: " + eventLoop);
		
		  final String value1 = "dcfa7d0973834e5c9f480b65de19d684dcfa7d097383dcfa7d0973834e5c9f480b65de19d684dcfa7d097383dcfa7d0973834e5c9f480b65de19d684dcfa7d097383dcfa7d0973834e5c9f480b65de19d684dcfa7d097383";
		  final String StaticValue = value1 + value1 + value1 + value1 + value1;

		try {


			Future<String> result = rConn.get("testPuneet");
			String s = result.get();
			System.out.println("testPuneet: " + s);

//			for (int i=0; i<1000; i++) {
//				System.out.println(i);
//				rConn.set("T" + i, StaticValue).get();
//			}
			
			
			final RedisAsyncConnection<String, String> asyncConn = rConn;
			
			//final CountDownLatch latch = new CountDownLatch(10);
			final AtomicInteger total = new AtomicInteger(0);
			final AtomicInteger prev = new AtomicInteger(0);
			
			final List<String> list = new ArrayList<String>();
			for (int i=0; i<10000; i++) {
				list.add("T"+i);
			}
			
			final int  size = list.size();
			final Random random = new Random();

			for (int i=0; i<numThreads; i++) {
				threadPool.submit(new Callable<Void>() {

					@Override
					public Void call() throws Exception {
						
						while (!stop.get() && !Thread.currentThread().isInterrupted()) {
							
							int index = random.nextInt(size);
							try {
							asyncConn.get(list.get(index)).get(1000, TimeUnit.MILLISECONDS);
							} catch (Exception e) {
								//e.printStackTrace();
								break;
							}
							total.incrementAndGet();
						}
						return null;
					}
				});
			}

			threadPool.submit(new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					while (!stop.get() && !Thread.currentThread().isInterrupted()) {
						try {
							int tCount = total.get();
							System.out.println("RPS: " + (tCount - prev.get())/5);
							prev.set(tCount);
							Thread.sleep(5000);
							//asyncConn.ping().get(1000, TimeUnit.MILLISECONDS);
						} catch (Exception e) {
							//System.out.println("PING FAILURE " + e.getMessage());
							e.printStackTrace();
							break;
						}
					}
					return null;
				}
				
			});
			
//			Thread.sleep(1000*300);
//			threadPool.shutdownNow();
			
		} finally {
//			if (rConn != null) {
//				rConn.close();
//			}
//			if (client != null) {
//				client.shutdown();
//			}
//			eg.shutdownGracefully().get();
		}

	}

	public void stop() {
		stop.set(true);
		threadPool.shutdownNow();
		
		if (rConn != null) {
			rConn.close();
		}
		if (client != null) {
			client.shutdown();
		}
		try {
			eg.shutdownGracefully().get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public static void main(String[] args) {

		RedissonDemo demo = new RedissonDemo(10, 2);
		try {
			demo.run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
