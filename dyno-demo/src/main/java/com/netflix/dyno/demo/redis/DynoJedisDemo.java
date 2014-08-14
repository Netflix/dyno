package com.netflix.dyno.demo.redis;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.BasicConfigurator;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.jedis.DynoJedisClient;

public class DynoJedisDemo {

	private DynoJedisClient client; 

	public DynoJedisDemo() {

	}

	public void init() throws Exception {

		final int port = 6379; 
		
		final Host localHost = new Host("localhost", port, Status.Up);

		final HostSupplier localHostSupplier = new HostSupplier() {

			@Override
			public Collection<Host> getHosts() {
				return Collections.singletonList(localHost);
			}
		};

		final TokenMapSupplier supplier = new TokenMapSupplier() {

			final HostToken localHostToken = new HostToken(100000L, localHost);

			@Override
			public void initWithHosts(Collection<Host> hosts) {
			}

			@Override
			public List<HostToken> getTokens() {
				return Collections.singletonList(localHostToken);
			}

			@Override
			public HostToken getTokenForHost(Host host) {
				return localHostToken;
			}
		};
		
		
//		final int port = 8102; 
//		final HostSupplier clusterHostSupplier = new HostSupplier() {
//
//			@Override
//			public Collection<Host> getHosts() {
//				
//				List<Host> hosts = new ArrayList<Host>();
//				
//				hosts.add(new Host("ec2-23-20-44-7.compute-1.amazonaws.com", port, Status.Up).setDC("us-east-1e"));
//				hosts.add(new Host("ec2-54-237-48-188.compute-1.amazonaws.com", port, Status.Up).setDC("us-east-1e"));
//				hosts.add(new Host("ec2-54-80-237-37.compute-1.amazonaws.com", port, Status.Up).setDC("us-east-1c"));
//				hosts.add(new Host("ec2-54-204-199-94.compute-1.amazonaws.com", port, Status.Up).setDC("us-east-1c"));
//
//				return hosts;
//			}
//		};
		
		client = new DynoJedisClient.Builder()
		.withApplicationName("demo")
		.withDynomiteClusterName("demo")
		.withHostSupplier(localHostSupplier)
		.withCPConfig(new ConnectionPoolConfigurationImpl("demo")
		.withTokenSupplier(supplier))
		.withPort(port)
		.build();

	}

	public void runSimpleTest() throws Exception {

		// write
		for (int i=0; i<10; i++) {
			client.set("" + i, "" + i);
		}
		// read
		for (int i=0; i<10; i++) {
			OperationResult<String> result = client.d_get(""+i);
			System.out.println("Key: " + i + ", Value: " + result.getResult() + " " + result.getNode());
		}
	}

	public void runMultiThreaded() throws Exception {

		final int nKeys = 1000;
		// writes for initial seeding
		for (int i=0; i<nKeys; i++) {
			System.out.println("Writing : " + i);
			client.set("" + i, "" + i);
		}

		final int numReaders = 2;
		final int numWriters = 2;
		final int nThreads = numReaders + numWriters + 1;

		final ExecutorService threadPool = Executors.newFixedThreadPool(nThreads);

		final AtomicBoolean stop = new AtomicBoolean(false);
		final CountDownLatch latch = new CountDownLatch(nThreads);

		final AtomicInteger success = new AtomicInteger(0);
		final AtomicInteger failure = new AtomicInteger(0);
		final AtomicInteger emptyReads = new AtomicInteger(0);

		startWrites(nKeys, numWriters, threadPool, stop, latch, success, failure);
		startReads(nKeys, numReaders, threadPool, stop, latch, success, failure, emptyReads);

		threadPool.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				while (!stop.get()) {
					System.out.println("Success: " + success.get() + ", failure: " + failure.get() + ", emptyReads: " + emptyReads.get());
					Thread.sleep(1000);
				}
				latch.countDown();
				return null;
			}

		});

		Thread.sleep(15*1000);

		stop.set(true);
		latch.await();
		threadPool.shutdownNow();

		System.out.println("FINAL RESULT \nSuccess: " + success.get() + ", failure: " + failure.get() + ", emptyReads: " + emptyReads.get());
		
	}


	private void startWrites(final int nKeys, final int numWriters, 
			final ExecutorService threadPool, 
			final AtomicBoolean stop, final CountDownLatch latch,
			final AtomicInteger success, final AtomicInteger failure) {

		for (int i=0; i<numWriters; i++) {

			threadPool.submit(new Callable<Void>() {

				final Random random = new Random();
				@Override
				public Void call() throws Exception {

					while (!stop.get()) {
						int key = random.nextInt(nKeys);
						int value = random.nextInt(nKeys);

						try { 
							client.set(""+key, ""+value);
							success.incrementAndGet();
						} catch (Exception e) {
							System.out.println("WRITE FAILURE: " + e.getMessage());
							failure.incrementAndGet();
						}
					}

					latch.countDown();
					return null;
				}

			});
		}
	}


	private void startReads(final int nKeys, final int numReaders, 
			final ExecutorService threadPool, 
			final AtomicBoolean stop, final CountDownLatch latch,
			final AtomicInteger success, final AtomicInteger failure, final AtomicInteger emptyReads) {

		for (int i=0; i<numReaders; i++) {

			threadPool.submit(new Callable<Void>() {

				final Random random = new Random();
				@Override
				public Void call() throws Exception {

					while (!stop.get()) {
						int key = random.nextInt(nKeys);

						try { 
							String value = client.get(""+key);
							success.incrementAndGet();
							if (value == null || value.isEmpty()) {
								emptyReads.incrementAndGet();
							}
						} catch (Exception e) {
							System.out.println("READ FAILURE: " + e.getMessage());
							failure.incrementAndGet();
						}
					}

					latch.countDown();
					return null;
				}
			});
		}
	}

	public void stop() {
		if (client != null) {
			client.stopClient();
		}
	}

	public static void main(String args[]) {

		BasicConfigurator.configure();
		DynoJedisDemo demo = new DynoJedisDemo();

		try {
			demo.init();
			demo.runSimpleTest();
			demo.runMultiThreaded();
			Thread.sleep(1000);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			demo.stop();
		}
	}
}
