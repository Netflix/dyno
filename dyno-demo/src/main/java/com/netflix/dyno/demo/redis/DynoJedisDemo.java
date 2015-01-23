package com.netflix.dyno.demo.redis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import redis.clients.jedis.Response;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.jedis.DynoJedisPipeline;

public class DynoJedisDemo {

	private DynoJedisClient client; 

	public DynoJedisDemo() {

	}

	public void initWithLocalHost() throws Exception {

		final int port = 6379; 
		
		final Host localHost = new Host("localhost", port, Status.Up);

		final HostSupplier localHostSupplier = new HostSupplier() {

			@Override
			public Collection<Host> getHosts() {
				return Collections.singletonList(localHost);
			}
		};

		final TokenMapSupplier tokenSupplier = new TokenMapSupplier() {

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

		init(localHostSupplier, port, tokenSupplier);
	}
	
	private void initWithRemoteCluster(final List<Host> hosts, final int port) throws Exception {
		final HostSupplier clusterHostSupplier = new HostSupplier() {

			@Override
			public Collection<Host> getHosts() {
				return hosts;
			}
		};

		init(clusterHostSupplier, port, null);
	}
	
	public void initWithRemoteClusterFromFile(final String filename, final int port) throws Exception {
		initWithRemoteCluster(readHostsFromFile(filename, port), port);
	}
	
	public void initWithRemoteClusterFromEurekaUrl(final String clusterName, final int port) throws Exception {
		initWithRemoteCluster(getHostsFromDiscovery(clusterName), port);
	}

	public void init(HostSupplier hostSupplier, int port, TokenMapSupplier tokenSupplier) throws Exception {
		
		
		client = new DynoJedisClient.Builder()
		.withApplicationName("demo")
		.withDynomiteClusterName("dyno_demo")
		.withHostSupplier(hostSupplier)
		.withCPConfig(new ConnectionPoolConfigurationImpl("demo")
		.setLocalDC("us-east-1e")
		//.withTokenSupplier(tokenSupplier)
				)
		.withPort(port)
		.build();

//		final String applicationName = "Demo";
//		final String clusterName = "dynomite_redis_puneet";
//
//		client = new DynoJedisClient.Builder()
//        .withApplicationName(applicationName)
//        .withDynomiteClusterName(clusterName)
//        //.withDiscoveryClient(client)
//        .withHostSupplier(hostSupplier)
//        .withPort(port)
//        .withCPConfig(
//            new ConnectionPoolConfigurationImpl(clusterName)
//                .setMaxConnsPerHost(5)
//                .setLoadBalancingStrategy(LoadBalancingStrategy.RoundRobin)
//                .setRetryPolicyFactory(new RetryNTimes.RetryFactory(3, true)
//            )
//        ).build();
	}

	public void runSimpleTest() throws Exception {

		int numKeys = 11;
		// write
		for (int i=0; i<numKeys; i++) {
			client.set("PuneetTest" + i, "" + i);
		}
		// read
		for (int i=0; i<numKeys; i++) {
			OperationResult<String> result = client.d_get("PuneetTest"+i);
			System.out.println("Key: " + i + ", Value: " + result.getResult() + " " + result.getNode());
		}
	}

	

	public void runKeysTest() throws Exception {

//		for (int i=0; i<10; i++) {
//			client.set("foo"+i, "bar"+i);
//		}

//		System.out.println(client.get("foo"));
//		System.out.println(client.get("foo1"));
//		System.out.println(client.get("foo2"));
		
		Set<String> result = client.keys("PuneetTest*");
		System.out.println("Result: " + result);
	}
	
	public void cleanup() throws Exception {

		final int nKeys = 1000;
		// writes for initial seeding
		for (int i=0; i<nKeys; i++) {
			System.out.println("Deleting : " + i);
			client.del("PuneetTest" + i);
		}
	}
	
	public void runMultiThreaded() throws Exception {

		final int nKeys = 1000;
		// writes for initial seeding
		for (int i=0; i<nKeys; i++) {
			System.out.println("Writing : " + i);
			client.set("PuneetTest" + i, "" + i);
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
	
	private List<Host> readHostsFromFile(String filename, int port) throws Exception {
		
		List<Host> hosts = new ArrayList<Host>();
		File file = new File(filename);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		
		try {
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (line.trim().isEmpty()) {
					continue;
				}
				String[] parts = line.trim().split(" ");
				if (parts.length != 2) {
					throw new RuntimeException("Bad data format in file:" + line);
				}
				Host host = new Host(parts[0].trim(), port, Status.Up).setRack(parts[1].trim());
				hosts.add(host);
			}
		} finally {
			reader.close();
		}
		return hosts;
	}
	
	public void runSinglePipeline() throws Exception {

		for (int i=0; i<10; i++) {
			DynoJedisPipeline pipeline = client.pipelined();

			Response<Long> resultA1 = pipeline.hset("Puneet_pipeline" + i, "a1", "v11");
			Response<Long> resultA2 = pipeline.hset("Puneet_pipeline" + i, "a2", "v11");

			pipeline.sync();

			System.out.println(resultA1.get() + " "  + resultA2.get());
		}
	}
	
	public void runPipeline() throws Exception {
		
		int numThreads = 5; 
		
		final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
		final AtomicBoolean stop = new AtomicBoolean(false);
		final CountDownLatch latch = new CountDownLatch(numThreads);
		
		for (int i=0; i<numThreads; i++) {
			threadPool.submit(new Callable<Void>() {

				final Random rand = new Random();
				@Override
				public Void call() throws Exception {
					
					final AtomicInteger iter = new AtomicInteger(0);
					
					while (!stop.get()) {
						int index = rand.nextInt(5);
						int i = iter.incrementAndGet();
						DynoJedisPipeline pipeline = client.pipelined();

						try {
							Response<Long> resultA1 = pipeline.hset("Puneet_pipeline" + index, "a1", "v" + i);
							Response<Long> resultA2 = pipeline.hset("Puneet_pipeline" + index, "a2", "v" + i);

							pipeline.sync();

							System.out.println(resultA1.get() + " "  + resultA2.get());
							
						} catch (Exception e) {
							pipeline.discardPipelineAndReleaseConnection();
							throw e;
						}
						
					}
					latch.countDown();
					return null;
				}
			});
		}
		
		Thread.sleep(5000);
		stop.set(true);
		latch.await();
		
		threadPool.shutdownNow();
	}
	
	private List<Host> getHostsFromDiscovery(final String clusterName) {
		
		String url = "http://discovery.cloudqa.netflix.net:7001/discovery/v2/apps/" + clusterName;
		
		//HttpClient client = new DefaultHttpClient();
		try {
			//HttpResponse response = client.execute(new HttpGet(url));
			//InputStream in = response.getEntity().getContent();
			
			InputStream in = new FileInputStream(new File("/tmp/aa"));
			
			SAXParserFactory parserFactor = SAXParserFactory.newInstance();
			
			SAXParser parser = parserFactor.newSAXParser();
			SAXHandler handler = new SAXHandler("instance", "public-hostname", "availability-zone", "status");
			parser.parse(in, handler);
			
			List<Host> hosts = new ArrayList<Host>();
			
			for (Map<String, String> map : handler.getList()) {
				
				Host host = new Host(map.get("public-hostname"), 8102);
				host.setRack(map.get("availability-zone"));
				host.setStatus(map.get("status").equalsIgnoreCase("UP") ? Status.Up : Status.Down);
				
				hosts.add(host);
				System.out.println("Host: " + host);
			}
			
			return hosts;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	
	public void runLongTest() throws InterruptedException {
		
		final ExecutorService threadPool = Executors.newFixedThreadPool(2);

		final AtomicBoolean stop = new AtomicBoolean(false);
		final CountDownLatch latch = new CountDownLatch(2);

		final AtomicInteger success = new AtomicInteger(0);
		final AtomicInteger failure = new AtomicInteger(0);
		final AtomicInteger emptyReads = new AtomicInteger(0);
		
		threadPool.submit(new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				while (!stop.get()) {
					System.out.println("Getting Value for key '0'");
					String value = client.get("0");
					System.out.println("Got Value for key '0' : " + value);
					Thread.sleep(5000);
				}
				latch.countDown();
				return null;
			}
			
		});

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

		Thread.sleep(60*1000);

		stop.set(true);
		latch.await();
		threadPool.shutdownNow();
	}
	
	private class SAXHandler extends DefaultHandler {

		private final List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		private final String rootElement; 
		private final Set<String> interestElements = new HashSet<String>();
		
		private Map<String, String> currentPayload = null;
		private String currentInterestElement = null;
		
		private SAXHandler(String root, String ... interests) {
			
			rootElement = root;
			for (String s : interests) {
				interestElements.add(s);
			}
		}
		
		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
			
			if (qName.equalsIgnoreCase(rootElement)) {
				// prep for next instance
				currentPayload = new HashMap<String, String>();
				return;
			}
			
			if (interestElements.contains(qName)) {
				// note the element to be parsed. this will be used in chars callback
				currentInterestElement = qName;
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {

			// add host to list
			if (qName.equalsIgnoreCase(rootElement)) {
				list.add(currentPayload);
				currentPayload = null;
			}
		}

		@Override
		public void characters(char[] ch, int start, int length) throws SAXException {

			String value = new String(ch, start, length);
			
			if (currentInterestElement != null && currentPayload != null) {
				currentPayload.put(currentInterestElement, value);
				currentInterestElement = null;
			}
		}
		
		public List<Map<String, String>> getList() {
			return list;
		}
	};
	
	public static void main(String args[]) {

		//BasicConfigurator.configure();
		DynoJedisDemo demo = new DynoJedisDemo();

		try {
			
			//demo.getHostsFromDiscovery("dyno_perfload2");
//			demo.initWithRemoteCluster("src/main/java/dyno_igor.hosts", 8102);
			
			demo.initWithRemoteClusterFromEurekaUrl("dyno_perfload2", 8102);
			System.out.println("Connected");

			demo.runSimpleTest();
			//demo.runKeysTest();
			//demo.runMultiThreaded();
			//demo.runSinglePipeline();
			//demo.runPipeline();
			
			//demo.runLongTest();
			Thread.sleep(1000);
			
			//		demo.cleanup();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			demo.stop();
		}

	}
}
