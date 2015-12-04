package com.netflix.dyno.demo.redis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import com.netflix.dyno.connectionpool.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import redis.clients.jedis.Response;

import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.jedis.DynoJedisPipeline;

public class DynoJedisDemo {

	protected DynoJedisClient client;

    protected int numKeys;

    protected final String localDC;

	public DynoJedisDemo(String localDC) {
        this.localDC = localDC;
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
			public List<HostToken> getTokens(Set<Host> activeHosts) {
				return Collections.singletonList(localHostToken);
			}

			@Override
			public HostToken getTokenForHost(Host host, Set<Host> activeHosts) {
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
		.withCPConfig(
                new ConnectionPoolConfigurationImpl("demo")
                        .setLocalDC(this.localDC)
                        //.setCompressionStrategy(ConnectionPoolConfiguration.CompressionStrategy.THRESHOLD)
                        //.setCompressionThreshold(2)
        )
		.withPort(port)
		.build();
	}

	public void runSimpleTest() throws Exception {

		this.numKeys = 10;

		// write
		for (int i=0; i<numKeys; i++) {
            System.out.println("Writing key/value => DynoClientTest-" + i + " / " + i);
			client.set("DynoClientTest-" + i, "" + i);
		}
		// read
		for (int i=0; i<numKeys; i++) {
			OperationResult<String> result = client.d_get("DynoClientTest-"+i);
			System.out.println("Reading Key: " + i + ", Value: " + result.getResult() + " " + result.getNode());
		}
	}

	public void runPipelineEmptyResult() throws Exception {
		DynoJedisPipeline pipeline  = client.pipelined();
        DynoJedisPipeline pipeline2  = client.pipelined();

		try {
			byte[] field1 = "field1".getBytes();
			byte[] field2 = "field2".getBytes();

			pipeline.hset("myHash".getBytes(), field1, "hello".getBytes());
			pipeline.hset("myHash".getBytes(), field2, "world".getBytes());

			Thread.sleep(1000);

			Response<List<byte[]>> result = pipeline.hmget("myHash".getBytes(), field1, field2, "miss".getBytes());

			pipeline.sync();

			System.out.println("TEST-1: hmget for 2 valid results and 1 non-existent field");
            for (int i=0; i < result.get().size(); i++) {
                byte[] val = result.get().get(i);
                if (val != null) {
                    System.out.println("TEST-1:Result => " + i + ") " +  new String(val) );
                } else {
                    System.out.println("TEST-1:Result => " + i + ") " + val);
                }
            }

		} catch (Exception e) {
			pipeline.discardPipelineAndReleaseConnection();
			throw e;
		}

        try {
            Response<List<byte[]>> result2 = pipeline2.hmget("foo".getBytes(), "miss1".getBytes(), "miss2".getBytes());

            pipeline2.sync();

            System.out.println("TEST-2: hmget when all fields (3) are not present in the hash");
            if (result2.get() == null) {
                System.out.println("TEST-2: result is null");
            } else {
                for (int i = 0; i < result2.get().size(); i++) {
                    System.out.println("TEST-2:" + Arrays.toString(result2.get().get(i)));
                }
            }
        } catch (Exception e) {
            pipeline.discardPipelineAndReleaseConnection();
            throw e;
        }
	}

	public void runKeysTest() throws Exception {

		for (int i=0; i<10; i++) {
			client.set("keys"+i, "bar"+i);
		}

		System.out.println(client.get("keys"));
		System.out.println(client.get("keys1"));
		System.out.println(client.get("keys2"));
		
		Set<String> result = client.keys("PuneetTest*");
		System.out.println("Result: " + result);
	}
	
	public void cleanup(int nKeys) throws Exception {

		// writes for initial seeding
		for (int i=0; i<nKeys; i++) {
			System.out.println("Deleting : " + i);
			client.del("DynoDemoTest" + i);
		}
	}

    public void runMultiThreaded() throws Exception {
        this.runMultiThreaded(1000, true, 2, 2);
    }

	public void runMultiThreaded(final int items, boolean doSeed, final int numReaders, final int numWriters) throws Exception {

		final int nKeys = items;
        if (doSeed) {
            // writes for initial seeding
            for (int i = 0; i < nKeys; i++) {
                System.out.println("Writing : " + i);
                client.set("DynoDemoTest" + i, "" + i);
            }
        }

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

		Thread.sleep(15 * 1000);

		stop.set(true);
		latch.await();
		threadPool.shutdownNow();

        executePostRunActions();

        System.out.println("Cleaning up keys");
		cleanup(nKeys);

		System.out.println("FINAL RESULT \nSuccess: " + success.get() + ", failure: " + failure.get() + ", emptyReads: " + emptyReads.get());
		
	}

    protected void executePostRunActions() {
        // nothing to do here
    }

    protected void startWrites(final int nKeys, final int numWriters,
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
							client.set("DynoDemoTest"+key, ""+value);
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


	protected void startReads(final int nKeys, final int numReaders,
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
							String value = client.get("DynoDemoTest"+key);
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
	
	public void runBinarySinglePipeline() throws Exception {

		for (int i=0; i<10; i++) {
			DynoJedisPipeline pipeline = client.pipelined();

			Map<byte[], byte[]> bar = new HashMap<byte[], byte[]>();
			bar.put("key__1".getBytes(), "value__1".getBytes());
			bar.put("key__2".getBytes(), "value__2".getBytes());

			Response<String> hmsetResult = pipeline.hmset(("hash__" + i).getBytes(), bar);

			pipeline.sync();

			System.out.println(hmsetResult.get());
		}

		System.out.println("Reading all keys");

		DynoJedisPipeline readPipeline = client.pipelined();
		Response<Map<byte[], byte[]>> resp = readPipeline.hgetAll("hash__1".getBytes());
		readPipeline.sync();

        StringBuilder sb = new StringBuilder();
        for (byte[] bytes: resp.get().keySet()) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append(new String(bytes));
        }
		System.out.println("Got hash :" + sb.toString());
	}

    public void runSinglePipelineWithCompression(boolean useBinary) throws Exception {
        for (int i=0; i<3; i++) {
            DynoJedisPipeline pipeline = client.pipelined();

            // Map
//            Map<String, String> map = new HashMap<String, String>();
//            String value1 = generateValue(3);
//            String value2 = generateValue(4);
//            map.put("key__1", value1);
//            map.put("key__2", value2);
//            Response<String> hmsetResult = pipeline.hmset(("hash__" + i).getBytes(), bar);

            // Strings
            String value1 = generateValue(3);
            Response<String> resp = pipeline.set("DynoJedisDemo__key__" + i, value1);

            pipeline.sync();

            System.out.println(resp.get());
        }

        System.out.println("Reading keys");

        for (int i=0; i<3; i++) {
            DynoJedisPipeline readPipeline = client.pipelined();
            Response<String> resp = readPipeline.get("DynoJedisDemo__key__" + i);
            readPipeline.sync();
            System.out.println("Result => " + i + ") " +  resp.get());
        }

    }

    public void runSandboxTest() throws Exception {
        Set<String> keys = client.keys("zuulRules:*");
        System.out.println("GOT KEYS");
        System.out.println(keys.size());
    }

    private static String generateValue(int kilobytes) {
        StringBuilder sb = new StringBuilder(kilobytes * 512); // estimating 2 bytes per char
        for (int i = 0; i < kilobytes; i++) {
            for (int j = 0; j < 10; j++) {
                sb.append("abcdefghijklmnopqrstuvwxzy0123456789a1b2c3d4f5g6h7"); // 50 characters (~100 bytes)
                sb.append(":");
                sb.append("abcdefghijklmnopqrstuvwxzy0123456789a1b2c3d4f5g6h7");
                sb.append(":");
            }
        }

        return sb.toString();

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
		
		HttpClient client = new DefaultHttpClient();
		try {
			HttpResponse response = client.execute(new HttpGet(url));
			InputStream in = response.getEntity().getContent();
			
			//InputStream in = new FileInputStream(new File("/tmp/aa"));
			
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

	/**
	 *
	 * @param args Should contain:
	 *             <ol>dynomite_cluster_name</ol>
	 *             <ol>
	 *                 test number, in the set:
	 *                 1 - simple test
	 *                 2 - keys test
	 *                 3 - multi-threaded
	 *                 4 - singe pipeline
	 *                 5 - pipeline
	 *                 6 - binary single pipeline
	 *             </ol>
	 */
	public static void main(String args[]) {

		if (args.length < 2) {
			System.out.println("Incorrect numer of arguments.");
			printUsage();
			System.exit(1);
		}

		String clusterName = args[0];
		int testNumber = Integer.valueOf(args[1]);
        String localDC = args.length > 2 ? args[2] : "us-east-1e";
        String hostsFile = args.length == 4 ? args[3] : null;

        DynoJedisDemo demo = new DynoJedisDemo(localDC);

        try {
			if (hostsFile != null) {
                demo.initWithRemoteClusterFromFile(hostsFile, 8102);
            } else {
                demo.initWithRemoteClusterFromEurekaUrl(clusterName, 8102);
            }

			System.out.println("Connected");

			switch (testNumber) {
				case 1: {
					demo.runSimpleTest();
					break;
				}
				case 2: {
					demo.runKeysTest();
					break;
				}
				case 3: {
					demo.runMultiThreaded();
					break;
				}
			}

			//demo.runSinglePipeline();
			//demo.runPipeline();
			//demo.runBinarySinglePipeline();
			//demo.runPipelineEmptyResult();
            //demo.runSinglePipelineWithCompression(false);
			//demo.runLongTest();
            demo.runSandboxTest();

			Thread.sleep(1000);
			
			demo.cleanup(demo.numKeys);
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			demo.stop();
            System.out.println("Done");
		}
	}

    protected static void printUsage() {
        // todo
    }
}
