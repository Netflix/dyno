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
package com.netflix.dyno.demo.redis;

import com.google.common.collect.Lists;
import com.netflix.dyno.connectionpool.*;
import com.netflix.dyno.connectionpool.Host.Status;
import com.netflix.dyno.connectionpool.exception.PoolOfflineException;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.jedis.DynoJedisClient;
import com.netflix.dyno.jedis.DynoJedisPipeline;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;
import redis.clients.jedis.Response;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DynoJedisDemo {

	private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DynoJedisDemo.class);

	public static final String randomValue = "dcfa7d0973834e5c9f480b65de19d684dcfa7d097383dcfa7d0973834e5c9f480b65de19d684dcfa7d097383dcfa7d0973834e5c9f480b65de19d684dcfa7d097383dcfa7d0973834e5c9f480b65de19d684dcfa7d097383";

	protected DynoJedisClient client;

	protected int numKeys;

	protected final String localRack;
	protected final String clusterName;

	public DynoJedisDemo(String clusterName, String localRack) {
		this.clusterName = clusterName;
		this.localRack = localRack;
	}

	public void initWithLocalHost() throws Exception {

		final int port = 6379;


		final HostSupplier localHostSupplier = new HostSupplier() {
			final Host hostSupplierHost = new Host("localhost", localRack, Status.Up);

			@Override
			public List<Host> getHosts() {
				return Collections.singletonList(hostSupplierHost);
			}
		};

		final TokenMapSupplier tokenSupplier = new TokenMapSupplier() {

			final Host tokenHost = new Host("localhost", port, localRack, Status.Up);
			final HostToken localHostToken = new HostToken(100000L, tokenHost);

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
			public List<Host> getHosts() {
				return getHostsFromDiscovery(clusterName);
//				return hosts;
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

		client = new DynoJedisClient.Builder().withApplicationName("demo").withDynomiteClusterName("dyno_dev")
				.withHostSupplier(hostSupplier)
				.withTokenMapSupplier(tokenSupplier)
				// .withCPConfig(
				// new ConnectionPoolConfigurationImpl("demo")
				// .setCompressionStrategy(ConnectionPoolConfiguration.CompressionStrategy.THRESHOLD)
				// .setCompressionThreshold(2048)
				// .setLocalRack(this.localRack)
				// )
				.build();
	}

	public void runSimpleTest() throws Exception {

		this.numKeys = 10;
		System.out.println("Simple test selected");

		// write
		for (int i = 0; i < numKeys; i++) {
			System.out.println("Writing key/value => DynoClientTest-" + i + " / " + i);
			client.set("DynoClientTest-" + i, "" + i);
		}
		// read
		for (int i = 0; i < numKeys; i++) {
			OperationResult<String> result = client.d_get("DynoClientTest-" + i);
			System.out.println("Reading Key: " + i + ", Value: " + result.getResult() + " " + result.getNode());
		}
	}

	/**
	 * This tests covers the use of binary keys
	 * 
	 * @throws Exception
	 */
	public void runBinaryKeyTest() throws Exception {

		System.out.println("Binary Key test selected");
        byte[] videoInt =ByteBuffer.allocate(4).putInt(new Integer(100)).array();
        byte[] locInt =ByteBuffer.allocate(4).putInt(new Integer(200)).array();
        byte[] overallKey = new byte[videoInt.length + locInt.length];
		
		byte[] firstWindow = ByteBuffer.allocate(4).putFloat(new Float(1.25)).array();
		byte[] secondWindow = ByteBuffer.allocate(4).putFloat(new Float(1.5)).array();
		byte[] thirdWindow = ByteBuffer.allocate(4).putFloat(new Float(1.75)).array();
		byte[] fourthWindow = ByteBuffer.allocate(4).putFloat(new Float(2.0)).array();

		byte[] overallVal = new byte[firstWindow.length + secondWindow.length + thirdWindow.length
				+ fourthWindow.length];

        byte[] newKey = new byte[videoInt.length+locInt.length];
		
		// write
		client.set(overallKey, overallVal);
		System.out.println("Writing Key: " +  new String(overallKey, Charset.forName("UTF-8")));
		
		// read
		OperationResult<byte[]> result = client.d_get(newKey);
		System.out.println("Reading Key: " + new String(newKey, Charset.forName("UTF-8")) + ", Value: " + result.getResult().toString() + " " + result.getNode());
		
	}

	/**
	 * To run this test, the hashtag FP must be set on the Dynomite cluster. The
	 * assumed hashtag for this test is {} hence each key is foo-{<String>}. To
	 * validate that this test succeeds observe the cluster manually.
	 * 
	 * @throws Exception
	 */
	public void runSimpleTestWithHashtag() throws Exception {

		this.numKeys = 100;
		System.out.println("Simple test with hashtag selected");

		// write
		for (int i = 0; i < numKeys; i++) {
			System.out.println("Writing key/value => DynoClientTest-" + i + " / " + i);
			client.set(i + "-{bar}", " " + i);
		}
		// read
		for (int i = 0; i < numKeys; i++) {
			OperationResult<String> result = client.d_get(i + "-{bar}");
			System.out.println(
					"Reading Key: " + i + "-{bar}" + " , Value: " + result.getResult() + " " + result.getNode());
		}
	}

	public void runPipelineEmptyResult() throws Exception {
		DynoJedisPipeline pipeline = client.pipelined();
		DynoJedisPipeline pipeline2 = client.pipelined();

		try {
			byte[] field1 = "field1".getBytes();
			byte[] field2 = "field2".getBytes();

			pipeline.hset("myHash".getBytes(), field1, "hello".getBytes());
			pipeline.hset("myHash".getBytes(), field2, "world".getBytes());

			Thread.sleep(1000);

			Response<List<byte[]>> result = pipeline.hmget("myHash".getBytes(), field1, field2, "miss".getBytes());

			pipeline.sync();

			System.out.println("TEST-1: hmget for 2 valid results and 1 non-existent field");
			for (int i = 0; i < result.get().size(); i++) {
				byte[] val = result.get().get(i);
				if (val != null) {
					System.out.println("TEST-1:Result => " + i + ") " + new String(val));
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
		System.out.println("Writing 10,000 keys to dynomite...");

		for (int i = 0; i < 500; i++) {
			client.set("DynoClientTest_KEYS-TEST-key" + i, "value-" + i);
		}

		System.out.println("finished writing 10000 keys, querying for keys(\"DynoClientTest_KYES-TEST*\")");

		Set<String> result = client.keys("DynoClientTest_KEYS-TEST*");

		System.out.println("Got " + result.size() + " results, below");
		System.out.println(result);
	}

	public void runScanTest(boolean populateKeys) throws Exception {
		logger.info("SCAN TEST -- begin");

		final String keyPattern = System.getProperty("dyno.demo.scan.key.pattern", "DynoClientTest_key-*");
		final String keyPrefix = System.getProperty("dyno.demo.scan.key.prefix", "DynoClientTest_key-");

		if (populateKeys) {
			logger.info("Writing 500 keys to {} with prefix {}", this.clusterName, keyPrefix);
			for (int i = 0; i < 500; i++) {
				client.set(keyPrefix + i, "value-" + i);
			}
		}

		logger.info("Reading keys from {} with pattern {}", this.clusterName, keyPattern);
		CursorBasedResult<String> cbi = null;
		long start = System.currentTimeMillis();
		int count = 0;
		do {
			try {

				cbi = client.dyno_scan(cbi, 5, keyPattern);
			} catch (PoolOfflineException ex) {
				logger.info("Caught exception.... retrying scan");
				cbi = null;
				continue;
			}


			List<String> results = cbi.getStringResult();
			count += results.size();
			int i = 0;
			for (String res : results) {
				logger.info("{}) {}", i, res);
				i++;
			}
		} while ((cbi == null) || !cbi.isComplete());
		long end = System.currentTimeMillis();


		logger.info("SCAN TEST -- done {} results in {}ms", count, end - start);
	}

	public void runSScanTest(boolean populateKeys) throws Exception {
		logger.info("SET SCAN TEST -- begin");

		final String key = "DynoClientTest_Set";

		if (populateKeys) {
			logger.info("Populating set in cluster {} with key {}", this.clusterName, key);
			for (int i = 0; i < 50; i++) {
				client.sadd(key, "value-" + i);
			}
		}

		logger.info("Reading members of set from cluster {} with key {}", this.clusterName, key);
		ScanResult<String> scanResult;
		final Set<String> matches = new HashSet<>();
		String cursor = "0";
		do {

			final ScanParams scanParams = new ScanParams().count(10);
			scanParams.match("*");
			scanResult = client.sscan(key, cursor, scanParams);
			matches.addAll(scanResult.getResult());
			cursor = scanResult.getStringCursor();
			if ("0".equals(cursor)) {
				break;
			}
		} while (true);
		logger.info("SET SCAN TEST -- done");
	}

	public void cleanup(int nKeys) throws Exception {

		// writes for initial seeding
		for (int i = 0; i < nKeys; i++) {
			System.out.println("Deleting : " + i);
			client.del("DynoDemoTest" + i);
		}
	}

	public void runMultiThreaded() throws Exception {
		this.runMultiThreaded(1000, true, 2, 2);
	}

	public void runMultiThreaded(final int items, boolean doSeed, final int numReaders, final int numWriters)
			throws Exception {

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
					System.out.println("Success: " + success.get() + ", failure: " + failure.get() + ", emptyReads: "
							+ emptyReads.get());
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

		System.out.println("FINAL RESULT \nSuccess: " + success.get() + ", failure: " + failure.get() + ", emptyReads: "
				+ emptyReads.get());

	}

	protected void executePostRunActions() {
		// nothing to do here
	}

	protected void startWrites(final int nKeys, final int numWriters, final ExecutorService threadPool,
			final AtomicBoolean stop, final CountDownLatch latch, final AtomicInteger success,
			final AtomicInteger failure) {

		for (int i = 0; i < numWriters; i++) {

			threadPool.submit(new Callable<Void>() {

				final Random random = new Random();

				@Override
				public Void call() throws Exception {

					while (!stop.get()) {
						int key = random.nextInt(nKeys);
						int value = random.nextInt(nKeys);

						try {
							client.set("DynoDemoTest" + key, "" + value);
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

	protected void startReads(final int nKeys, final int numReaders, final ExecutorService threadPool,
			final AtomicBoolean stop, final CountDownLatch latch, final AtomicInteger success,
			final AtomicInteger failure, final AtomicInteger emptyReads) {

		for (int i = 0; i < numReaders; i++) {

			threadPool.submit(new Callable<Void>() {

				final Random random = new Random();

				@Override
				public Void call() throws Exception {

					while (!stop.get()) {
						int key = random.nextInt(nKeys);

						try {
							String value = client.get("DynoDemoTest" + key);
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
				Host host = new Host(parts[0].trim(), port, parts[1].trim(), Status.Up);
				hosts.add(host);
			}
		} finally {
			reader.close();
		}
		return hosts;
	}

	public void runBinarySinglePipeline() throws Exception {

		for (int i = 0; i < 10; i++) {
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
		for (byte[] bytes : resp.get().keySet()) {
			if (sb.length() > 0) {
				sb.append(",");
			}
			sb.append(new String(bytes));
		}
		System.out.println("Got hash :" + sb.toString());
	}

	public void runCompressionInPipelineTest() throws Exception {
		final int maxNumKeys = 100;
		final int maxPipelineSize = 10;
		final int maxOperations = 500;
		final Random rand = new Random();

		for (int operationIter = 0; operationIter < maxOperations; operationIter++) {

			DynoJedisPipeline pipeline = client.pipelined();
			int pipelineSize = 1 + rand.nextInt(maxPipelineSize);

			// key to be used in pipeline
			String key = "hash__" + rand.nextInt(maxNumKeys);

			// Map of field -> value
			Map<String, String> map = new HashMap<>();

			// List of fields to be later used in HMGet
			List<String> fields = new ArrayList<>(pipelineSize);

			// Create a map of field -> value, also accumulate all fields
			for (int pipelineIter = 0; pipelineIter < pipelineSize; pipelineIter++) {
				String field = "field_" + pipelineIter;
				fields.add(field);
				String prefixSuffix = key + "_" + field;
				String value = prefixSuffix + "_" + generateValue(pipelineIter) + "_" + prefixSuffix;
				map.put(field, value);
			}

			Response<String> HMSetResult = pipeline.hmset(key, map);
			Response<List<String>> HMGetResult = pipeline.hmget(key, fields.toArray(new String[fields.size()]));
			try {
				pipeline.sync();
			} catch (Exception e) {
				pipeline.discardPipelineAndReleaseConnection();
				System.out.println("Exception while writing key " + key + " fields: " + fields);
				throw e;
			}

			if (!HMSetResult.get().equals("OK")) {
				System.out.println("Result mismatch for HMSet key: '" + key + "' fields: '" + fields + "' result: '"
						+ HMSetResult.get() + "'");
			}
			if ((operationIter % 100) == 0) {
				System.out.println("\n>>>>>>>> " + operationIter + " operations performed....");
			}
			List<String> HMGetResultStrings = HMGetResult.get();
			for (int i = 0; i < HMGetResultStrings.size(); i++) {
				String prefixSuffix = key + "_" + fields.get(i);
				String value = HMGetResultStrings.get(i);
				if (value.startsWith(prefixSuffix) && value.endsWith(prefixSuffix)) {
					continue;
				} else {
					System.out.println("Result mismatch key: '" + key + "' field: '" + fields.get(i) + "' value: '"
							+ HMGetResultStrings.get(i) + "'");
				}

			}
		}
		System.out.println("Compression test Done: " + maxOperations + " pipeline operations performed.");

	}

	public void runSandboxTest() throws Exception {
		Set<String> keys = client.keys("zuulRules:*");
		System.out.println("GOT KEYS");
		System.out.println(keys.size());
	}

	private static String generateValue(int kilobytes) {
		StringBuilder sb = new StringBuilder(kilobytes * 512); // estimating 2
																// bytes per char
		for (int i = 0; i < kilobytes; i++) {
			for (int j = 0; j < 10; j++) {
				sb.append("abcdefghijklmnopqrstuvwxzy0123456789a1b2c3d4f5g6h7"); // 50
																					// characters
																					// (~100
																					// bytes)
				sb.append(":");
				sb.append("abcdefghijklmnopqrstuvwxzy0123456789a1b2c3d4f5g6h7");
				sb.append(":");
			}
		}

		return sb.toString();

	}

	/**
	 * This demo runs a pipeline across ten different keys. The pipeline leverages
	 * the hash value {bar} to determine the node where to send the data.
	 * 
	 * @throws Exception
	 */
	public void runPipelineWithHashtag() throws Exception {

		DynoJedisPipeline pipeline = client.pipelined();
		try {

			pipeline.set("pipeline-hashtag1-{bar}", "value-1");
			pipeline.set("pipeline-hashtag2-{bar}", "value-2");
			pipeline.set("pipeline-hashtag3-{bar}", "value-3");
			pipeline.set("pipeline-hashtag4-{bar}", "value-4");
			pipeline.set("pipeline-hashtag5-{bar}", "value-5");
			pipeline.set("pipeline-hashtag6-{bar}", "value-6");
			pipeline.set("pipeline-hashtag7-{bar}", "value-7");
			pipeline.set("pipeline-hashtag8-{bar}", "value-8");
			pipeline.set("pipeline-hashtag9-{bar}", "value-9");
			pipeline.set("pipeline-hashtag10-{bar}", "value-10");

			Response<String> value1 = pipeline.get("pipeline-hashtag1-{bar}");
			Response<String> value2 = pipeline.get("pipeline-hashtag2-{bar}");
			Response<String> value3 = pipeline.get("pipeline-hashtag3-{bar}");
			Response<String> value4 = pipeline.get("pipeline-hashtag4-{bar}");
			Response<String> value5 = pipeline.get("pipeline-hashtag5-{bar}");
			Response<String> value6 = pipeline.get("pipeline-hashtag6-{bar}");
			pipeline.sync();

			System.out.println(value1.get());
			System.out.println(value2.get());
			System.out.println(value3.get());
			System.out.println(value4.get());
			System.out.println(value5.get());
			System.out.println(value6.get());
		} catch (Exception e) {
			pipeline.discardPipelineAndReleaseConnection();
			throw e;
		}
	}

	public void runPipeline() throws Exception {

		int numThreads = 5;

		final ExecutorService threadPool = Executors.newFixedThreadPool(numThreads);
		final AtomicBoolean stop = new AtomicBoolean(false);
		final CountDownLatch latch = new CountDownLatch(numThreads);

		for (int i = 0; i < numThreads; i++) {
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
							Response<Long> resultA1 = pipeline.hset("DynoJedisDemo_pipeline-" + index, "a1",
									constructRandomValue(index));
							Response<Long> resultA2 = pipeline.hset("DynoJedisDemo_pipeline-" + index, "a2",
									"value-" + i);

							pipeline.sync();

							System.out.println(resultA1.get() + " " + resultA2.get());

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

	private String constructRandomValue(int sizeInKB) {

		int requriredLength = sizeInKB * 1024;

		String s = randomValue;
		int sLength = s.length();

		StringBuilder sb = new StringBuilder();
		int lengthSoFar = 0;

		do {
			sb.append(s);
			lengthSoFar += sLength;
		} while (lengthSoFar < requriredLength);

		String ss = sb.toString();

		if (ss.length() > requriredLength) {
			ss = sb.substring(0, requriredLength);
		}

		return ss;
	}

	private List<Host> getHostsFromDiscovery(final String clusterName) {

		String env = System.getProperty("netflix.environment", "test");
		String discoveryKey = String.format("dyno.demo.discovery.%s", env);

		if (!System.getProperties().containsKey(discoveryKey)) {
			throw new IllegalArgumentException("Discovery URL not found");
		}

		String localDatacenter = System.getProperty("LOCAL_DATACENTER");
		final String discoveryUrl = String.format(System.getProperty(discoveryKey), localDatacenter);

		final String url = String.format("http://%s/%s", discoveryUrl, clusterName);

		HttpClient client = new DefaultHttpClient();
		try {
			HttpResponse response = client.execute(new HttpGet(url));
			InputStream in = response.getEntity().getContent();

			SAXParserFactory parserFactor = SAXParserFactory.newInstance();

			SAXParser parser = parserFactor.newSAXParser();
			SAXHandler handler = new SAXHandler("instance", "public-hostname", "availability-zone", "status",
					"local-ipv4");
			parser.parse(in, handler);

			List<Host> hosts = new ArrayList<Host>();

			for (Map<String, String> map : handler.getList()) {
				String rack = map.get("availability-zone");
				Status status = map.get("status").equalsIgnoreCase("UP") ? Status.Up : Status.Down;
				Host host = new Host(map.get("public-hostname"), map.get("local-ipv4"), rack, status);
				hosts.add(host);
				System.out.println("Host: " + host);
			}

			return hosts;
		} catch (Throwable e) {
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
					System.out.println("Success: " + success.get() + ", failure: " + failure.get() + ", emptyReads: "
							+ emptyReads.get());
					Thread.sleep(1000);
				}
				latch.countDown();
				return null;
			}

		});

		Thread.sleep(60 * 1000);

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

		private SAXHandler(String root, String... interests) {

			rootElement = root;
			for (String s : interests) {
				interestElements.add(s);
			}
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes)
				throws SAXException {

			if (qName.equalsIgnoreCase(rootElement)) {
				// prep for next instance
				currentPayload = new HashMap<String, String>();
				return;
			}

			if (interestElements.contains(qName)) {
				// note the element to be parsed. this will be used in chars
				// callback
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
	}

	public void runEvalTest() throws Exception {

		client.set("EvalTest", "true");

		List<String> keys = Lists.newArrayList("EvalTest");
		List<String> args = Lists.newArrayList("true");
		Object obj = client.eval(
				"if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
				keys, args);
		if (obj.toString().equals("1"))
			System.out.println("EVAL Test Succeeded");
		else
			System.out.println("EVAL Test Failed");

	}

	/**
	 *
	 * @param args
	 *            Should contain:
	 *            <ol>
	 *            dynomite_cluster_name, e.g. dyno_sandbox_quorum
	 *            </ol>
	 *            <ol>
	 *            test number, in the set: 1 - simple test 2 - keys test 3 - simple
	 *            test with hashtag 4- multi-threaded 5 - SCAN test 6 - pipeline 7 -
	 *            run pipeline with hashtag 7 - runn SSCAN test
	 *            </ol>
	 */
	public static void main(String args[]) throws IOException {

		if (args.length < 2) {
			System.out.println("Incorrect number of arguments.");
			printUsage();
			System.exit(1);
		}

		String clusterName = args[0];
		int testNumber = Integer.valueOf(args[1]);

		Properties props = new Properties();
		props.load(DynoJedisDemo.class.getResourceAsStream("/demo.properties"));
		for (String name : props.stringPropertyNames()) {
			System.setProperty(name, props.getProperty(name));
		}

		if (!props.containsKey("EC2_AVAILABILITY_ZONE") && !props.containsKey("dyno.dyno_demo.lbStrategy")) {
			throw new IllegalArgumentException(
					"MUST set local for load balancing OR set the load balancing strategy to round robin");
		}

		String rack = props.getProperty("EC2_AVAILABILITY_ZONE", "us-east-1e");
		String hostsFile = props.getProperty("dyno.demo.hostsFile");
		int port = Integer.valueOf(props.getProperty("dyno.demo.port", "8102"));

		DynoJedisDemo demo = new DynoJedisDemo(clusterName, rack);

		try {
			if (hostsFile != null) {
				demo.initWithRemoteClusterFromFile(hostsFile, port);
			} else {
				demo.initWithRemoteClusterFromEurekaUrl(clusterName, port);
			}
//			demo.initWithLocalHost();

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
				demo.runSimpleTestWithHashtag();
				break;
			}
			case 4: {
				demo.runMultiThreaded();
				break;
			}
			case 5: {
				final boolean writeKeys = Boolean.valueOf(props.getProperty("dyno.demo.scan.populateKeys"));
				demo.runScanTest(writeKeys);
				break;
			}
			case 6: {
				demo.runPipeline();
				break;
			}
			case 7: {
				demo.runPipelineWithHashtag();
				break;
			}
			case 8: {
				demo.runSScanTest(true);
				break;
			}
			case 9: {
				demo.runCompressionInPipelineTest();
				break;
			}
			case 10: {
				demo.runEvalTest();
				break;
			}

			case 11: {
				demo.runBinaryKeyTest();
				break;
			}
			}

			// demo.runSinglePipeline();
			// demo.runPipeline();
			// demo.runBinarySinglePipeline();
			// demo.runPipelineEmptyResult();
			// demo.runSinglePipelineWithCompression(false);
			// demo.runLongTest();
			// demo.runSandboxTest();

			Thread.sleep(1000);

			// demo.cleanup(demo.numKeys);

		} catch (Throwable e) {
			e.printStackTrace();
		} finally {
			demo.stop();
			System.out.println("Done");

			System.out.flush();
			System.err.flush();
			System.exit(0);
		}
	}

	protected static void printUsage() {
		// todo
	}
}