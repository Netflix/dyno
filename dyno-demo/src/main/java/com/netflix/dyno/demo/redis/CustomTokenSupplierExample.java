/**
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.dyno.demo.redis;

import com.netflix.dyno.connectionpool.Host;
import com.netflix.dyno.connectionpool.HostSupplier;
import com.netflix.dyno.connectionpool.OperationResult;
import com.netflix.dyno.connectionpool.TokenMapSupplier;
import com.netflix.dyno.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.dyno.connectionpool.impl.lb.HostToken;
import com.netflix.dyno.jedis.DynoJedisClient;
import org.apache.log4j.BasicConfigurator;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Simple example that illustrates using a custom token map supplier. This example works with a local redis
 * installation on the default port. To setup and run:
 * <ol>
 * <li>brew install redis</li>
 * <li>redis-server /usr/local/etc/redis.conf</li>
 * <li>java -classpath &lt;...classpath...&gt; com.netflix.dyno.demo.redis.CustomTokenSupplierExample</li>
 * </ol>
 */
public class CustomTokenSupplierExample {

    private DynoJedisClient client;

    public CustomTokenSupplierExample() {
    }

    public void init() throws Exception {

        final int port = 6379;

        final Host localHost = new Host("localhost", port, "localrack", Host.Status.Up);

        final HostSupplier localHostSupplier = new HostSupplier() {

            @Override
            public List<Host> getHosts() {
                return Collections.singletonList(localHost);
            }
        };

        final TokenMapSupplier supplier = new TokenMapSupplier() {

            @Override
            public List<HostToken> getTokens(Set<Host> activeHosts) {
                return Collections.singletonList(localHostToken);
            }

            @Override
            public HostToken getTokenForHost(Host host, Set<Host> activeHosts) {
                return localHostToken;
            }

            final HostToken localHostToken = new HostToken(100000L, localHost);

        };

        client = new DynoJedisClient.Builder()
                .withApplicationName("tokenSupplierExample")
                .withDynomiteClusterName("tokenSupplierExample")
                .withHostSupplier(localHostSupplier)
                .withCPConfig(new ConnectionPoolConfigurationImpl("tokenSupplierExample")
                        .withTokenSupplier(supplier))
                .build();

    }

    public void runSimpleTest() throws Exception {

        // write
        for (int i = 0; i < 10; i++) {
            client.set("" + i, "" + i);
        }
        // read
        for (int i = 0; i < 10; i++) {
            OperationResult<String> result = client.d_get("" + i);
            System.out.println("Key: " + i + ", Value: " + result.getResult() + " " + result.getNode());
        }
    }


    public void stop() {
        if (client != null) {
            client.stopClient();
        }
    }

    public static void main(String args[]) {

        BasicConfigurator.configure();
        CustomTokenSupplierExample example = new CustomTokenSupplierExample();

        try {
            example.init();
            example.runSimpleTest();
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            example.stop();
        }
    }
}
