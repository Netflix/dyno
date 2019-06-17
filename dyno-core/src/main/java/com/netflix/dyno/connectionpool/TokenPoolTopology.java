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
package com.netflix.dyno.connectionpool;

import com.netflix.dyno.connectionpool.impl.ConnectionPoolImpl;
import com.netflix.dyno.connectionpool.impl.utils.CollectionUtils;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class TokenPoolTopology {
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(TokenPoolTopology.class);

    private final ConcurrentHashMap<String, List<TokenStatus>> map = new ConcurrentHashMap<String, List<TokenStatus>>();
    private final ConcurrentHashMap<String, Map<Long, Host>> rackTokenHostMap = new ConcurrentHashMap<String, Map<Long, Host>>();
    private final int replicationFactor;

    public TokenPoolTopology(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public void addToken(String rack, Long token, HostConnectionPool<?> hostPool) {

        List<TokenStatus> list = map.get(rack);
        if (list == null) {
            list = new ArrayList<TokenStatus>();
            map.put(rack, list);
        }

        list.add(new TokenStatus(token, hostPool));
    }

    public void addHostToken(String rack, Long token, Host host) {
        Logger.info("Adding Host to Topology" + host);
        Map<Long, Host> tokenHostMap = rackTokenHostMap.get(rack);
        if (tokenHostMap == null) {
            tokenHostMap = new HashMap<>();
            rackTokenHostMap.put(rack, tokenHostMap);
        }
        tokenHostMap.put(token, host);
    }

    public void removeHost(String rack, Long token, Host host) {
        Logger.info("Removing Host from Topology" + host);

        Map<Long, Host> tokenHostMap = rackTokenHostMap.get(rack);
        if (tokenHostMap == null) {
            return;
        }
        tokenHostMap.put(token, null);
    }

    public ConcurrentHashMap<String, List<TokenStatus>> getAllTokens() {
        return map;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public String getRandomRack() {
        List<String> racks = new ArrayList<String>(rackTokenHostMap.keySet());
        Collections.shuffle(racks);
        return racks.get(0);
    }

    public List<TokenStatus> getTokensForRack(String rack) {
        if (rack != null && map.containsKey(rack)) {
            return map.get(rack);
        }

        return null;
    }

    public Map<Long, Host> getTokenHostsForRack(String rack) {
        if (rack != null && rackTokenHostMap.containsKey(rack)) {
            return rackTokenHostMap.get(rack);
        }

        return null;
    }

    public String toString() {

        ArrayList<String> keyList = new ArrayList<String>(map.keySet());
        Collections.sort(keyList);

        StringBuilder sb = new StringBuilder();
        sb.append("TokenPoolTopology\n");

        for (String key : keyList) {
            sb.append("\nRack: " + key + "\n");
            List<TokenStatus> list = map.get(key);
            Collections.sort(list);
            for (TokenStatus token : list) {
                sb.append(token.toString()).append("\n");
            }
        }

        return sb.toString();
    }

    public static class TokenStatus implements Comparable<TokenStatus> {

        private Long token;
        private HostConnectionPool<?> hostPool;

        private TokenStatus(Long t, HostConnectionPool<?> pool) {
            token = t;
            hostPool = pool;
        }

        public Long getToken() {
            return token;
        }

        public HostConnectionPool<?> getHostPool() {
            return hostPool;
        }

        @Override
        public int compareTo(TokenStatus o) {
            return this.token.compareTo(o.token);
        }

        public String toString() {
            return token + " ==> " + hostPool.toString();
        }
    }

}
