/*******************************************************************************
 * Copyright 2011 Netflix
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.dyno.connectionpool.impl.lb;

import com.netflix.dyno.connectionpool.Host;

/**
 * Simple class that encapsulates a host and it's token on the dynomite topology ring.
 * The class must implements Comparable<Long> so that it can be stored in a sorted collection that can then be
 * used in search algos like binary search for efficiently finding the owning token for a hash operation key.
 *
 * @author poberai
 */
public class HostToken implements Comparable<Long> {

    private final Long token;
    private final Host host;

    public HostToken(Long token, Host host) {
        this.token = token;
        this.host = host;
    }

    public Long getToken() {
        return token;
    }

    public Host getHost() {
        return host;
    }

    @Override
    public String toString() {
        return "HostToken [token=" + token + ", host=" + host + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((host == null) ? 0 : host.hashCode());
        result = prime * result + ((token == null) ? 0 : token.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {

        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;

        HostToken other = (HostToken) obj;
        boolean equals = true;
        equals &= (token != null) ? (token.equals(other.token)) : (other.token == null);
        equals &= (host != null) ? (host.equals(other.host)) : (other.host == null);
        return equals;
    }

    @Override
    public int compareTo(Long o) {
        return this.token.compareTo(o);
    }

    public int compareTo(HostToken o) {
        return this.token.compareTo(o.getToken());
    }
}
