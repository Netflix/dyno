/*******************************************************************************
 * Copyright 2015 Netflix
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
package com.netflix.dyno.jedis;

import com.netflix.dyno.connectionpool.CursorBasedIterator;
import redis.clients.jedis.ScanResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates the results of performing a distributed SCAN operation.
 */
public class CursorBasedIteratorImpl<T> implements CursorBasedIterator<T> {

    private final Map<String, ScanResult<T>> result;

    public CursorBasedIteratorImpl(Map<String, ScanResult<T>> result) {
        this.result = result;
    }

    @Override
    public List<T> getResult() {
        final List<T> aggregated = new ArrayList<>();
        for (ScanResult<T> sr: result.values()) {
            aggregated.addAll(sr.getResult());
        }
        return aggregated;
    }

    @Override
    public String getCursorForHost(String host) {
        ScanResult<T> sr = result.get(host);
        if (sr != null) {
            return sr.getStringCursor();
        }

        return null;
    }

}
