/* ******************************************************************************
 * Copyright 2018 Netflix, Inc.
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
 * *****************************************************************************/
package com.netflix.dyno.jedis;

import org.apache.commons.lang3.tuple.Pair;
import redis.clients.jedis.ScanResult;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Jedis commands specific to Dynomite client.
 */
public interface DynoJedisCommands {
    /**
     * Sets field in the expire hash stored at key to value and ttl.
     * @param key expire hash key
     * @param field hash field
     * @param value field value
     * @param ttl time to live, in seconds
     * @return
     *      1 - if field is a new field in the expire hash and value, ttl was set.
     *      0 - if field already exists and the value, ttl was updated.
     */
    Long ehset(String key, String field, String value, long ttl);

    /**
     * Gets the value stored at field in expire hash with key
     * @param key expire hash key
     * @param field hash field
     * @return
     *      String value, if exists
     *      null - if field doesn't exist.
     */
    String ehget(String key, String field);

    /**
     * Delete the field in the expire hash with key
     * @param key expire hash key
     * @param fields hash field
     * @return
     *      number of fields deleted.
     */
    Long ehdel(String key, String... fields);

    /**
     * Check if a field exists in the expire hash with key
     * @param key expire hash key
     * @param field hash field
     * @return
     *      returns true if field exists, false otherwise.
     */
    Boolean ehexists(String key, String field);

    /**
     * Get all fields and its values in the expire hash
     * @param key expire hash key
     * @return
     *      Map of all fields and its values.
     */
    Map<String, String> ehgetall(String key);

    /**
     * Get all fields in the expire hash
     * @param key expire hash key
     * @return
     *      Set of all fields.
     */
    Set<String> ehkeys(String key);

    /**
     * Get all values stored in the expire hash
     * @param key expire hash key
     * @return
     *      List of all values stored.
     */
    List<String> ehvals(String key);

    /**
     * Get multiple fields from the expire hash
     * @param key expire hash key
     * @param fields hash fields
     * @return
     *      List of requested field values.
     */
    List<String> ehmget(String key, String... fields);

    /**
     * Set multiple fields in the expire hash
     * @param key expire hash key
     * @param hash Tuple of field, value and its TTL
     * @return
     *      returns "OK" if the values were set.
     */
    String ehmset(String key, Map<String, Pair<String, Long>> hash);

    /**
     * Set a field in the expire hash only if it doesn't exist already
     * @param key expire hash key
     * @param field hash field
     * @param value field value
     * @param ttl time to live
     * @return
     *      returns 1 if field set or 0 otherwise.
     */
    Long ehsetnx(String key, String field, String value, Long ttl);

    /**
     * Scan fields in the expire hash
     * @param key expire hashkey
     * @param cursor cursor
     * @return
     *      Map of fields and values in the expire hash.
     */
    ScanResult<Map.Entry<String, String>> ehscan(String key, String cursor);

    /**
     * Increase current value stored in the field of the expire hash
     * @param key expire hash key
     * @param field hash field
     * @param value value to increment by
     * @return
     *      Value of the field after the increment.
     */
    Long ehincrby(String key, String field, long value);

    /**
     * Increase current value stored in the field by a double value
     * @param key expire hash key
     * @param field hash field
     * @param value value to increment by
     * @return
     *      Value of the field after the increment.
     */
    Double ehincrbyfloat(String key, String field, double value);

    /**
     * Number of fields stored in the expire hash
     * @param key expire hash key
     * @return
     *      Count of fields in the expire hash.
     */
    Long ehlen(String key);

    /**
     * Rename the expire hash key.
     * @param oldKey old expire hash key
     * @param newKey new expire hash key
     * @return
     *      returns "OK" if rename was successful.
     */
    String ehrename(String oldKey, String newKey);

    /**
     * Rename the expire hash key if the new key doesn't already exist
     * @param oldKey old expire hash key
     * @param newKey new expire hash key
     * @return
     *      returns 1 if rename was successful, 0 otherwise.
     */
    Long ehrenamenx(String oldKey, String newKey);

    /**
     * Set expiry on the expire hash
     * @param key expire hash key
     * @param seconds expiry in seconds
     * @return
     *      returns 1 if timeout was set, 0 otherwise.
     */
    Long ehexpire(String key, int seconds);

    /**
     * Set expiry on the expire hash
     * @param key expire hash key
     * @param timestamp expiry in unix timestamp (seconds)
     * @return
     *      returns 1 if timeout was set, 0 otherwise.
     */
    Long ehexpireat(String key, long timestamp);

    /**
     * Set expiry on the expire hash
     * @param key expire hash key
     * @param timestamp expiry in unix timestamp (milliseconds)
     * @return
     *      returns 1 if timeout was set, 0 otherwise.
     */
    Long ehpexpireat(String key, long timestamp);

    /**
     * Remove existing timeout on the expire hash
     * @param key expire hash key
     * @return
     *      returns 1 if timeout was removed, 0 otherwise.
     */
    Long ehpersist(String key);

    /**
     * Returns the remaining time on the expire hash
     * @param key expire hash key
     * @return
     *      returns -2 if key does not exist, -1 if the timeout is not set, otherwise returns remaining time.
     */
    Long ehttl(String key);

    /**
     * Returns the remaining time on the expire hash field
     * @param key expire hash key
     * @param field hash field
     * @return
     *      returns remaining time in seconds if key and field exists, 0 otherwise.
     */
    Long ehttl(String key, String field);

    /**
     * Returns the remaining time on the expire hash
     * @param key expire hash key
     * @return
     *      returns -2 if key does not exist, -1 if the timeout is not set, otherwise returns remaining time
     *      in milliseconds.
     */
    Long ehpttl(String key);

    /**
     * Returns the remaining time on the expire hash field
     * @param key expire hash key
     * @param field hash field
     * @return
     *      returns remaining time in milliseconds if key and field exists, 0 otherwise.
     */
    Long ehpttl(String key, String field);

    /**
     * Return a sorted list of expire hash fields
     * @param key expire hash key
     * @return
     *      Sorted list of fields.
     */
    List<String> ehsort(String key);
}
