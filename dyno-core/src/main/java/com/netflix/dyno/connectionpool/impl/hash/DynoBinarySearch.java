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
package com.netflix.dyno.connectionpool.impl.hash;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import com.netflix.dyno.connectionpool.HashPartitioner;

/**
 * Utility class that maps a given token hashed from a key using a {@link HashPartitioner}
 * to a dynomite server on the dynomite topology ring.
 * <p>
 * Note that as long as the Token T implements the comparable interface this class can be used
 * to perform the bin search on other homogeneous lists as well.
 * <p>
 * Here are the imp details of the mapping algorithm
 * 1.  If a hashed token directly maps to a point on that ring, then that point is chosen.
 * 2.  If a hashed token maps between 2 points A and B where A > B then B is chosen as the owner of the token
 * 3.  All hashed tokens that go past the last point on the ring are mapped to the first point on the ring.
 * <p>
 * e.g
 * <p>
 * Consider the following points on the ring.
 * <p>
 * 10, 20, 30, 40, 50, 60, 70, 80, 90, 100
 * <p>
 * Elements 0 .. 9 --> 10
 * 10 --> 10
 * 15 --> 20
 * 30 --> 30
 * 58 --> 60
 * 100 --> 100
 * 100 +  --> 10
 *
 * @param <T>
 * @author poberai
 */
public class DynoBinarySearch<T extends Comparable<T>> {

    private final List<DynoTokenRange<T>> rangeList = new ArrayList<DynoTokenRange<T>>();

    private final AtomicBoolean listEmpty = new AtomicBoolean(false);

    public DynoBinarySearch(List<T> list) {

        if (list.isEmpty()) {
            listEmpty.set(true);
            return;
        }

        if (list.size() == 1) {
            rangeList.add(new DynoTokenRange<T>(null, list.get(0)));

        } else {
            // add the first range
            rangeList.add(new DynoTokenRange<T>(null, list.get(0)));
            // add rest of the tokens
            for (int i = 1; i < (list.size()); i++) {
                rangeList.add(new DynoTokenRange<T>(list.get(i - 1), list.get(i)));
            }

            rangeList.get(rangeList.size() - 1).isLastRange = true;
        }
    }


    public T getTokenOwner(T token) {

        // Some quick boundary checks
        if (listEmpty.get()) {
            return null;
        }

        if (rangeList.size() == 1) {
            return rangeList.get(0).getTokenOwner();
        }

        DynoTokenRange<T> firstRange = rangeList.get(0);
        DynoTokenRange<T> lastRange = rangeList.get(rangeList.size() - 1);

        if (firstRange.compareTo(token) == 0) {
            // Token is smaller than FIRST range, map to first range.
            return firstRange.getTokenOwner();
        }

        if (lastRange.compareTo(token) < 0) {
            // Token is greater than LAST range, map to first range.
            return firstRange.getTokenOwner();
        }

        int index = Collections.binarySearch(rangeList, token);

        if (index < 0) {
            throw new RuntimeException("Token not found!: " + token);
        }

        return rangeList.get(index).getTokenOwner();
    }

    public String toString() {

        StringBuilder sb = new StringBuilder("[DynoBinarySearch:\n");
        for (DynoTokenRange<T> r : rangeList) {
            sb.append(r.toString()).append("\n");
        }
        sb.append("]");
        return sb.toString();
    }

    static class DynoTokenRange<T extends Comparable<T>> implements Comparable<T> {

        final T start;
        final T end;
        boolean isFirstRange = false;
        boolean isLastRange = false;

        DynoTokenRange(T s, T e) {
            this.start = s;
            this.end = e;

            if (s == null) {
                isFirstRange = true;
            }

            if (isFirstRange) {
                if (end == null) {
                    throw new RuntimeException("Bad Range: end must not be null");
                }
            } else if (!(lessThan(start, end))) {
                throw new RuntimeException("Bad Range: start must be less than end: " + this.toString());
            }
        }

        public T getTokenOwner() {
            return end;
        }

        public String toString() {
            if (isFirstRange) {
                return "(null," + end + "]";
            } else {
                return "(" + start + "," + end + "]";
            }
        }

        @Override
        public int compareTo(T key) {

            // Boundary checks, to be safe!

            if (isFirstRange) {

                if (lessThanEquals(key, end)) {
                    return 0;  // This key is within this range
                } else {
                    // else This range is smaller than this key
                    return -1;
                }
            }

            // Functionality for any other range i.e in another position in the list except for the first
            if (lessThanEquals(key, start)) {
                // This range is greater than the key
                return 1;
            }

            if (lessThan(start, key) && lessThanEquals(key, end)) {
                // This key is within this range
                return 0;
            }

            if (lessThan(end, key)) {
                // This range is smaller than this key
                return -1;
            } else {
                throw new RuntimeException("Invalid key for bin search: " + key + ", this range: " + this.toString());
            }
        }

        private boolean lessThan(T left, T right) {
            return left.compareTo(right) < 0;
        }

        private boolean lessThanEquals(T left, T right) {
            return left.compareTo(right) <= 0;
        }
    }
}
