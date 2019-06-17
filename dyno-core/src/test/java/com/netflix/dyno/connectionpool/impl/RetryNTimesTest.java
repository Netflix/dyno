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
package com.netflix.dyno.connectionpool.impl;

import org.junit.Assert;
import org.junit.Test;

public class RetryNTimesTest {

    @Test
    public void testNRetries() throws Exception {

        RetryNTimes retry = new RetryNTimes(3, true);

        RuntimeException e = new RuntimeException("failure");
        retry.begin();

        Assert.assertTrue(retry.allowRetry());

        retry.failure(e);
        Assert.assertTrue(retry.allowRetry());
        retry.failure(e);
        Assert.assertTrue(retry.allowRetry());
        retry.failure(e);
        Assert.assertTrue(retry.allowRetry());

        retry.failure(e);
        Assert.assertFalse(retry.allowRetry());

        Assert.assertEquals(4, retry.getAttemptCount());
    }
}
