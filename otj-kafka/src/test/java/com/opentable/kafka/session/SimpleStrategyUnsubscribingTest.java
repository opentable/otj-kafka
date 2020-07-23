/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.opentable.kafka.session;

import java.util.concurrent.ExecutionException;

import org.junit.Test;


public class SimpleStrategyUnsubscribingTest extends AbstractUnsubscribingTest {

    // Shows a simple one consumer processes as expected, even after losing partitions
    // These have single producer/consumer, with bounded message count
    @Test(timeout = 35000L)
    public void testSimpleStrategy() throws ExecutionException, InterruptedException {
        performTestSimpleWithStategy();
    }

}
