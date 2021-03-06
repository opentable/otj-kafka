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


public class ComplexNoStrategySleepingTest extends AbstractSleepingTest {


    // Same as the simple test, but with 3 consumers
    // Total messages remains bounded, and no sleep, so under normal circumstances no rebalance
    @Test(timeout = 15000)
    public void testComplexNoStrategy() throws ExecutionException, InterruptedException {
        performComplexTestWithNoStrategy();
    }

}
